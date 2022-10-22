import argparse
from datetime import datetime, timedelta, timezone
import dulwich.objects
import dulwich.repo
import itertools
import logging
from pathlib import Path
import pymysql.cursors
import requests
from typing import Any, Iterable, NoReturn, Optional, Tuple, TypeVar

from instiki2git.percent_code import PercentCode
import instiki2git.git_tools as git

logger: logging.Logger = logging.getLogger(__name__)

# General helper functions.

T = TypeVar('T')
def iter_inhabited(it: Iterable[T]) -> (bool, Iterable[T]):
    """
    Test whether an iterable is inhabited.
    Since this modifies the original iterable, a replacement iterable is returned.
    """
    try:
        x = next(it)
    except StopIteration:
        return (False, tuple())
    return (True, itertools.chain((x,), it))

# Database queries.

def query_iterator(
    cursor: pymysql.cursors.DictCursorMixin,
    query: str,
    params: Iterable[dict[str, str]] = None,
) -> Iterable[dict[str, Any]]:
    """
    Run a query and return an iterator of result rows
    This iterator needs to be exhausted before further queries can be made using the underlying connection.
    """
    logger.debug(f'Query: {query}')
    if params:
        params = tuple(params)
        logger.debug(f'Query parameters: {params}')
    cursor.execute(query, params)
    return iter(cursor)

def query_singleton(
    cursor: pymysql.cursors.DictCursorMixin,
    query: str,
    params: Iterable[dict[str, str]] = None,
) -> dict[str, Any]:
    """Run a query with exactly one result row."""
    [result] = query_iterator(cursor, query, params)
    return result

# Commit message key-value coding.

# Used by the following two functions.
commit_msg_key_code = PercentCode(reserved = map(ord, ['\0', '\n', ':']))
commit_msg_value_code = PercentCode(reserved = map(ord, ['\0', '\n']))

def commit_message_encode(values: dict[str, str]) -> bytes:
    """Encode metadata in the commit message."""
    return b''.join(b''.join([
        commit_msg_key_code.encode(key.encode('utf8')),
        b': ',
        commit_msg_value_code.encode(value.encode('utf8')),
        b'\n',
    ]) for (key, value) in values.items())

def commit_message_decode(msg: bytes) -> dict[str, str]:
    """Decode metadata in the commit message."""
    def f(line):
        key: str
        value: str
        (key, value) = line.split(b': ', 1)
        return (
            commit_msg_key_code.decode(key).decode('utf8'),
            commit_msg_value_code.decode(value).decode('utf8'),
        )

    return dict(map(f, msg.splitlines()))

Position = Tuple[datetime, int]
Position.__doc__ = """Positions represent a total ordering of revisions in time."""

def commit_values_position_encode(position: Position) -> Iterable[Tuple[str, str]]:
    (update_date, id) = position
    yield ('Revision ID', str(id))
    yield ('Update date', str(update_date))

def commit_values_position_decode(values: dict[str, str]) -> Position:
    return (datetime.fromisoformat(values['Update date']), int(values['Revision ID']))

# git functions.

git_script_identity: bytes = git.identity_with_empty_email(b'instiki2git')

def git_commit(
    repo: dulwich.repo.Repo,
    entries: Iterable[Tuple[list[bytes], dulwich.objects.ShaFile]],
    commit_values: Iterable[Tuple[str, str]],
    author: str = None,
    author_time: datetime = None,
) -> NoReturn:
    """
    Perform a commit.
    The message is formed by encoding the given commit values.
    Entries are iterated over before the commit values.
    An optional author (without email) can be given.
    """
    commit_prev: dulwich.repo.Commit = git.get_commit(repo, repo.head())
    logger.debug(f'Previous commit: {commit_prev.id}')

    logger.debug('Creating tree.')
    tree = repo.get_object(commit_prev.tree)
    for (path, obj) in entries:
        tree = git.tree_replace_nested(repo, tree, path, obj)

    logger.debug('Committing.')
    repo.do_commit(
        tree = tree.id,
        message = commit_message_encode(dict(commit_values)),
        committer = git_script_identity,
        author = None if author is None else git.identity_with_empty_email(
            git.identity_code.encode(author.encode('utf8'))
        ),
        author_timestamp = None if author_time is None else author_time.timestamp(),
    )

def get_current_position(repo: dulwich.repo.Repo) -> Optional[Position]:
    """
    Return the tuple of (updated_at, id) for the revision encoded by the head commit.
    Returns None if the head commit is the initial commit.
    """
    commit = git.get_commit(repo, repo.head())
    if commit.message.lower().startswith(b'initial commit'):
        return None

    commit_values: dict[str, str] = commit_message_decode(commit.message)
    return commit_values_position_decode(commit_values)

# Revision functions.

def revision_datetime(revision: dict[str, Any], field: str) -> datetime:
    """
    Extract an aware (UTC) datetime object from a given field in the revision.
    We assume datetime fields in the database use UTC.
    """
    return revision[field].replace(tzinfo = timezone.utc)

def revision_position(revision: dict[str, Any]) -> Position:
    return (revision_datetime(revision, 'updated_at'), revision['id'])

# Other helper functions.

def get_web_address(cursor: pymysql.cursors.DictCursorMixin, web_id: int) -> str:
    return query_singleton(cursor, 'select address from webs where id = %s', (web_id,))['address']

def download_page(session: requests.Session, url: str) -> bytes:
    logger.debug(f'Loading {url}.')
    return session.get(url).content

def partition_id(sizes: Iterable[int], id) -> list[Path]:
    """
    Partition the decimal representation of the given id into a relative path.
    The given sizes control how many digits are used for each segment, starting with the least significant.

    Example:
        partition_id([2, 3], 4567) = Path() / '67' / '045'.
    """
    def f() -> Iterable[str]:
        nonlocal id
        for size in sizes:
            (id, key) = divmod(id, pow(10, size))
            yield f'{key:0{size}}'

    return Path().joinpath(*f())

# Main work function.
def load_commit_and_push(
    repo: dulwich.repo.Repo,
    cursor: pymysql.cursors.DictCursorMixin,
    web_id: int,
    horizon: Optional[datetime] = None,
    partition_sizes: list[int] = list(),
    push: bool = False,
    html_mode: bool = False,
    include_ip: bool = False,
    http_url: str = None,
) -> NoReturn:
    """
    Arguments:
    * repo:
        The repository to modify.
        Commits are made to the current branch.
        The index and working directory are not modified.
    * web_id: the id of the web from which to load revisions.
    * cursor:
        The database connection cursor.
        Must return a dictionary for each queried row.
    * horizon:
        If given, only consider revisions with prior update time (updated_at).
        This should be about a minute behind current time.
        This helps prevent missing revision updates with identical revision time.
    * partition_sizes:
        Partition pages into nested subdirectories.
        See partition_id.
    * push:
        If set, push after finishing comitting.
        This requires the current branch to have a default push target set up.
    * html_mode:
        Back up rendered HTML pages instead of sources.
        Note: do note mix source and html repositories.
    * include_ip:
        Whether to include the author IP in the commit message for each revision.
        Only used in source mode.
    * http_url:
        URL prefix to use for html backup.
        Only used in HTML mode.
    """
    pos = get_current_position(repo)
    logger.info(f'Current revision position: {pos}.')

    time_before_query = datetime.now(timezone.utc)
    logger.debug(f'Time before query: {time_before_query}.')

    if html_mode:
        web_address = get_web_address(cursor, web_id)
        logger.debug(f'Web address: {web_address}')

    horizon_msg = f'before {horizon} ' if horizon else ''
    logger.info(f'Loading revisions {horizon_msg}from database.')

    constraints = []
    params = []

    constraints.append('idx.web_id = %s')
    params.append(web_id)

    if pos:
        constraints.append('(idx.updated_at, idx.id) > (%s, %s)')
        params.extend(pos)

    if horizon:
        constraints.append('idx.updated_at < %s')
        params.append(horizon)

    constraint_str = 'where ' + ' and '.join(constraints)
    order_str = 'order by idx.updated_at, idx.id'

    if not html_mode:
        query = f'''\
select r.*, p.* \
from revisions idx force index(index_revisions_on_web_id_and_updated_at_and_id_and_page_id) \
join revisions r on r.id = idx.id \
join pages p on p.id = idx.page_id \
{constraint_str} \
{order_str}\
'''
    else:
        inner_query = f'''\
select id, updated_at, page_id, row_number() over (partition by page_id order by updated_at, id) rank \
from revisions idx force index(index_revisions_on_web_id_and_updated_at_and_id_and_page_id) \
{constraint_str}\
'''
        query = f'''\
select idx.id, idx.updated_at, idx.page_id, p.name \
from ({inner_query}) idx \
inner join pages p on p.id = idx.page_id \
where idx.rank = 1 \
{order_str}\
'''
    revisions = query_iterator(cursor, query, params)

    def git_path(page_id) -> list[bytes]:
        return git.path(Path('pages') / partition_id(partition_sizes, page_id) / str(page_id))

    (updated, revisions) = iter_inhabited(revisions)
    if not updated:
        logger.info('No new revisions found.')
        return

    if not html_mode:
        for revision in revisions:
            id = revision['id']
            logger.info(f'Committing revision {id}.')

            tree = git.add_flat_tree(repo, [
                (b'content.md', revision['content'].encode('utf8')),
                (b'name', revision['name'].encode('utf8'))
            ])

            def commit_values():
                yield from commit_values_position_encode(revision_position(revision))
                yield ('Page name', revision['name'])
                if include_ip:
                    yield ('IP address', revision['ip'])

            git_commit(
                repo = repo,
                entries = [(git_path(revision['page_id']), tree)],
                commit_values = commit_values(),
                author = revision['author'],
                author_time = revision_datetime(revision, 'revised_at'),
            )
    else:
        session = requests.Session()
        address_base = f'{http_url}/{web_address}/show_by_id/'
        num_entries = 0

        def entries():
            nonlocal revision, num_entries
            for revision in revisions:
                num_entries += 1
                page_id = revision['page_id']
                logger.info(f'Updating HTML for page {page_id}')
                logger.debug(f'Revision {revision["id"]}, updated at {revision["updated_at"]}.')

                tree = git.add_flat_tree(repo, [
                    (b'content.html', download_page(session, address_base + str(page_id))),
                    (b'revision_id', str(revision['id']).encode('utf8')),
                    (b'name', revision['name'].encode('utf8')),
                ])
                yield (git_path(page_id), tree)

        # Iterated over after entries.
        def commit_values():
            yield from commit_values_position_encode(revision_position(revision))
            yield ('Comment', f'updated {num_entries} pages')

        git_commit(
            repo = repo,
            entries = entries(),
            commit_values = commit_values(),
            author_time = time_before_query,
        )

    if push:
        logger.info('Pushing repository.')
        dulwich.porcelain.push(repo = repo)

# Command-line interface.
def cli() -> NoReturn:
    parser = argparse.ArgumentParser(
        description = 'A tool for exporting an Instiki installation to a git repository.',
        add_help = False,
    )
    parser.add_argument('--repo', type = Path, metavar = 'DIR', default = Path(), help = '''
Path to a git repository (may be bare) that source revisions or HTML renderings should be comitted to.
For an initial run, the current commit must have message starting with 'initial commit' (up to capitalization).
Defaults to working directory.
''')
    parser.add_argument('--web-id', type = int, metavar = 'ID', required = True, help = 'ID of the web to back up.')
    parser.add_argument(
        '--partition-sizes',
        nargs = '*',
        metavar = 'NUM_DIGITS',
        type = int,
        choices = range(1, 5),
        default = [],
        help = '''
Partition pages into nested subdirectories.
The given sizes define how many digits to use at each level, starting with the least significant.
Example: --partition 2 3 stores page 4567 at pages/67/045/4567.
''')
    parser.add_argument('--push', action = 'store_true', help = '''
Push after finishing comitting.
For this to work, the current branch must have a default push remote set up.
''')
    parser.add_argument('--html', action = 'store_true', help = 'Back up HTML instead of source.')

    g = parser.add_argument_group(title = 'source backup options')
    parser.add_argument('--include-ip', action = 'store_true', help = '''
Include author IP in the commit message for each revision.
g''')

    g = parser.add_argument_group(title = 'HTML backup options')
    g.add_argument('--http-url', type = str, metavar = 'URL', default = 'http://localhost', help = '''
Base URL for downloading pages.
HTML mode only.
Defaults to http://localhost.
''')

    g = parser.add_argument_group(title = 'horizon options')
    g.add_argument('--safety-interval', type = int, metavar = 'SECONDS', default = 300, help = '''
Only consider revisions updated more than the given time interval in the past (default: 300s).
This helps prevent missing revisions that arrive out of order in the database or have identical revision time.
''')
    g.add_argument('--horizon', type = int, help = '''
    Only consider revisions older than the given UTC timestamp.
    Overrides `--safety-interval`.
''')

    g = parser.add_argument_group(title = 'database connection')
    g.add_argument('--host', type = str)
    g.add_argument('--port', type = int)
    g.add_argument('--unix-socket', type = Path, metavar = 'PATH', help = 'Alternative to host and port.')
    g.add_argument('--user', type = str, help = 'Not needed when authenticating via Unix domain socket.')
    g.add_argument('--password', type = str)
    g.add_argument('--database', type = str, required = True, help = 'Required.')

    g = parser.add_argument_group(title = 'help and debugging')
    g.add_argument('-h', '--help', action = 'help', help = 'Show this help message and exit.')
    g.add_argument('-v', '--verbose', action = 'count', default = 0, help = '''
Print informational (specify once) or debug (specify twice) messages on stderr.
''')

    # Parse arguments.
    args = parser.parse_args()

    # Configure logging.
    logging.basicConfig()
    logger.setLevel({
        0: logging.WARNING,
        1: logging.INFO,
    }.get(args.verbose, logging.DEBUG))

    # Compute horizon.
    if args.horizon is not None:
        horizon = datetime.utcfromtimestamp(args.horizon)
    else:
        horizon = datetime.now() - timedelta(minutes = args.safety_interval)
    logger.debug(f'Horizon: {horizon}')

    logger.info('Reading repository.')
    logger.debug(f'Repository path: {args.repo}')
    repo = dulwich.repo.Repo(str(args.repo))

    logger.info('Connecting to database.')
    connection: pymysql.Connection = pymysql.connect(
        host = args.host,
        port = args.port,
        unix_socket = None if args.unix_socket is None else bytes(args.unix_socket),
        user = args.user,
        password = args.password,
        database = args.database,
        cursorclass = pymysql.cursors.SSDictCursor,
        charset = 'utf8',
        use_unicode = True,
    )
    cursor: pymysql.cursors.DictCursorMixin = connection.cursor()

    load_commit_and_push(
        repo,
        cursor,
        args.web_id,
        horizon = horizon,
        partition_sizes = args.partition_sizes,
        push = args.push,
        http_url = args.http_url,
        include_ip = args.include_ip,
        html_mode = args.html
    )
