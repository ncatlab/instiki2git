import argparse
from datetime import datetime, timedelta, timezone
import dulwich.objects
import dulwich.repo
import logging
from pathlib import Path
import pymysql.cursors
import requests
from typing import Any, Iterable, NoReturn, Optional, Tuple

from instiki2git.percent_code import PercentCode


logger: logging.Logger = logging.getLogger(__name__)

# Database queries

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

def get_head(repo: dulwich.repo.Repo) -> Optional[bytes]:
    try:
        return repo.head()
    except KeyError:
        return None

def get_commit(repo: dulwich.repo.Repo, sha: bytes) -> dulwich.repo.Commit:
    sha_file: dulwich.objects.ShaFile = repo.get_object(sha)
    if not isinstance(sha_file, dulwich.repo.Commit):
        raise TypeError('does not refer to a commit: {sha.decode()}')
    return sha_file

def git_add_file(repo: dulwich.repo.Repo, path: Path, content: bytes | str) -> NoReturn:
    """
    Create a file in the given repository and stage it.
    This creates all needed parent directories.

    Note: the given path must be relative to the repository path.
    """
    for ancestor in reversed(path.parents):
        ancestor.mkdir(exist_ok = True)

    if isinstance(content, str):
        path.write_text(content, encoding = 'utf8')
    else:
        path.write_bytes(content)

    repo.stage(path)

# These are the reserved bytes in git commit authors and committers.
git_identity_code = PercentCode(reserved = map(ord, ['\0', '\n', '<', '>']))

def git_identity_with_empty_email(name: bytes) -> bytes:
    """Format a git user identity without an email address."""
    return name + b' <>'

git_script_identity: bytes = git_identity_with_empty_email(b'instiki2git')

def git_commit(
    repo: dulwich.repo.Repo,
    commit_values: Iterable[Tuple[str, str]],
    author: str = None,
    author_time: datetime = None,
) -> NoReturn:
    """
    Perform a commit.
    The message is formed by encoding the given commit values.
    An optional author (without email) can be given.
    """
    repo.do_commit(
        message = commit_message_encode(dict(commit_values)),
        committer = git_script_identity,
        author = None if author is None else git_identity_with_empty_email(
            git_identity_code.encode(author.encode('utf8'))
        ),
        author_timestamp = None if author_time is None else author_time.timestamp(),
    )

def get_current_position(repo: dulwich.repo.Repo) -> Optional[Position]:
    """
    Return the tuple of (updated_at, id) for the revision encoded by the head commit.
    Returns None if there is no head (e.g. if the repository is empty).
    """
    ref: bytes = get_head(repo)
    if ref:
        commit: dulwich.objects.Commit = get_commit(repo, ref)
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

def download_page(url: str) -> bytes:
    logger.debug(f'Loading {url}.')
    return requests.get(url).content

# Main work function.
def load_commit_and_push(
    repo: dulwich.repo.Repo,
    cursor: pymysql.cursors.DictCursorMixin,
    web_id: int,
    horizon: Optional[datetime] = None,
    include_ip: bool = False,
    http_url: str = None,
    html_mode: bool = False,
) -> NoReturn:
    """
    Arguments:
    * repo:
        The repository to modify.
        Commits are made to the current branch.
    * web_id: the id of the web from which to load revisions.
    * cursor:
        The database connection cursor.
        Must return a dictionary for each queried row.
    * horizon:
        If given, only consider revisions with prior update time (updated_at).
        This should be about a minute behind current time.
        This helps prevent missing revision updates with identical revision time.
    * include_ip:
        Whether to include the author IP in the commit message for each revision.
        Only used in source mode.
    * http_url:
        URL prefix to use for html backup.
        Only used in HTML mode.
    * html_mode:
        Back up rendered HTML pages instead of sources.
        Note: do note mix source and html repositories.
    """
    pos = get_current_position(repo)
    logger.info(f'Current revision position: {pos}.')

    time_before_query = datetime.now(timezone.utc)
    logger.debug(f'Time before query: {time_before_query}.')

    horizon_msg = f'before {horizon} ' if horizon else ''
    logger.info(f'Loading revisions {horizon_msg}from database.')

    if not html_mode:
        fields = 'r.*, p.*'
    else:
        # Exclude r.content.
        # Not needed and main source of space consumption.
        fields = 'r.id, r.updated_at, r.page_id, p.*'

    # SQL printing, so sad.
    query = f'''\
select {fields} \
from revisions idx force index(index_revisions_on_web_id_and_updated_at_and_id_and_page_id) \
join revisions r on r.id = idx.id \
join pages p on p.id = idx.page_id \
where idx.web_id = %s\
'''
    params = [web_id]
    if pos:
        query += ' and (idx.updated_at, idx.id) > (%s, %s)'
        params.extend(pos)
    if horizon:
        query += ' and idx.updated_at < %s'
        params.append(horizon)
    revisions = query_iterator(cursor, query, params)

    updated = False
    if not html_mode:
        for revision in revisions:
            updated = True
            id = revision['id']
            logger.info(f'Committing revision {id}.')

            for (filename, content) in [
                ('content.md', revision['content']),
                ('name', revision['name'])
            ]:
                git_add_file(repo, Path('pages') / str(revision['page_id']) / filename, content)

            def commit_values():
                yield from commit_values_position_encode(revision_position(revision))
                yield ('Page name', revision['name'])
                if include_ip:
                    yield ('IP address', revision['ip'])

            git_commit(repo, commit_values(), revision['author'], revision_datetime(revision, 'revised_at'))
    else:
        to_update = dict()
        last_revision = None
        for revision in revisions:
            to_update[revision['page_id']] = revision
            last_revision = revision

        if last_revision:
            updated = True
            address_base = http_url + '/' + get_web_address(web_id) + '/show_by_id/'
            for (page_id, revision) in to_update.items():
                logger.info(f'Updating HTML for page {page_id}')
                for (filename, content) in [
                    ('content.html', download_page(address_base + page_id)),
                    ('revision_id', str(revision['id'])),
                    ('name', revision['name']),
                ]:
                    git_add_file(repo, Path('pages') / str(page_id) / filename, content)

            def commit_values():
                yield from commit_values_position_encode(revision_position(last_revision))
                yield ('Comment', f'updated {len(to_update)} pages')

            git_commit(repo, commit_values(), time_before_query)

    if updated:
        logger.info('Pushing repository.')
        dulwich.porcelain.push(repo = repo)

# Command-line interface
def cli() -> NoReturn:
    parser = argparse.ArgumentParser(
        description = 'A tool for exporting an Instiki installation to a git repository.',
        add_help = False,
    )
    parser.add_argument('--repo', type = Path, metavar = 'DIR', default = Path(), help = '''
defaults to working directory
''')
    parser.add_argument('--web-id', type = int, metavar = 'ID', required = True, help = 'ID of the web to back up')
    parser.add_argument('--html', action = 'store_true', help = 'back up html instead of source')
    parser.add_argument('--include-ip', action = 'store_true', help = '''
Include author IP in the commit message for each revision.
Source mode only.
''')

    g = parser.add_argument_group(title = 'HTML backup options')
    g.add_argument('--http-url', type = str, metavar = 'URL', default = 'http://localhost', help = '''
Base URL for downloading pages.
HTML mode only.
Defaults to http://localhost.
''')

    g = parser.add_argument_group(title = 'horizon options')
    g.add_argument('--safety-interval', type = int, metavar = 'SECONDS', default = 300, help = '''
Only consider revisions older than the given time interval (default: 300s).
This helps prevent missing revisions that arrive out of order in the database or have identical revision time.
''')
    g.add_argument('--horizon', type = int, help = '''
    Only consider revisions older than the given UTC timestamp.
    Overrides `--safety-interval`.
''')

    g = parser.add_argument_group(title = 'database connection')
    g.add_argument('--host', type = str)
    g.add_argument('--port', type = int)
    g.add_argument('--unix_socket', type = Path, metavar = 'PATH', help = 'alternative to host and port')
    g.add_argument('--user', type = str, help = 'not needed when authenticating via Unix domain socket')
    g.add_argument('--password', type = str)
    g.add_argument('--database', type = str, required = True, help = 'required')

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
    repo = dulwich.repo.Repo(args.repo)

    logger.info('Connecting to database.')
    connection: pymysql.Connection = pymysql.connect(
        host = args.host,
        port = args.port,
        unix_socket = args.unix_socket,
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
        include_ip = args.include_ip,
        http_url = args.http_url,
        html_mode = args.html
    )
