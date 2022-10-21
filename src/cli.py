import os,errno
import time
from datetime import datetime, timedelta, timezone
import configparser
import pymysql.cursors
import dulwich.repo
import dulwich.porcelain
import click
import requests
import re
from pathlib import Path
from typing import Any, Iterable, NoReturn, Optional, Tuple
import logging
import sys

from instiki2git.percent_code import PercentCode


logger = logging.getLogger(__name__)

def get_db_conn(**kwargs):
  return pymysql.connect(
    **kwargs,
    cursorclass=pymysql.cursors.SSDictCursor,
    charset = 'utf8',
    use_unicode = True,
  )

def query_iterator(cursor, query: str, params: Iterable[Any] = None):
  logger.debug(f'Query: {query}')
  if params:
    params = tuple(params)
    logger.debug(f'Query parameters: {params}')
  cursor.execute(query, params)
  return cursor.fetchall()

def load_new_revisions_by_time(db_config, web_id, latest_revision_time=None):
  """
  Load all revisions, using the given database configuration to connect.  If a
  `latest_revision_time` is provided, only loads revisions committed after that.
  """
  db_conn = get_db_conn(**db_config)
  try:
    with db_conn.cursor() as cursor:
      date_selector = ""
      if latest_revision_time:
        time = latest_revision_time.strftime('%Y-%m-%d %H:%M:%S')
        date_selector = " AND r.updated_at >= '%s'" % time
      query = ("SELECT r.id,r.page_id,r.author,r.ip,r.revised_at,r.content,p.name"
        " FROM revisions r INNER JOIN pages p ON (r.page_id=p.id)"
        " WHERE r.web_id=%s%s"
        " ORDER BY r.id") % (web_id, date_selector)
      cursor.execute(query)
      revisions = cursor.fetchall()
  finally:
    db_conn.close()
  return revisions

def load_revisions_between(db_config, web_id, start_after, stop_at):
  """
  Load all revisions newer than the given time `start_after` and older than or
  equal to `stop_at`, using the given database configuration to connect.
  `start_after` and `stop_at` have to be in the format '%Y-%m-%d %H:%M:%S'.
  """
  db_conn = get_db_conn(**db_config)
  try:
    with db_conn.cursor() as cursor:
      query = ("SELECT r.id,r.page_id,r.author,r.ip,r.revised_at,r.content,p.name"
        " FROM revisions r INNER JOIN pages p ON (r.page_id=p.id)"
        " WHERE r.web_id=%s"
        " AND r.updated_at>'%s'"
        " AND r.updated_at<='%s'"
        " ORDER BY r.id") % (web_id, start_after, stop_at)
      cursor.execute(query)
      revisions = cursor.fetchall()
  finally:
    db_conn.close()
  return revisions

# Used by the following two functions.
commit_data_key_code = PercentCode(reserved = map(ord, ['\0', '\n', ':']))
commit_data_value_code = PercentCode(reserved = map(ord, ['\0', '\n']))

def commit_message_encode(values: dict[str, str]) -> bytes:
  """Encode metadata in the commit message."""
  return b''.join(b''.join([
      commit_data_key_code.encode(key.encode('utf8')),
      b': ',
      commit_data_value_code.encode(value.encode('utf8')),
      b'\n',
    ]) for (key, value) in values.items()
  )

def commit_message_decode(msg: bytes) -> dict[str, str]:
  """Decode metadata in the commit message."""
  def f(line):
    (key, value) = line.split(b': ', 1)
    return (
      commit_data_key_code.decode(key).decode('utf8'),
      commit_data_value_code.decode(value).decode('utf8'),
    )

  return dict(map(f, msg.splitlines()))

def add_file(repo: dulwich.repo.Repo, path: Path, content: bytes | str) -> NoReturn:
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

def commit_revision(repo: dulwich.repo.Repo, revision: dict, include_ip: bool):
  """Commit a revision to a git repository."""
  id = revision['id']
  logger.info(f'Committing revision {id}.')

  path = Path('pages') / str(revision['page_id'])
  add_file(repo, path / 'content.md', revision['content'])
  add_file(repo, path / 'name', revision['name'])

  # git insists on an email address, so we add an empty one.
  def with_empty_email(xs):
    return xs + b' <>'

  # We assume datetime fields in the database use UTC.
  revision_date = revision['revised_at'].replace(tzinfo = timezone.utc)

  def fields():
    yield ('Revision ID', str(revision['id']))
    yield ('Page name', revision['name'])
    if include_ip:
      yield ('IP address', revision['ip'])

  repo.do_commit(
    message = commit_message_encode(dict(fields())),
    committer = with_empty_email(b'instiki2git'),
    author = with_empty_email(
      git_identity_code.encode(revision['author'].encode('utf8'))
    ),
    author_timestamp = revision_date.timestamp(),
    author_timezone = 0,
  )

def get_head(repo: dulwich.repo.Repo) -> Optional[bytes]:
  try:
    return repo.head()
  except KeyError:
    return None

def get_commit(repo: dulwich.repo.Repo, sha: bytes) -> dulwich.repo.Commit:
  x = repo.get_object(sha)
  if not isinstance(x, dulwich.repo.Commit):
    raise TypeError('does not refer to a commit: {sha.decode()}')
  return x

def get_current_position(repo: dulwich.repo.Repo) -> Optional[Tuple[datetime, int]]:
  """
  Return the tuple of (revised_at, id) for the revision encoded by the head commit.
  Returns None if there is no head (e.g. if the repository is empty).
  """
  ref = get_head(repo)
  if ref:
    commit = get_commit(repo, ref)
    revision_date = datetime.fromtimestamp(commit.author_time)
    metadata = commit_message_decode(commit.message)
    return (revision_date, metadata['Revision ID'])

def load_and_commit_new_revisions(
    repo: dulwich.repo.Repo,
    cursor,
    web_id: int,
    horizon: Optional[datetime] = None,
    include_ip: bool = False,
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
      If given, only consider revisions with prior revision time (revised_at).
      This should be about a minute behind current time.
      This helps prevent missing revision updates with identical revision time.
  * include_ip: whether to include the author IP in the commit message for each revision.
  """
  pos = get_current_position(repo)
  logger.info(f'Current revision position: {pos}')

  # So sad (for multiple reasons).
  query = f'''\
select r.*, p.* \
from revisions idx force index(index_revisions_on_web_id_and_revised_at_and_id_and_page_id) \
join revisions r on r.id = idx.id \
join pages p on p.id = idx.page_id \
where idx.web_id = %s\
'''
  params = [web_id]
  if pos:
    query += ' and (idx.revised_at, idx.id) > (%s, %s)'
    params.extend(pos)
  if horizon:
    query += ' and idx.revised_at < %s'
    params.append(horizon)

  horizon_msg = f'before {horizon} ' if horizon else ''
  logger.info(f'Loading revisions {horizon_msg}from database.')
  for revision in query_iterator(cursor, query, params):
    commit_revision(repo, revision, include_ip)

# begin html repository functions

def read_latest_download_file(latest_download_file):
  try:
    datetime.fromtimestamp(latest_download_file.read_text())
  except FileNotFoundError:
    return 0

def write_latest_download_file(latest_download_file):
  latest_download_file.write_text(str(time.time()))

def download_html_page(page_id, html_repo_path, web_http_url):
  r = requests.get("%s/show_by_id/%s" % (web_http_url, page_id))
  page_path = html_repo_path / 'pages' / f'{page_id}.html'
  page_path.write_bytes(r.content)

def download_and_stage_html_pages(pages_list, html_repo, html_repo_path,
  web_http_url):
  for p in pages_list:
    download_html_page(p, html_repo_path, web_http_url)
  html_repo.stage(map(lambda p: "pages/%d.html" % p, pages_list))

def html_repo_populate(repo_path, html_repo_path, web_http_url,
  latest_download_file):
  """
  This is meant to be used in case the usual repository is already set up, and
  but the html repository is not.  It will download the latest html version of
  each page and save them all to the html repository as one commit.  It will
  ignore pages that already have html versions downloaded.
  """
  write_latest_download_file(latest_download_file)
  pages_list = map(lambda f: f[:-3],
                   filter(lambda f: (f.endswith(".md") and
                            not os.path.isfile(
                              os.path.join(html_repo_path, "pages",
                                           "%s.html" % f[:-3]))),
                          os.listdir(os.path.join(repo_path, "pages"))))
  html_repo = dulwich.repo.Repo(html_repo_path)
  download_and_stage_html_pages(pages_list, html_repo, html_repo_path,
    web_http_url)
  html_repo.do_commit("Added current html versions.",
    "instiki2git <>")

def html_repo_update(html_repo_path, db_config, web_id, web_http_url,
  latest_download_file):
  latest_download_time = read_latest_download_file(latest_download_file)
  write_latest_download_file(latest_download_file)
  revs = load_new_revisions_by_time(db_config, web_id, latest_download_time)
  pages_list = {r["page_id"] for r in revs}
  if pages_list:
    html_repo = dulwich.repo.Repo(html_repo_path)
    download_and_stage_html_pages(pages_list, html_repo, html_repo_path,
      web_http_url)
    html_repo.do_commit("Updated %d pages." % len(pages_list),
      "instiki2git <>")

# end html repository functions

def read_config(config_file):
  config_parser = configparser.RawConfigParser()
  config_parser.read(config_file)
  repo_path = config_parser.get("repository", "path")
  html_repo_path = config_parser.get("html_repository", "path")
  return {
    "repo_path": repo_path,
    "html_repo_path": html_repo_path,
  }

def setup_logging(verbose):
  logging.basicConfig()
  logger.setLevel({
    0: logging.WARNING,
    1: logging.INFO,
  }.get(verbose, logging.DEBUG))

@click.command()
@click.option("--config-file",
  type=click.Path(exists = True, path_type = Path),
  default=os.path.expanduser("~/.instiki2git"),
  help="Path to configuration file.")
@click.option('-h', '--host', type = str)
@click.option('-p', '--port', type = int)
@click.option('-S', '--unix_socket', type = str)
@click.option('-u', '--user', type = str)
@click.option('-p', '--password', type = str)
@click.option('-d', '--database', type = str, required = True)
@click.option('-w', '--web-id', type = int, required = True)
@click.option('--safety-interval', type = int, default = 300, show_default = True)
@click.option('--include-ip', is_flag = True)
@click.option('-v', '--verbose', count = True)
def cli(config_file, host, port, unix_socket, user, password, database, web_id, safety_interval, include_ip, verbose):
  setup_logging(verbose)
  logger.debug(f'Host: {host}')
  logger.debug(f'User: {user}')
  # Do not log password.
  #logger.debug(f'Password': {password}')
  logger.debug(f'Database: {database}')
  logger.debug(f'Safety interval (in seconds): {safety_interval}')
  logger.debug(f'Include IPs: {include_ip}')
  logger.debug(f'Verbosity level: {verbose}')

  config = read_config(config_file)
  repo_path = os.path.abspath(config["repo_path"])

  logger.info('Reading repository.')
  repo = dulwich.repo.Repo(repo_path)

  logger.info('Connecting to database.')
  connection = get_db_conn(
    host = host,
    port = port,
    unix_socket = unix_socket,
    user = user,
    password = password,
    database = database
  )

  with connection.cursor() as cursor:
    load_and_commit_new_revisions(
      repo,
      cursor,
      web_id,
      horizon = datetime.now() - timedelta(minutes = safety_interval),
      include_ip = include_ip
    )

  logger.info('Pushing repository.')
  dulwich.porcelain.push(repo = repo)

@click.command()
@click.option("--config-file",
  type=click.Path(exists = True, path_type = Path),
  default=Path('~/.instiki2git').expanduser(),
  help="Path to configuration file.")
@click.option('-h', '--host', type = str)
@click.option('-p', '--port', type = int)
@click.option('-S', '--unix-socket', type = str)
@click.option('-u', '--user', type = str)
@click.option('-p', '--password', type = str)
@click.option('-d', '--database', type = str, required = True)
@click.option('-w', '--web-id', type = int, required = True)
@click.option('-W', '--web-http-url', type = str, required = True)
@click.option("--latest-download-file",
  type=click.Path(path_type = Path),
  default=Path('/tmp/instiki2git-html'),
  help="Path to a file containing the time of the last html download.")
@click.option("--populate",
  is_flag=True,
  default=False,
  help="Run in populate mode")
@click.option('-v', '--verbose', count = True)
def cli_html(config_file, host, port, unix_socket, user, password, database, web_id, web_http_url, latest_download_file, populate, verbose):
  setup_logging(verbose)

  config = read_config(config_file)
  repo_path = os.path.abspath(config["repo_path"])
  html_repo_path = os.path.abspath(config["html_repo_path"])
  db_config = {
    'host': host,
    'port': port,
    'unix_socket': unix_socket,
    'user': user,
    'password': password,
    'database': database,
  }
  if web_http_url.endswith("/"):
    web_http_url = web_http_url[:-1]
  if populate:
    html_repo_populate(repo_path, html_repo_path, web_http_url,
      latest_download_file)
  else:
    html_repo_update(html_repo_path, db_config, web_id, web_http_url,
      latest_download_file)
