import os,errno
import time
from datetime import datetime, timezone
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

def get_db_conn(db_config):
  return pymysql.connect(host=db_config["host"],
                         user=db_config["user"],
                         password=db_config["password"],
                         db=db_config["db"],
                         charset=db_config["charset"],
                         cursorclass=pymysql.cursors.SSDictCursor,
                         use_unicode=True)

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
  db_conn = get_db_conn(db_config)
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
  db_conn = get_db_conn(db_config)
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

# These are the reserved bytes in git commit authors and committers.
git_identity_code = PercentCode(reserved = map(ord, ['\0', '\n', '<', '>']))

def commit_revision(repo: dulwich.repo.Repo, revision: dict):
  """Commit a revision to a git repository."""
  id = revision['id']
  logger.info(f'Committing revision {id}.')

  dir_pages = Path(repo.path) / 'pages'
  dir_pages.mkdir(exist_ok = True)

  def add_file(filename: str, content: bytes | str):
    path = dir_pages / filename
    if isinstance(content, str):
      path.write_text(content, encoding = 'utf8')
    else:
      path.write_bytes(content)
    repo.stage(Path('pages') / filename)

  page_id = revision['page_id']
  add_file(f'{page_id}.md', revision['content'])
  add_file(f'{page_id}.meta', commit_message_encode({'Name': revision['name']}))

  # git insists on an email address, so we add an empty one.
  def with_empty_email(xs):
    return xs + b' <>'

  # We assume datetime fields in the database use UTC.
  revision_date = revision['revised_at'].replace(tzinfo = timezone.utc)

  repo.do_commit(
    message = commit_message_encode({
      'Revision ID': str(revision['id']),
      'Revision date': str(revision_date),
      'Page name': revision['name'],
      'Author': revision['author'],
      'IP address': revision['ip'],
    }),
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
  """
  pos = get_current_position(repo)
  logger.info(f'Current revision position: {pos}')

  # So sad (for multiple reasons).
  query = f'''\
select r.*, p.* \
from revisions idx force index(quad) \
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
    commit_revision(repo, revision)

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
  db_config = {"host": config_parser.get("database", "host"),
    "db": config_parser.get("database", "db"),
    "user": config_parser.get("database", "user"),
    "password": config_parser.get("database", "password"),
    "charset": config_parser.get("database", "charset")}
  web_id = config_parser.get("web", "id")
  web_http_url = config_parser.get("web", "http_url")
  return {
    "repo_path": repo_path,
    "html_repo_path": html_repo_path,
    "db_config": db_config,
    "web_id": web_id,
    "web_http_url": web_http_url}

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
@click.option('-v', '--verbose', count = True)
def cli(config_file, verbose):
  setup_logging(verbose)

  config = read_config(config_file)
  repo_path = os.path.abspath(config["repo_path"])
  db_config = config["db_config"]
  web_id = config["web_id"]

  logger.info('Reading repository.')
  repo = dulwich.repo.Repo(repo_path)

  logger.info('Connecting to database.')
  connection = get_db_conn(db_config)

  with connection.cursor() as cursor:
    load_and_commit_new_revisions(repo, cursor, web_id)

  logger.info('Pushing repository.')
  dulwich.porcelain.push(repo = repo)

@click.command()
@click.option("--config-file",
  type=click.Path(exists = True, path_type = Path),
  default=Path('~/.instiki2git').expanduser(),
  help="Path to configuration file.")
@click.option("--latest-download-file",
  type=click.Path(path_type = Path),
  default=Path('/tmp/instiki2git-html'),
  help="Path to a file containing the time of the last html download.")
@click.option("--populate",
  is_flag=True,
  default=False,
  help="Run in populate mode")
@click.option('-v', '--verbose', count = True)
def cli_html(config_file, latest_download_file, populate, verbose):
  setup_logging(verbose)

  config = read_config(config_file)
  repo_path = os.path.abspath(config["repo_path"])
  html_repo_path = os.path.abspath(config["html_repo_path"])
  db_config = config["db_config"]
  web_id = config["web_id"]
  web_http_url = config["web_http_url"]
  if web_http_url.endswith("/"):
    web_http_url = web_http_url[:-1]
  if populate:
    html_repo_populate(repo_path, html_repo_path, web_http_url,
      latest_download_file)
  else:
    html_repo_update(html_repo_path, db_config, web_id, web_http_url,
      latest_download_file)
