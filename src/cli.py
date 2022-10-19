import os,errno
import time, datetime
import configparser
import pymysql.cursors
from dulwich.repo import Repo as git_repo
from dulwich.porcelain import push as git_push
import click
import requests
import re
from pathlib import Path

from instiki2git.percent_code import PercentCode

def load_repo(repo_path: Path):
  """
  Load git repository at the given path, initializing if necessary.

  Inputs:
    * repo_path (str): a path to the output repository
  Output:
    * repo (dulwich.repo.Repo): the git repository
  """
  repo_path.mkdir(exist_ok = True)
  try:
    repo = git_repo.init(repo_path)
  except OSError as e:
    if e.errno == errno.EEXIST:
      repo = git_repo(repo_path)
    else:
      raise
  try:
    os.mkdir(os.path.join(repo_path, "pages"))
  except OSError as e:
    if not e.errno == errno.EEXIST:
      raise
  return repo

def get_db_conn(db_config):
  db_conn = pymysql.connect(host=db_config["host"],
                            user=db_config["user"],
                            password=db_config["password"],
                            db=db_config["db"],
                            charset=db_config["charset"],
                            cursorclass=pymysql.cursors.SSDictCursor,
                            use_unicode=True)
  return db_conn

def load_new_revisions(db_config, web_id, latest_revision_id=0):
  """
  Load all revisions, using the given database configuration to connect.  If a
  `latest_revision_id` is provided, only loads revisions committed after that.
  """
  db_conn = get_db_conn(db_config)
  try:
    with db_conn.cursor() as cursor:
      query = ("SELECT r.id,r.page_id,r.author,r.ip,r.revised_at,r.content,p.name"
        " FROM revisions r INNER JOIN pages p ON (r.page_id=p.id)"
        " WHERE r.web_id=%s AND r.id>%s"
        " ORDER BY r.id") % (web_id, latest_revision_id)
      cursor.execute(query)
      revisions = cursor.fetchall()
  finally:
    db_conn.close()
  return revisions

def load_new_revisions_before(db_config, web_id, revision_id, before):
  """
  Load all revisions newer than the given `revision_id`,
  that where 'updated_at' a time before `before`.
  """
  db_conn = get_db_conn(db_config)
  try:
    with db_conn.cursor() as cursor:
      time = before.strftime('%Y-%m-%d %H:%M:%S')
      date_selector = " AND r.updated_at < '%s'" % time
      query = ("SELECT r.id,r.page_id,r.author,r.ip,r.revised_at,r.content,p.name"
        " FROM revisions r INNER JOIN pages p ON (r.page_id=p.id)"
        " WHERE r.web_id=%s%s"
        " AND r.id>%s"
        " ORDER BY r.id") % (web_id, date_selector, revision_id)
      cursor.execute(query)
      revisions = cursor.fetchall()
  finally:
    db_conn.close()
  return revisions

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

def commit_revision_to_repo(repo: git_repo, rev: dict):
  """Commit a revision to a git repository."""
  def add_file(filename: str, content: bytes | str):
    path = Path(repo.path) / 'pages' / filename
    if isinstance(content, str):
      path.write_text(content, encoding = 'utf8')
    else:
      path.write_bytes(content)
    repo.stage(Path('pages') / filename)

  page_id = rev['page_id']
  add_file(f'{page_id}.md', rev['content'])
  add_file(f'{page_id}.meta', commit_message_encode({'Name': rev['name']}))

  # dulwich and github insist on an email address, we add an empty one.
  def with_empty_email(xs):
    return xs + b' <>'

  repo.do_commit(
    message = commit_message_encode({
      'Revision ID': str(rev['id']),
      'Revision date': str(rev['revised_at']),
      'Page name': rev['name'],
      'Author': rev['author'],
      'IP address': rev['ip'],
    }),
    committer = with_empty_email(b'instiki2git'),
    author = with_empty_email(
      git_identity_code.encode(rev['author'].encode('utf8'))
    ),
  )

def read_latest_revision_id(latest_revision_file):
  try:
    return int(latest_revision_file.read_text())
  except FileNotFoundError:
    return 0

def check_time_file(latest_time_file):
  if not latest_time_file.exists():
    raise Exception(f"Given path '{latest_time_file}' of file with time up until which everything is committed does not exist.")

def read_latest_time(latest_time_file):
  check_time_file(latest_time_file)
  file_content = latest_time_file.read_text()
  if file_content == "updating":
    raise Exception("instikit2git didn't properly finish.\n" +
                    "The latest-time-file '{latest_time_file}' still contains 'updating' instead of a time. \n" +
                    "You have to manually figure out what happened and fix it. ")
  try:
    return datetime.datetime.strptime(file_content, '%Y-%m-%d %H:%M:%S')
  except ValueError:
    raise Exception("Time of latest committed revision could not be read. \n" +
                    "Format is '%Y-%m-%d %H:%M:%S'. \n" +
                    "Trailing newline is not supported.")

def write_updating(latest_time_file):
  check_time_file(latest_time_file)
  latest_time_file.write_text('updating')

def write_time_to_file(formatted_time, latest_time_file):
  """
  Write the '%Y-%m-%d %H:%M:%S' formatted time `formatted_time` into the file `latest_time_file`.
  """
  check_time_file(latest_time_file)
  latest_time_file.write_text(formatted_time)


def load_and_commit_new_revisions(repo_path, db_config, web_id,
                                  latest_revision_file,
                                  latest_time_file):
  latest_revision_id = read_latest_revision_id(latest_revision_file)
  latest_committed_time = read_latest_time(latest_time_file)
  write_updating(latest_time_file)

  # the following time should be at least one second behind 'now', to ensure that
  # no updates written after the query in 'load_revisions_between' are missed.
  twentyseven_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=27)

  formatted_end = twentyseven_hours_ago.strftime('%Y-%m-%d %H:%M:%S')
  formatted_start = latest_committed_time.strftime('%Y-%m-%d %H:%M:%S')
  revs = load_revisions_between(db_config, web_id,
                                start_after=formatted_start,
                                stop_at=formatted_end)

  repo = load_repo(repo_path)
  for rev in revs:
    commit_revision_to_repo(repo, rev)
  git_push(repo=repo)
  write_time_to_file(formatted_end, latest_time_file)

# begin html repository functions

def read_latest_download_file(latest_download_file):
  try:
    datetime.datetime.fromtimestamp(latest_download_file.read_text())
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
  html_repo = load_repo(html_repo_path)
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
    html_repo = load_repo(html_repo_path)
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

@click.command()
@click.option("--config-file",
  type=click.Path(exists = True, path_type = Path),
  default=os.path.expanduser("~/.instiki2git"),
  help="Path to configuration file.")
@click.option("--latest-revision-file",
  type=click.Path(path_type = Path),
  default=Path('/tmp/instiki2git'),
  help="Path to a file containing the ID of the last committed revision.")
@click.option("--latest-time-file",
  type=click.Path(path_type = Path),
  default=Path('/tmp/instiki2git-time'),
  help="Path to a file containing the time of the last committed revision update.")
def cli(config_file, latest_revision_file, latest_time_file):
  config = read_config(config_file)
  repo_path = os.path.abspath(config["repo_path"])
  db_config = config["db_config"]
  web_id = config["web_id"]
  load_and_commit_new_revisions(repo_path, db_config, web_id,
                                latest_revision_file, latest_time_file)

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
def cli_html(config_file, latest_download_file, populate):
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
