import os,errno
import time, datetime
import configparser
import pymysql.cursors
from dulwich.repo import Repo as git_repo
from dulwich.porcelain import push as git_push
import click
import requests
import re

def load_repo(repo_path):
  """
  Load git repository at the given path, initializing if necessary.

  Inputs:
    * repo_path (str): a path to the output repository
  Output:
    * repo (dulwich.repo.Repo): the git repository
  """
  try:
    os.mkdir(repo_path)
  except OSError as e:
    if e.errno == errno.EEXIST:
      pass
    else:
      raise e
  try:
    repo = git_repo.init(repo_path)
  except OSError as e:
    if e.errno == errno.EEXIST:
      repo = git_repo(repo_path)
    else:
      raise e
  try:
    os.mkdir(os.path.join(repo_path, "pages"))
  except OSError as e:
    if e.errno == errno.EEXIST:
      pass
    else:
      raise e
  return repo

def get_db_conn(db_config):
  db_conn = pymysql.connect(host=db_config["host"],
                            user=db_config["user"],
                            password=db_config["password"],
                            db=db_config["db"],
                            charset=db_config["charset"],
                            cursorclass=pymysql.cursors.DictCursor,
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

def commit_revisions_to_repo(repo, revisions):
  """
  Commits a set of revisions to the given git repository.

  Inputs:
    * repo (dulwich.repo.Repo): the git repository
    * revisions ([dict]): the revisions, as a list of dictionaries
    * latest_revision_file (file): the file where latest_revision_id is stored
  Output: none
  """
  for rev in revisions:
    with open("%s/pages/%s.md" % (repo.path, rev["page_id"]), "w") as f:
      f.write(rev["content"])
    repo.stage(["pages/%d.md" % rev["page_id"]])
    with open("%s/pages/%s.meta" % (repo.path, rev["page_id"]), "w") as f:
      f.write("Name: %s\n" % rev["name"])
    repo.stage([
      "pages/%d.md" % rev["page_id"],
      "pages/%d.meta" % rev["page_id"]])
    commit_msg = """Revision ID: %s
Revision date: %s
Page name: %s
Author: %s
IP address: %s""" % (rev["id"], rev["revised_at"], rev["name"], rev["author"],
      rev["ip"])

    # non-alphanumeric characters can cause git errors
    # so we remove them with this regexp I found on stackoverflow
    # Also: dulwich and github insist on an email address,
    #       we add an empty one with '<>'
    commit_author = "%s <>" % re.sub(r'[^\w]', ' ', rev["author"])

    repo.do_commit(
      message=commit_msg.encode("utf8"),
      committer=b"instiki2git script <>",  # without this, a committer is generated
      author=commit_author.encode("utf8"))
    
    with click.open_file(latest_revision_file, 'w') as f_:
      f_.write(str(rev["id"]))

def read_latest_revision_id(latest_revision_file):
  if os.path.exists(latest_revision_file):
    with open(latest_revision_file, 'r') as f:
      try:
        return int(f.read())
      except ValueError:
        return 0
  else:
    return 0

def load_and_commit_new_revisions(repo_path, db_config, web_id, \
                                  latest_revision_file):
  latest_revision_id = read_latest_revision_id(latest_revision_file)
  twentyseven_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=27)
  revs = load_new_revisions_before(db_config, web_id, latest_revision_id, twentyseven_hours_ago)
  repo = load_repo(repo_path)
  commit_revisions_to_repo(repo, revs, latest_revision_file)
  git_push(repo=repo)

# begin html repository functions

def read_latest_download_file(latest_download_file):
  if os.path.exists(latest_download_file):
    with open(latest_download_file, 'r') as f:
      try:
        return datetime.datetime.fromtimestamp(int(f.read()))
      except ValueError:
        return 0
  else:
    return 0

def write_latest_download_file(latest_download_file):
  with open(latest_download_file, 'w') as f:
    f.write("%d" % time.time())

def download_html_page(page_id, html_repo_path, web_http_url):
  r = requests.get("%s/show_by_id/%s" % (web_http_url, page_id))
  page_path = os.path.join(html_repo_path, "pages", "%s.html" % page_id)
  with open(page_path, "wb") as f:
    f.write(r.content)

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
  type=click.Path(exists=True),
  default=os.path.expanduser("~/.instiki2git"),
  help="Path to configuration file.")
@click.option("--latest-revision-file",
  type=click.Path(),
  default="/tmp/instiki2git",
  help="Path to a file containing the ID of the last committed revision.")
def cli(config_file, latest_revision_file):
  config = read_config(config_file)
  repo_path = os.path.abspath(config["repo_path"])
  db_config = config["db_config"]
  web_id = config["web_id"]
  load_and_commit_new_revisions(repo_path, db_config, web_id,
    latest_revision_file)

@click.command()
@click.option("--config-file",
  type=click.Path(exists=True),
  default=os.path.expanduser("~/.instiki2git"),
  help="Path to configuration file.")
@click.option("--latest-download-file",
  type=click.Path(),
  default="/tmp/instiki2git-html",
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
