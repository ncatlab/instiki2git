from setuptools import setup

setup(name="instiki2git",
  version="0.1",
  description="A tool for exporting an Instiki installation to a git repository.",
  url="https://github.com/adeel/instiki2git",
  author="adeel",
  author_email="adeel@yusufzai.me",
  license="MIT",
  package_dir={'instiki2git': 'src'},
  install_requires=["PyMySQL", "dulwich", "click", "requests", "configparser"],
  python_requires="~=3.9",
  entry_points="""
    [console_scripts]
    instiki2git=instiki2git.cli:cli
    instiki2git-html=instiki2git.cli:cli_html
  """)
