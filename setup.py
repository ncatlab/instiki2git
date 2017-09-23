from setuptools import setup

setup(name="instiki2git",
  version="0.1",
  description="A script for exporting an Instiki installation to a git repository.",
  url="https://github.com/adeel/instiki2git",
  author="adeel",
  author_email="adeel@yusufzai.me",
  license="MIT",
  include_package_data=True,
  py_modules=["instiki2git"],
  install_requires=["PyMySQL", "dulwich", "click", "requests"],
  entry_points="""
    [console_scripts]
    instiki2git=instiki2git:cli
    instiki2git-html=instiki2git:cli_html
  """)