# instiki2git

A script for exporting an Instiki installation to a git repository.

## Installation

Run the following in the source directory:

    $ pip install .

## Configuration

Place a configuration file at ~/.instiki2git which has the following format:

    # ~/.instiki2git
    [web]
    id = 1

    [repository]
    path = /home/nlab/nlab-content.git

    [database]
    host = localhost
    db = nlab
    user = nlab
    password = xxx
    charset = utf8

## Usage

Run the following:

    $ instiki2git

Optionally you can specify the `--config-file` (`~/.instiki2git` by default) or
`--latest-revision-file` (`/tmp/instiki2git` by default) parameters.