# instiki2git

A tool for exporting an Instiki installation to a git repository.

## Installation

Run the following in the source directory:
```shell
$ pip install .
```

## Configuration

Via command-line options.

## Usage

See:
```shell
$ instiki2git --help
```

## Details

### Revision ordering

Commits are ordered by database update time (as encoded by the field `updated_at` in the revisions table), with ties decided by revision id.
This is the only ordering that makes sense for a tool that runs repeatedly and whose overall output should not depend on when exactly it is run.
In contrast, revisions in instiki are ordered by the field `revised_at` (revision time).
For this reason, we record the revision time in each revision commit as the author time.

This has several consequences:

* We may miss updates of the same revision if are sent to the database within the same second.
  This can happen if two people save a page update using the previous author name at similar times.
  It can also happen if someone edits a page in quick succession.

* Revisions may be committed in a different order in the source backup repository than shown on the history page of Instiki.
  In particular, the pre-2015 revisions in the nLab database all have similar `updated_at` fields that are unrelated to the actual revision time.

### Metadata encoding

We store revision metadata in the git commit information.
Depending on where we store something, we use a percent encoding for reserved characters.

* The author name of a git commit has reserved characters `\0`, `\n`, `<`, `>`.
  We use it to store revision author names.

* Commit messages are lines of the form "<key>: <value>" with `\0` and `\n` reserved in key and value and additionally `:` reserved in the key.

### Compacting the backup repository

After committing a large number of revisions, it can be beneficial to run `git gc` in the backup repository.
This packs and compresses similar blobs.
It can reduce in a storage by a factor of ten.

## Tutorial

Create and move to an empty directory.
Initialize a git repository and make an initial commit:

```shell
$ git init
$ git commit --allow-empty -m 'Initial commit.'
```
Set up a default push remote for the default branch.
Run instiki2git in this directory:
``` shell
$ instiki2git -v --unix-socket [...] --web-id 1 --push --partition-sizes 1 1 1 1
```
This will record the Instiki revisions of the given web in the repository.

To update, just rerun the same instiki2git command.

There is an option `--html` for HTML instead of source backup.
Currently, you have to use different repositories for the two.

## Acknowledgment

This script was coded by Adeel and [Sajeel](https://sajeelk.github.io/) Khan.
It was improved by Felix Wellen.
It was rewritten by Christian Sattler.
