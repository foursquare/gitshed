Gitshed
=======

A tool to manage file metadata in a git repo, while keeping the file contents elsewhere.

<!-- toc -->

* [Background](#background)
* [Concepts](#concepts)
* [Usage](#usage)
  * [manage](#manage)
  * [sync](#sync)
  * [status](#status)
  * [synced](#synced)
  * [unsynced](#unsynced)
  * [setup](#setup)
* [Workflow](#workflow)
* [Installation](#installation)
  * [Build](#build)
  * [Setup](#setup)
  * [Configuration](#configuration)

<!-- toc stop -->


Background
==========

It's generally not a good idea to put large binary files in git. Gitshed allows you to 
put only the file name and version metadata in git, while storing the file contents outside
the git repo.

Gitshed is similar in spirit to [git-annex](https://git-annex.branchable.com/), but simpler and 
more tailored to the author's requirements.

The name "gitshed" is a triple pun:

- It helps you "shed" binary files from your git repo.
- The files live outside the repo, in a "shed".
- The project was named in a bikeshedding session.


Concepts
========

Gitshed stores file contents in a key->value *content store*. The content store is typically hosted 
on a remote server, so multiple collaborators can use it.

File contents are pulled from the content store and stored locally in the *shed*
(`<repo root>/.gitshed/files`). The shed directory must be gitignored.

Files are represented in the repo as relative symlinks into the shed directory. A file managed by
Gitshed can be in one of two states: *unsynced* or *synced*.

- An unsynced file has no content in the shed, and is represented by a broken symlink.
- A synced file has content in the shed, and is represented by a symlink to that content.

*Syncing* a file pulls the content in from the content store into the shed, healing the symlink.


Usage
=====

Gitshed is typically installed as a custom git command, and invoked thus: `git shed <subcommand> <args>`.
It may also be invoked directly, but for the rest of this documentation we'll assume the former invocation style.

Gitshed has several subcommands.  Some of these take file paths as arguments, and these can be specified
in two ways:

- As a list of globs: `git shed manage data/*.bin archives/*.jar`
- As a file containing paths, one per line: 
  `git shed manage -f path/to/argfile` or
  `git shed manage --argfile=path/to/argfile`
  
Run `git shed` or `git shed --help` to get help. Run `git shed <subcommand> --help` to get help for that subcommand.

manage
------

Places files under gitshed's management.

`git shed manage <file args>`

Each file is uploaded to the content store, moved into the shed and replaced by a symlink.

sync
----

Syncs files into the shed.

`git shed sync <file args>`

The contents of all managed but unsynced files are pulled from the content store
to their symlinked locations in the shed.

To sync every managed file in the repo, omit the file arguments:

`git shed sync`


resync
------

Re-download managed files into the shed.

`git shed resync <file args>`

The contents of all managed files, even those already synced, are pulled from the content store
to their symlinked location in the shed.

To resync every managed file in the repo, omit the file arguments:

`git shed resync`

status
------

Prints a short status message with counts of synced and unsynced files.

`git shed status`

synced
------

Lists all synced files.

`git shed synced`


unsynced
--------

Lists all unsynced files.

`git shed unsynced`


unmanage
--------

Remove files from gitshed's management.

`git shed unmanage <file args>`

Symlinks are overwritten with the content they symlinked to.

To unmanage all managed files in the repo, omit the file arguments:

`git shed unmanage`


setup
-----

Verifies that the Gitshed instance is working (e.g., that it's able to communicate with the
content store, and that the shed directory `<repo root>/.gitshed/files` is gitignored).

`git shed setup`


Workflow
========

When adding a new file to the shed, typical workflow is:

1. Add the file at the relevant path in the repo.
2. `git manage path/to/file` to place the file under Gitshed's management. The file
   will be replaced by a symlink.
3. Commit the symlink.

After pulling, other contributors will have a broken symlink at `path/to/file` and will 
need to `git shed sync` to heal it and have access to the file content. 

You may want to use git hooks to have `git shed sync` called automatically when the workspace changes.
The relevant hooks are: `post-applypatch`, `post-checkout`, `post-commit`, `post-merge` and `post-rewrite`.


Installation
============

Build
-----

In the future we hope to create proper pypi releases. For now you have to build Gitshed from source.
Gitshed uses the [pants build tool](http://pantsbuild.github.io/) to create a `.pex` file. 
This is a self-contained, standalone python executable that requires only a python interpreter 
to run.

To build gitshed:

`pants binary src/python/gitshed`

This will create `dist/gitshed.pex`. 

You can run this file directly, but, as mentioned above, it's convenient to install it as a custom git command called 
'shed'. You can do so either using a wrapper script  named `git-shed` on your `PATH`,  or using `git config alias`. 


Setup
-----
The only setup steps are:

1. Add `<repo root>/.gitshed/files` to your `.gitignore` file.
2. Create a configuration file.


Configuration
-------------

Config lives in the file `<repo root>/.gitshed/config.json`. For example, to set up a remote content store:

    {
      ...
      "content_store": {
        "chunk_size": 20,
        "remote": {
          "host": "mycontentstore",
          "root_path": "/data/gitshed/myrepo/",
        }
      }
      ...
    }
    
This will upload and download using rsync, to/from the specified root path on the specified host. 
Each invocation of rsync will read/write chunk_size files. 

This is currently the only remote content store implementation available,  but it would be very straightforward to 
write new ones (e.g., a RESTful content store). Feel free to contribute one.
    
    {
      ...
      "exclude": [".pants.d", ".pants.bootstrap"],
      ...
    }

This will prevent pants from searching for symlinks into the shed in those directories, 
which can help performance.
    
    {
      ...
      "concurrency": {
        "get": 6,
        "put": 4
      }
      ...
    }
    
This will cause git shed to use 6 threads for downloading content while syncing and 
4 threads when uploading content while putting files under management.

There are example config files in this repo:

- `.gitshed/config.json.local`: For a local content store, useful for playing around.
- `.gitshed/config.json.remote`: For a remote content store.
