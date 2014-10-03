# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from contextlib import contextmanager
import os

from gitshed.error import GitShedError
from gitshed.repo import GitRepo
from gitshed.util import run_cmd_str, safe_makedirs, temporary_dir


@contextmanager
def temporary_test_dir(suffix=''):
  """A context yielding a temporary directory for testing."""
  with temporary_dir(suffix=suffix, prefix='gitshed_test.') as tmpdir:
    yield tmpdir


@contextmanager
def temporary_git_repo_root():
  """A context yielding a temporary git repo directory."""
  with temporary_test_dir(suffix='.temp_git_repo') as root_dir:
    retcode, _, _ = run_cmd_str('git init {0}'.format(root_dir))
    if retcode:
      raise GitShedError('Failed to initialize temporary git repo at {0}'.format(root_dir))
    yield root_dir


@contextmanager
def cd(path):
  """A context for changing working directories.

  The cwd is captured at context entry and restored on context exit.

  :param path: The cwd within the context.
  """
  cwd = os.getcwd()
  os.chdir(path)
  yield
  os.chdir(cwd)


@contextmanager
def temporary_git_repo(seed_files=None):
  """A context yielding a GitRepo instance representing a temporary git repo.

  :param seed_files: An optional dict of relpath->string, of contents to seed the repo with.
  """
  with temporary_git_repo_root() as root:
    with cd(root):
      files = dict(seed_files)  # Make a copy.
      if '.gitignore' not in files:
        # If not provided, we add a .gitignore, to ensure that the shed directory is ignored.
        files['.gitignore'] = os.path.join('.gitshed', 'files') + '\n'
      for path, contents in files.items():
        safe_makedirs(os.path.dirname(path))
        with open(path, 'w') as outfile:
          outfile.write(contents)
      yield GitRepo(root)
