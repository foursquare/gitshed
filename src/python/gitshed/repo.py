# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os

from gitshed.error import GitShedError
from gitshed.util import run_cmd_str


class GitRepo(object):
  """A git repo.

  The repo root must be the current working directory.
  """
  def __init__(self, root):
    self._root = os.path.realpath(os.path.abspath(os.path.expanduser(root)))
    cwd = os.path.realpath(os.path.normpath(os.getcwd()))
    if not self._root == cwd:
      raise GitShedError('Git root {0} is not the current working directory.'.format(root))

  def relpath(self, path):
    """Resolves path to a relative path under this repo's root.

    :param path: An absolute path or relative path (interpreted as relative to this repo's root.)
    :raises GitShedError if the resolved path is not under this root.
    """
    return os.path.relpath(self.abspath(path), self._root)

  def abspath(self, path):
    """Resolves path to an absolute path under this repo's root.

    :param path: An absolute path or relative path (interpreted as relative to this repo's root.)
    :raises GitShedError if the resolved path is not under this root.
    """
    abspath = os.path.realpath(os.path.normpath(os.path.join(self._root, os.path.expanduser(path))))
    if not abspath.startswith(self._root):
      raise GitShedError('{0} is not under git repo root {1}'.format(abspath, self._root))
    return abspath

  def is_ignored(self, path):
    """Checks if a path is gitignored in this repo.

    :param path: The path to check.
    """
    retcode, _, _ = run_cmd_str('git check-ignore -q {0}'.format(path))
    return retcode == 0
