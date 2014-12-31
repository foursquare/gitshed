# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from contextlib import contextmanager
import errno
import os
import shlex
import shutil
import subprocess
import tempfile

from gitshed.error import GitShedError


def safe_makedirs(path):
  """Ensures that a directory exists.

  :param path: On return this directory is guaranteed to exist.
  """
  if path:
    try:
      os.makedirs(path)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise


def safe_rmtree(path):
  """Delete a directory if it's present, or no-op otherwise.

  :param path: The directory to remove.
  """
  if os.path.islink(path):
    raise GitShedError('path must not be a symlink.')
  if not os.path.isdir(path):
    raise GitShedError('path must be a directory.')
  shutil.rmtree(path, True)


def make_mode_read_only(mode):
  return mode & ~0222

def make_read_only(path):
  mode = os.stat(path).st_mode
  os.chmod(path, make_mode_read_only(mode))

def make_user_writeable(path):
  mode = os.stat(path).st_mode
  os.chmod(path, mode | 0200)


@contextmanager
def temporary_dir(suffix='', prefix='gitshed.', ignore_errors=False, cleanup=True):
  """A context yielding an empty temporary directory.

  The directory is guaranteed to exist within the context and to be cleaned up on context exit.
  """
  ret = tempfile.mkdtemp(suffix=suffix, prefix=prefix)
  yield ret
  if cleanup:
    shutil.rmtree(ret, ignore_errors=ignore_errors)


def run_cmd_str(cmd_str):
  """Spawns a command specified as a single string.

  Tokenizes the string appropriately, so the caller need not worry about spaces, escaping etc.
  """
  cmd = shlex.split(cmd_str.encode('utf8'))
  try:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, stdout, stderr
  except OSError as e:
    raise GitShedError('Error running "{0}": {1}'.format(' '.join(cmd), str(e)))


def can_ssh(host):
  """Checks if we can ssh to a given host.

  :param host: The host to attempt to ssh to.
  """
  cmd_str = 'ssh {0} pwd'.format(host)
  retcode, stdout, stderr = run_cmd_str(cmd_str)
  if retcode:
    print('Failed to ssh to {0}\ncommand: {1}\nstdout: {2}\nstderr: {3}'.format(
          host, cmd_str, stdout, stderr))
    return False
  return True
