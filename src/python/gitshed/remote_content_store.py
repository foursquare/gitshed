# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os

from gitshed.content_store import ContentStore
from gitshed.error import GitShedError
from gitshed.util import run_cmd_str, temporary_dir


class RSyncedRemoteContentStore(ContentStore):
  """A remote content_store that writes using rsync."""
  def __init__(self, host, root_path, chunk_size=20, get_concurrency=None, put_concurrency=None):
    super(RSyncedRemoteContentStore, self).__init__(chunk_size, get_concurrency, put_concurrency)
    self._host = host
    self._remote_root_path = root_path

  def raw_get(self, content_store_paths, target_dir_tmp):
    cmd_str, retcode, stdout, stderr = self._try_raw_get(content_store_paths, target_dir_tmp)
    if retcode:
      raise GitShedError(
        'Failed to rsync {src} from {host} to {dst}.\ncommand: {cmd}\nstdout: {stdout}\nstderr: {stderr}'.format(
          src=content_store_paths, host=self._host, dst=target_dir_tmp, cmd=cmd_str, stdout=stdout, stderr=stderr))

  def raw_has(self, path):
    # We implement by actually downloading the file to a dummy location.  This isn't particularly
    # efficient, but is only used in tests/client checks anyway.
    with temporary_dir() as tmpdir:
      _, retcode, _, _ = self._try_raw_get([path], tmpdir)
      return retcode == 0

  def _try_raw_get(self, content_store_paths, target_dir):
    remote_paths = [os.path.join(self._remote_root_path, self.escape(p)) for p in content_store_paths]
    cmd_str = """rsync -acvz {0}:'{1}' "{2}" """.format(self._host, ' '.join(remote_paths), target_dir)
    retcode, stdout, stderr = run_cmd_str(cmd_str)
    return cmd_str, retcode, stdout, stderr

  def raw_put(self, src_paths, content_store_dir):
    # Note that rsync does an atomic rename at the end of a write, so we don't need
    # to emulate that functionality ourselves.
    remote_dir = os.path.join(self._remote_root_path, content_store_dir)
    src_paths_str = ' '.join("'{0}'".format(src_path) for src_path in src_paths)
    cmd_str = """rsync -acvz --rsync-path="sudo mkdir -p {0} && sudo rsync" {1} {2}:{3}""".format(
      remote_dir, src_paths_str, self._host, remote_dir)
    retcode, stdout, stderr = run_cmd_str(cmd_str)
    if retcode:
      raise GitShedError('Failed to rsync {0} to {1}:{2}.\ncommand: {3}\nstdout: {4}\nstderr: {5}'.
                         format(src_paths_str, self._host, content_store_dir, cmd_str, stdout, stderr))


  @staticmethod
  def escape(s):
    """Escape spaces in the argument string.

    The rsync command accepts multiple remote paths by space-separating them. This means we
    must escape spaces in the paths themselves, if any.
    We assume that rsync will use a shell that understands backslash escapes.
    """
    return s.replace(' ', '\\ ')

