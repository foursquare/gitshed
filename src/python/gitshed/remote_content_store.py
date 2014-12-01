# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os

from gitshed.content_store import ContentStore
from gitshed.error import GitShedError
from gitshed.util import run_cmd_str, temporary_dir


class RSyncedRemoteContentStore(ContentStore):
  """A remote content_store that writes using rsync."""
  def __init__(self, host, root_path, get_concurrency=None, put_concurrency=None):
    super(RSyncedRemoteContentStore, self).__init__(get_concurrency, put_concurrency)
    self._host = host
    self._remote_root_path = root_path

  def raw_get(self, content_store_path, target_path_tmp):
    cmd_str, retcode, stdout, stderr = self._try_raw_get(content_store_path, target_path_tmp)
    if retcode:
      raise GitShedError(
        'Failed to rsync {0} from {1}:{2}.\ncommand: {3}\nstdout: {4}\nstderr: {5}'.format(
          target_path_tmp, self._host, content_store_path, cmd_str, stdout, stderr))

  def raw_has(self, path):
    # We implement by actually downloading the file to a dummy location.  This isn't particularly
    # efficient, but this is only used in tests/client checks anyway.
    with temporary_dir() as tmpdir:
      dummy = os.path.join(tmpdir, 'dummy')
      _, retcode, _, _ = self._try_raw_get(path, dummy)
      return retcode == 0

  def _try_raw_get(self, content_store_path, target_path_tmp):
    remote_path = os.path.join(self._remote_root_path, content_store_path)
    cmd_str = 'rsync -acvz {0}:{1} "{2}"'.format(self._host, remote_path, target_path_tmp)
    retcode, stdout, stderr = run_cmd_str(cmd_str)
    return cmd_str, retcode, stdout, stderr

  def raw_put(self, src_path, content_store_path):
    # Note that rsync does an atomic rename at the end of a write, so we don't need
    # to emulate that functionality ourselves.
    remote_path = os.path.join(self._remote_root_path, content_store_path)
    remote_dir = os.path.dirname(remote_path)
    cmd_str = 'rsync -acvz --rsync-path="sudo mkdir -p {0} && sudo rsync" "{1}" {2}:{3}'.format(
      remote_dir, src_path, self._host, remote_path)
    retcode, stdout, stderr = run_cmd_str(cmd_str)
    if retcode:
      raise GitShedError('Failed to rsync {0} to {1}:{2}.\ncommand: {3}\nstdout: {4}\nstderr: {5}'.
                         format(src_path, self._host, content_store_path, cmd_str, stdout, stderr))

