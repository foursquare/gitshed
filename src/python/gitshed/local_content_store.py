# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os
import shutil

from gitshed.content_store import ContentStore
from gitshed.util import safe_makedirs


class LocalContentStore(ContentStore):
  """A ContentStore on the local filesystem.

  Useful for testing.
  """
  def __init__(self, root, chunk_size=20, get_concurrency=None, put_concurrency=None):
    super(LocalContentStore, self).__init__(chunk_size, get_concurrency, put_concurrency)
    self._root = root

  def raw_get(self, content_store_paths, target_dir_tmp):
    for path in content_store_paths:
      target_path_tmp = os.path.join(target_dir_tmp, os.path.basename(path))
      shutil.copy(self._get_full_content_store_path(path), target_path_tmp)

  def raw_put(self, src_path, content_store_path):
    self._safe_copy(src_path,  self._get_full_content_store_path(content_store_path))

  def raw_has(self, content_store_path):
    return os.path.isfile(self._get_full_content_store_path(content_store_path))

  def _get_full_content_store_path(self, path):
    """Converts a logical content_store path to the filesytem path for the content."""
    if os.path.sep != '/':
      path = os.path.join(*path.split('/'))
    return os.path.join(self._root, path)

  @classmethod
  def _safe_copy(cls, src, dest):
    """Copies the file at src to dest atomically."""
    safe_makedirs(os.path.dirname(dest))
    tmp_dest = dest + '.tmp'
    shutil.copy(src, tmp_dest)
    shutil.move(tmp_dest, dest)
