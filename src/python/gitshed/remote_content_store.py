# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os
import urlparse

import requests

from gitshed.content_store import ContentStore
from gitshed.error import GitShedError
from gitshed.util import run_cmd_str


class RemoteContentStore(ContentStore):
  """A content_store on a remote server.

  Reads using HTTP(S) GET/HEAD.  Subclasses must implement writes.
  """
  READ_SIZE_BYTES = 4 * 1024 * 1024

  def __init__(self, root_url, timeout_secs=None, get_concurrency=None, put_concurrency=None):
    super(RemoteContentStore, self).__init__(get_concurrency, put_concurrency)
    self._root_url = root_url
    self._timeout_secs = timeout_secs or 10
    self._session = requests.Session()

  def raw_get(self, content_store_path, target_path_tmp):
    url = self._get_full_content_store_url(content_store_path)
    response = self._session.get(url, timeout=self._timeout_secs, stream=True)
    if not self._is_ok(url, response):
      raise GitShedError('Resource does not exist: {0}'.format(url))
    with open(target_path_tmp, 'w') as outfile:
      for chunk in response.iter_content(self.READ_SIZE_BYTES):
        outfile.write(chunk)

  def raw_has(self, path):
    url = self._get_full_content_store_url(path)
    response = self._session.head(url, timeout=self._timeout_secs)
    return self._is_ok(url, response)

  def _get_full_content_store_url(self, content_store_path):
    return urlparse.urljoin(self._root_url, content_store_path)

  def _is_ok(self, url, response):
    """Does the response represent a successful HTTP round trip?"""
    if 200 <= response.status_code < 300:  # Allow all 2XX responses. E.g., HEAD can return 204.
      return True
    elif response.status_code == 404:
      return False
    else:
      raise GitShedError('Error accessing {0}: {1} {2}'.format(
        url, response.status_code, response.reason))


class RSyncedRemoteContentStore(RemoteContentStore):
  """A remote content_store that writes using rsync."""
  def __init__(self, host, root_path, root_url, timeout_secs=None,
               get_concurrency=None, put_concurrency=None):
    super(RSyncedRemoteContentStore, self).__init__(root_url, timeout_secs,
                                                    get_concurrency, put_concurrency)
    self._host = host
    self._remote_root_path = root_path

  def raw_put(self, src_path, content_store_path):
    # Note that rsync does an atomic rename at the end of a write, so we don't need
    # to emulate that functionality ourselves.
    remote_path = os.path.join(self._remote_root_path, content_store_path)
    remote_dir = os.path.dirname(remote_path)
    cmd_str = 'rsync -cv --rsync-path="sudo mkdir -p {0} && sudo rsync" "{1}" {2}:{3}'.format(
      remote_dir, src_path, self._host, remote_path)
    retcode, stdout, stderr = run_cmd_str(cmd_str)
    if retcode:
      raise GitShedError('Failed to rsync {0} to {1}:{2}.\ncommand: {3}\nstdout: {4}\nstderr: {5}'.
                         format(src_path, self._host, content_store_path, cmd_str, stdout, stderr))

