# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os
import pytest
import SimpleHTTPServer
import SocketServer
from threading import Thread
import unittest

from gitshed.content_store import ContentStore
from gitshed.error import GitShedError
from gitshed.local_content_store import LocalContentStore
from gitshed.remote_content_store import RSyncedRemoteContentStore
from gitshed.util import can_ssh, run_cmd_str
from gitshed_test.helpers import cd, temporary_git_repo, temporary_test_dir


class ContentStoreTest(unittest.TestCase):

  def test_sha(self):
    with temporary_git_repo({'foo.txt': 'HASH ME'}) as repo:
      _, expected_sha, _ = run_cmd_str('git hash-object foo.txt')
      # Verify that our computed sha is the same as the one that git computes.
      self.assertEqual(expected_sha.strip(), ContentStore.sha('foo.txt'))


  def test_content_store_path_from_key(self):
    path = ContentStore().content_store_path_from_key('da39a3ee5e6b4b0d3255bfef95601890afd80709')
    self.assertEquals('content_store/da39a3ee5e6b4b0d3255bfef95601890afd80709', path)

  def test_sha_verification(self):
    class BrokenContentStore(ContentStore):
      def raw_get(self, content_store_path, target_path_tmp):
        with open(target_path_tmp, 'w') as fp:
          fp.write('BAD CONTENT')

    with temporary_test_dir() as tmpdir:
      path = os.path.join(tmpdir, 'test')
      with pytest.raises(GitShedError):
        BrokenContentStore().get('0123456789012345678901234567890123456789', path)
      self.assertFalse(os.path.exists(path))

  def test_local_content_store(self):
    with temporary_test_dir() as content_store_root:
      content_store = LocalContentStore(content_store_root)
      self._test_contentstore(content_store)

  def test_remote_content_store(self):
    if not can_ssh('localhost'):
      pytest.skip(
        'Cannot ssh to localhost. Change your machine security settings. E.g., on OS X you can '
        'temporarily check System Preferences -> Sharing -> Remote Login and run test under sudo.')
    with temporary_test_dir() as content_store_root:
      httpd = None
      httpd_thread = None
      try:
        with cd(content_store_root):
          httpd = SocketServer.TCPServer(('localhost', 0),
                                         SimpleHTTPServer.SimpleHTTPRequestHandler)
          httpd_thread = Thread(target=httpd.serve_forever)
          httpd_thread.start()
          root_url = 'http://{0}:{1}/'.format(*httpd.server_address)
          content_store = RSyncedRemoteContentStore('localhost', content_store_root, root_url)
          self._test_contentstore(content_store)
      finally:
        if httpd:
          httpd.shutdown()
        if httpd_thread:
          httpd_thread.join()

  def _test_contentstore(self, content_store):
    with temporary_test_dir() as file_root:
      content = 'SOME CONTENT'
      relpath = 'foo/bar/baz.txt'
      full_path = os.path.join(file_root, relpath)
      os.makedirs(os.path.dirname(full_path))
      with open(full_path, 'w') as outfile:
        outfile.write(content)

      key = ContentStore.sha(full_path)

      self.assertFalse(content_store.has(key))
      content_store.put(full_path)
      self.assertTrue(content_store.has(key))

      self.assertTrue(os.path.exists(full_path))
      os.remove(full_path)
      self.assertTrue(content_store.has(key))

      self.assertFalse(os.path.exists(full_path))
      content_store.get(key, full_path)
      self.assertTrue(os.path.exists(full_path))
      with open(full_path, 'r') as infile:
        s = infile.read()
      self.assertEqual(content, s)

      # Test that the verify_setup() functionality works. It's pretty similar to the logic above,
      # but we still want to exercise this code path.
      content_store.verify_setup()
