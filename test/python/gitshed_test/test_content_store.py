# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from collections import defaultdict
import os
import stat
import unittest

import pytest

from gitshed.content_store import ContentStore
from gitshed.error import GitShedError
from gitshed.local_content_store import LocalContentStore
from gitshed.remote_content_store import RSyncedRemoteContentStore
from gitshed.util import can_ssh, run_cmd_str, safe_makedirs
from gitshed_test.helpers import cd, temporary_git_repo, temporary_test_dir


class ContentStoreTest(unittest.TestCase):

  chunk_sizes = [1, 2, 3, 4, 5, 6, 20]

  def test_sha(self):
    with temporary_git_repo({'foo.txt': 'HASH ME'}):
      _, expected_sha, _ = run_cmd_str('git hash-object foo.txt')
      # Verify that our computed sha is the same as the one that git computes.
      self.assertEqual(expected_sha.strip(), ContentStore.sha('foo.txt'))


  def test_content_store_path_from_key(self):
    path = ContentStore().content_store_path_from_key('da39a3ee5e6b4b0d3255bfef95601890afd80709')
    self.assertEquals('content_store/da39a3ee5e6b4b0d3255bfef95601890afd80709', path)

  def test_sha_verification(self):
    class BrokenContentStore(ContentStore):
      def raw_get(self, content_store_paths, target_dir_tmp):
        for p in content_store_paths:
          with open(os.path.join(target_dir_tmp, os.path.basename(p)), 'w') as fp:
            fp.write(b'BAD CONTENT')

    with temporary_test_dir() as tmpdir:
      path = os.path.join(tmpdir, 'test')
      with pytest.raises(GitShedError):
        BrokenContentStore().get({'0123456789012345678901234567890123456789': [path]})
      self.assertFalse(os.path.exists(path))

  def test_local_content_store(self):
    for chunk_size in self.chunk_sizes:
      self._test_local_content_store(chunk_size)

  def test_remote_content_store(self):
    if not can_ssh('localhost'):
      pytest.skip(
        'Cannot ssh to localhost. Change your machine security settings. E.g., on OS X you must:\n'
        '  A. Temporarily check System Preferences -> Sharing -> Remote Login.\n'
        '  B. Verify that your ~/.ssh/id_rsa.pub is in your ~/.ssh/authorized_keys.\n'
        '  C. Give your terminal session sudo (e.g., run sudo pwd right before running the tests).\n')
    for chunk_size in self.chunk_sizes:
      self._test_remote_content_store(chunk_size)

  def _test_local_content_store(self, chunk_size):
    with temporary_test_dir(cleanup=False) as content_store_root:
      content_store = LocalContentStore(content_store_root, chunk_size=chunk_size)
      self._test_contentstore(content_store)

  def _test_remote_content_store(self, chunk_size):
    # We ignore_errors because rsync may create dirs as root, which we won't be able to clean up.
    with temporary_test_dir(ignore_errors=True) as content_store_root:
      with cd(content_store_root):
        content_store = RSyncedRemoteContentStore('localhost', content_store_root, chunk_size=chunk_size)
        self._test_contentstore(content_store)

  def _test_contentstore(self, content_store):
    self._test_single_path_ops(content_store)
    self._test_multi_path_ops(content_store)
    # Test that the verify_setup() functionality works. It's pretty similar to the logic in _test_single_file(),
    # but we still want to exercise this code path.
    content_store.verify_setup()


  def _test_single_path_ops(self, content_store):
    with temporary_test_dir() as file_root:
      content = b'SOME CONTENT'
      relpath = 'foo/bar baz/qux quux.txt'  # Note spaces in paths.
      fullpath = os.path.join(file_root, relpath)
      os.makedirs(os.path.dirname(fullpath))
      with open(fullpath, 'w') as outfile:
        outfile.write(content)

      # # Make file executable.
      os.chmod(fullpath, 0755)
      old_mode = os.stat(fullpath)[stat.ST_MODE]

      key = ContentStore.sha(fullpath)

      self.assertFalse(content_store.has(key))
      content_store.put([fullpath])
      self.assertTrue(content_store.has(key))

      self.assertTrue(os.path.exists(fullpath))
      os.remove(fullpath)
      self.assertTrue(content_store.has(key))

      self.assertFalse(os.path.exists(fullpath))
      content_store.get({key: [fullpath]})
      self.assertTrue(os.path.exists(fullpath))
      with open(fullpath, 'r') as infile:
        s = infile.read()
      self.assertEqual(content, s)
      new_mode = os.stat(fullpath)[stat.ST_MODE]

      # Verify that file is still executable, but is read-only.
      self.assertEqual(old_mode & ~0222, new_mode)

  def _test_multi_path_ops(self, content_store):
    with temporary_test_dir() as file_root:
      relpath2content = {
        # Note spaces in paths.
        # Note that we can't currently handle different file permissions on the same content.
        'foo/bar baz1/qux quux.txt': (b'CONTENT1', 0755),
        'foo/bar baz2/qux quux.txt': (b'CONTENT1', 0755),  # Note: same content as previous file.
        'foo/bar baz/qux quux_A.txt': (b'CONTENT2', 0644),
        'foo/bar baz/qux quux_B.txt': (b'CONTENT3', 0644),
        'foo/bar baz1/qux quux_42.txt': (b'CONTENT4', 0400),
        'foo/bar baz2/qux quux_101.txt': (b'CONTENT5', 0754),
      }

      fullpath2content = {}
      fullpath2key = {}
      fullpath2mode = {}
      key2fullpaths = defaultdict(list)

      for relpath, (content, mode) in relpath2content.items():
        fullpath = os.path.join(file_root, relpath)
        safe_makedirs(os.path.dirname(fullpath))
        with open(fullpath, 'w') as outfile:
          outfile.write(content)
        # Make file executable.
        os.chmod(fullpath, mode)
        current_mode = os.stat(fullpath)[stat.ST_MODE]
        key = ContentStore.sha(fullpath)
        self.assertFalse(content_store.has(key))
        fullpath2content[fullpath] = content
        fullpath2key[fullpath] = key
        fullpath2mode[fullpath] = current_mode
        key2fullpaths[key].append(fullpath)

      content_store.put(fullpath2key.keys())

      for fullpath, key in fullpath2key.items():
        self.assertTrue(content_store.has(key))
        self.assertTrue(os.path.exists(fullpath))
        os.remove(fullpath)
        self.assertFalse(os.path.exists(fullpath))
        self.assertTrue(content_store.has(key))

      content_store.get(key2fullpaths)

      for fullpath, content in fullpath2content.items():
        self.assertTrue(os.path.exists(fullpath))
        with open(fullpath, 'r') as infile:
          s = infile.read()
        self.assertEqual(content, s)
        new_mode = os.stat(fullpath)[stat.ST_MODE]
        expected_mode = fullpath2mode[fullpath]
        # Verify that the file is read-only, but the mode otherwise unchanged (e.g., is still executable).
        self.assertEqual(expected_mode & ~0222, new_mode)
