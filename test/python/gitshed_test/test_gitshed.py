# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import os
import unittest

from gitshed.content_store import ContentStore
from gitshed.local_content_store import LocalContentStore
from gitshed.gitshed import GitShed
from gitshed_test.helpers import temporary_test_dir, temporary_git_repo


class gitshedTest(unittest.TestCase):

  def test_gitshed(self):
    file_relpath = os.path.join('foo', 'bar', 'baz')
    with temporary_git_repo({file_relpath: 'SOME FILE CONTENT'}) as repo:
      # Note: test assumes that our cwd is the repo root, which temporary_git_repo should ensure.
      with temporary_test_dir() as content_store_root:
        content_store = LocalContentStore(content_store_root)
        gitshed = GitShed(repo, content_store)

        def assert_status(expected_total, expected_unsynced):
          n, b = gitshed.get_status()
          self.assertEquals(expected_total, n)
          self.assertEquals(b, expected_unsynced)

        assert_status(0, 0)

        sha = ContentStore.sha(file_relpath)
        bucket_relpath = os.path.join('.gitshed', 'files', 'foo', 'bar', '{0}.baz'.format(sha))
        self.assertFalse(os.path.isfile(bucket_relpath))

        # Put the file under gitshed management. It should become a symlink.
        gitshed.manage([file_relpath])
        self.assertTrue(os.path.islink(file_relpath))
        self.assertTrue(os.path.isfile(bucket_relpath))
        link_abspath = os.path.abspath(os.path.join(os.path.dirname(file_relpath),
                                                    os.readlink(file_relpath)))
        self.assertEquals(os.path.abspath(bucket_relpath), link_abspath)
        assert_status(1, 0)

        # Remove the symlink target.
        os.unlink(bucket_relpath)
        self.assertFalse(os.path.isfile(file_relpath))
        assert_status(1, 1)

        # Restore it by syncing.
        gitshed.sync([file_relpath])
        self.assertTrue(os.path.isfile(file_relpath))
        assert_status(1, 0)
