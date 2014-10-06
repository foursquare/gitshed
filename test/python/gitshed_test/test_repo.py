# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import unittest

from gitshed_test.helpers import temporary_git_repo


class GitRepoTest(unittest.TestCase):
  def test_is_ignored(self):
    with temporary_git_repo({'.gitignore': '*.ignored'}) as repo:
      self.assertTrue(repo.is_ignored('foo.ignored'))
      self.assertFalse(repo.is_ignored('foo.notignored'))
