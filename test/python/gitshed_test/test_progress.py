# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import unittest

from gitshed.progress import Progress


class ProgressTest(unittest.TestCase):
  def test_progress(self):
    progress = Progress(total=200)
    for i in xrange(200):
      self.assertFalse(progress.is_complete())
      self.assertEquals(int(i / 2), progress.pct_complete())
      progress.increment()
    self.assertTrue(progress.is_complete())
    self.assertEquals(100, progress.pct_complete())

    # Further increments are a no-op.
    progress.increment()
    self.assertTrue(progress.is_complete())
    self.assertEquals(100, progress.pct_complete())
