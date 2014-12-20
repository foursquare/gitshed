# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import sys
import threading


class Progress(object):
  """Represents progress of multi-file operations.

  Draws an ASCII-art progress bar.
  """

  def __init__(self, total, num_increments=50):
    """
    :param total: The number of units of work to display progress for.
    :param num_increments: The number of increments to display in the progress bar.
                           each one represents (total/num_increments) units of work.

    """
    self._completed = 0
    self._total = total
    self._num_increments = num_increments
    self._increment_size = total / num_increments

    # The ascii-art progress bar format.
    # E.g.:
    # 120/200 files [..............................                    ]  60%
    self._bar_format = '{completed:>' + str(len(str(total))) + '}/' + str(total) + ' files [{dots}{spaces}] {pct:>3}%'
    self._lock = threading.Lock()

  def is_complete(self):
    return self._completed == self._total

  def pct_complete(self):
    return int(100 * self._completed / self._total)

  def increment(self, n=1):
    """Increment number of completed work units.

    :param n: Number of times to increment.
    """
    with self._lock:
      if self._completed < self._total:
        self._completed += min(n, self._total - self._completed)
        self.update_bar()

  def update_bar(self):
    """Redraw the progress bar.

    Note: Unsynchronized.
    """
    sys.stderr.write('\r')
    increments_done = int(self._completed / self._increment_size)
    sys.stderr.write(self._bar_format.format(
      completed=self._completed,
      dots='.' * increments_done,
      spaces=' ' * (self._num_increments - increments_done),
      pct=self.pct_complete()
    ))
    if self.pct_complete() == 100:
      sys.stderr.write('\n')
    sys.stderr.flush()
