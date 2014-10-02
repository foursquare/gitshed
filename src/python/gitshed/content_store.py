# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import hashlib
from multiprocessing.pool import ThreadPool
import os
import shutil
import uuid

from gitshed.error import GitShedError
from gitshed.progress import Progress
from gitshed.util import safe_makedirs, temporary_dir


class ContentStore(object):
  """An external store for file content, outside the git repo.

  An entry in a ContentStore is identified externally by a key (in practice the sha1 of
  the content) and internally by a logical path. This path may or may not correspond
  to a filesystem or URL path, depending on the implementation.

  Note that there is no delete functionality, by design.  Once a file's metadata has been
  committed, the content it references must live for all time, in case anyone inspects the repo
  at to that commit some time in the future.
  """

  @classmethod
  def sha(cls, path):
    """Computes a file's git sha.

    :param path: The file to fingerprint.
    """
    # Note that git computes its content fingerprints as sha1('blob ' + filesize + '\0' + data),
    # so we do the same. It's not crucial to use git's object hash as our fingerprint, but it
    # makes debugging easier.
    size = os.path.getsize(path)

    hasher = hashlib.sha1()
    hasher.update('blob ')
    hasher.update(str(size))
    hasher.update('\0')
    with open(path, 'r') as infile:
      data = infile.read(4096)
      while data:
        hasher.update(data)
        data = infile.read(4096)
    return hasher.hexdigest()

  def __init__(self, get_concurrency=None, put_concurrency=None):
    """
    :param get_concurrency: Size of threadpool for gets.
    :param put_concurrency: Size of threadpool for puts.
    """
    self._get_concurrency = get_concurrency or 12
    self._put_concurrency = put_concurrency or 4

  def content_store_path_from_key(self, key):
    """Returns the logical path at which to store the file with the given key.

    Logical paths always use forward slash separators, even on systems where this is not
    the filesystem separator.

    NOTE: Do *not* change the output of this method. That will sever the association of
    a file to its content.

    :param key: Return the logical path for this key.
    """
    # Our trivial implementation just puts everything under a single dir.
    # This is fine for our needs.
    # Subclasses may override for more sophisticated implementations (e.g., to split into
    # multiple dirs if you expect a large number of files.)
    return 'content_store/{0}'.format(key)

  def multi_get(self, key_target_path_pairs):
    """Gets the content of multiple files from this content_store.

    :param key_target_path_pairs: Iterable of (key, target_path) pairs, where those args are as
           described in get() below.
    """
    if not key_target_path_pairs:
      return

    if len(key_target_path_pairs) == 1:
      self.get(key_target_path_pairs[0][0], key_target_path_pairs[0][1])
      return

    n = len(key_target_path_pairs)  # Total number of files to get content for.
    progress = Progress(n)
    progress.update_bar()

    # The work is IO-bound, so we should be OK using threads.
    # TODO(benjy): See if it's worth using a process pool instead.
    pool = ThreadPool(self._get_concurrency)
    def do_get(arg):
      self.get(arg[0], arg[1])
      progress.increment()

    pool.map(do_get, key_target_path_pairs)
    print('')

  def get(self, key, target_path):
    """Gets the content of a file from this content store.

    :param key: Get the content with this key.
    :param target_path: Write the content to this file.
    """
    content_store_path = self.content_store_path_from_key(key)
    target_path_tmp = target_path + '.tmp'
    safe_makedirs(os.path.dirname(target_path_tmp))
    self.raw_get(content_store_path, target_path_tmp)
    sha = ContentStore.sha(target_path_tmp)
    if key != sha:
      raise GitShedError('Content sha mismatch for {0}! Expected {1} but got {2}.'.format(
        target_path_tmp, key, sha))
    shutil.move(target_path_tmp, target_path)

  def multi_put(self, src_paths):
    """Puts the content of multiple files into this content_store.

    :param src_paths: Iterable source paths to put into this content store.
    :returns An iterable of keys, one for each source path.
    """
    if not src_paths:
      return []

    # No point in spawning threads for just one file.
    if len(src_paths) == 1:
      return [self.put(src_paths[0])]

    n = len(src_paths)  # Total number of files to put.
    progress = Progress(n)
    progress.update_bar()

    # The work is IO-bound, so we should be OK using threads. Each thread
    # spawns rsync processes anyway.
    # TODO(benjy): See if it's worth using a process pool instead.
    pool = ThreadPool(self._put_concurrency)
    def do_put(src_path):
      ret = self.put(src_path)
      progress.increment()
      return ret

    ret = pool.map(do_put, src_paths)
    print('')
    return ret

  def put(self, src_path):
    """Puts the content of a file into this content store.

    :param src_path: Read the content from this file.
    :returns The key for the content.
    """
    key = ContentStore.sha(src_path)
    content_store_path = self.content_store_path_from_key(key)
    self.raw_put(src_path, content_store_path)
    return key

  def has(self, key):
    """Checks for the existence of content in this content store.

    :param key: Check for content under this key.
    """
    return self.raw_has(self.content_store_path_from_key(key))

  def verify_setup(self):
    """Check that this content store works from this client.

    Raises an error if any aspect of content_store interaction fails.

    Note, not a build-time test but a runtime check that all the moving parts work.
    """
    # This check pollutes the content_store. This shouldn't a problem, but we give these
    # files special path names so that detail-oriented admins can delete them if they choose.
    content_store_path = 'GITSHED_CLIENT_CHECK_DELETABLE/' + \
                         self.content_store_path_from_key(str(uuid.uuid4()))
    if self.raw_has(content_store_path):
      raise GitShedError('Test key unexpectedly found.')
    with temporary_dir() as tmpdir:
      tmpfile = os.path.join(tmpdir, 'GITSHED_CLIENT_CHECK')
      content = 'FAKE FILE CONTENT.'
      with open(tmpfile, 'w') as outfile:
        outfile.write(content)
      self.raw_put(tmpfile, content_store_path)
      if not self.raw_has(content_store_path):
        raise GitShedError('Test key not found. Content store write failed?')
      roundtripped_tmpfile = tmpfile + '.roundtripped'
      self.raw_get(content_store_path, roundtripped_tmpfile)
      with open(roundtripped_tmpfile, 'r') as infile:
        s = infile.read()
      if s != content:
        raise GitShedError('Mismatched content fetched from content_store.')

  def raw_get(self, content_store_path, target_path_tmp):
    """Gets the content of a file by its logical path.

    Writes to a temporary file which is verified against its key before being copied
    to the true location. So implementations needn't worry about handling data corruption.
    The file's directory is guaranteed to exist.

    Subclasses must implement.

    :param content_store_path: Get the content at this content store path.
    :param target_path_tmp: Write the content to this temporary file.
    """
    raise NotImplementedError()

  def raw_put(self, src_path, content_store_path):
    """Puts the content of a file into a location specified by a logical path.

    Subclasses must implement.

    :param src_path: Read the content from this file.
    :param content_store_path: Put the content at this content store path.
    """
    raise NotImplementedError()

  def raw_has(self, content_store_path):
    """Checks for the existence of content at a logical path in this content store.

    Subclasses must implement.

    :param content_store_path: Check for content at this content store path.
    """
    raise NotImplementedError()
