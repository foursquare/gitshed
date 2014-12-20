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
from gitshed.util import safe_makedirs, temporary_dir, make_read_only


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

  def __init__(self, chunk_size=20, get_concurrency=None, put_concurrency=None):
    """
    :param chunk_size: Get/put in chunks of this size.
    :param get_concurrency: Size of threadpool for gets.
    :param put_concurrency: Size of threadpool for puts.
    """
    self._chunk_size = chunk_size
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

  def get(self, key_to_target_paths):
    """Gets file content from this content_store.

    In the case of multiple files with the same content, will only fetch the content once.

    :param key_to_target_paths: Map of key -> [list of target_paths for the content at that key]
    """
    def num_files_including_duplicates(k2t):
      return reduce(lambda x, y: x + len(y), k2t.values(), 0)

    if not key_to_target_paths:
      return

    progress = Progress(num_files_including_duplicates(key_to_target_paths))
    progress.update_bar()

    pool = ThreadPool(self._get_concurrency)
    def do_get(chunk):
      self._get_chunk(chunk)
      progress.increment(num_files_including_duplicates(chunk))
    items = list(key_to_target_paths.items())
    chunks = [dict(items[i:i+self._chunk_size]) for i in range(0, len(items), self._chunk_size)]
    pool.map(do_get, chunks)
    print('')

  def _get_chunk(self, key_to_target_paths):
    """Gets the content of some files from this content store.

    :param key_to_target_paths: Map of key -> [list of target_paths], where those args are as
           described in get() below.
    """
    target_tmpdir = '/tmp/gitshed/{0}'.format(str(uuid.uuid4()))
    safe_makedirs(target_tmpdir)

    content_store_paths = []
    for key in key_to_target_paths:
      content_store_paths.append(self.content_store_path_from_key(key))

    self.raw_get(content_store_paths, target_tmpdir)

    for key, target_paths in key_to_target_paths.items():
      target_path_tmp = os.path.join(target_tmpdir, os.path.basename(self.content_store_path_from_key(key)))
      sha = ContentStore.sha(target_path_tmp)
      if key != sha:
        raise GitShedError('Content sha mismatch for {0}! Expected {1} but got {2}.'.format(
          target_path_tmp, key, sha))
      # Must not write through the symlink: those changes won't be seen by git (let alone git shed).
      make_read_only(target_path_tmp)
      for target_path in target_paths:
        safe_makedirs(os.path.dirname(target_path))
      for target_path in target_paths[:-1]:
        shutil.copy(target_path_tmp, target_path)
      shutil.move(target_path_tmp, target_paths[-1])

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

    Used only for testing/setup verification.

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
    content_store_path_suffix = self.content_store_path_from_key(str(uuid.uuid4()))
    filename = os.path.basename(content_store_path_suffix)
    content_store_path = 'GITSHED_CLIENT_CHECK_DELETABLE/' + content_store_path_suffix
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
      roundtripped_tmpdir = os.path.join(tmpdir, 'roundtripped')
      os.mkdir(roundtripped_tmpdir)
      self.raw_get([content_store_path], roundtripped_tmpdir)
      with open(os.path.join(roundtripped_tmpdir, filename), 'r') as infile:
        s = infile.read()
      if s != content:
        raise GitShedError('Mismatched content fetched from content_store.')

  def raw_get(self, content_store_paths, target_dir_tmp):
    """Gets the content of files by their logical paths.

    Writes to a temporary dir, which is guaranteed to exist.
    Content is verified against its key before being copied to the true location. So
    implementations needn't worry about handling data corruption.

    Subclasses must implement.

    :param content_store_paths: Get the contents at these content store paths.
    :param target_dir_tmp: Write the content to this temporary directory, using the filenames
                           in the content_store_paths (which must be unique).
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
