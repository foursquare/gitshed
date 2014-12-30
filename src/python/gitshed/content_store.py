# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from collections import defaultdict
import hashlib
from multiprocessing.pool import ThreadPool
import os
import re
import shutil
import uuid

from gitshed.error import GitShedError
from gitshed.progress import Progress
from gitshed.util import make_mode_read_only, make_read_only, safe_makedirs, temporary_dir


class ContentStore(object):
  """An external store for file content, outside the git repo.

  An entry in a ContentStore is identified externally by a key (in practice the sha1 of
  the content) and internally by a logical path. This path may or may not correspond
  to a filesystem or URL path, depending on the implementation.

  A ContentStore implementation must preserve file permissions.

  Note that there is no delete functionality, by design.  Once a file's metadata has been
  committed, the content it references must live for all time, in case anyone inspects the repo
  at to that commit some time in the future.
  """

  @staticmethod
  def _random_string():
    return uuid.uuid4().hex

  @classmethod
  def sha(cls, path):
    """Computes a file's git sha.

    :param path: The file to fingerprint.
    :returns: A string containing 40 hex digits.
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

  @classmethod
  def mode(cls, path):
    """Computes a file's mode.

    Returns just the permissions digits, not the ones that indicate device type and so on.
    Note that the mode ignores the read bit, as files managed by gitshed are always read-only.

    :param path: The file to compute a mode for.
    :returns: A 4-digit octal string (plus a leading 0).
    """
    # Files managed by gitshed are always read-only, so we ignore the write bits.
    mode = make_mode_read_only(os.stat(path).st_mode)
    # We want just the last 4 digits (plus the leading '0' to indicate octal).
    return ('0000' + oct(mode))[-5:]

  @classmethod
  def key(cls, path):
    """Computes a file's key.

    The key is a combination of the file's content fingerprint and its access permissions.
    This allows us to handle two different files with different permissions but the same sha.
    """
    sha = cls.sha(path)
    mode = cls.mode(path)
    return '{0}_{1}'.format(sha, mode)

  @classmethod
  def sha_from_key(cls, key):
    """Returns the sha part of a key."""
    return key[0:40]

  @classmethod
  def mode_from_key(cls, key):
    """Returns the access permissions part of a key, as an octal string."""
    return key[41:]

  _KEY_RE = re.compile(r'^[0-9a-f]{40}_0[0-7]{4}$')  # Matches exactly (40 hex digits)_0(4 octal digits).

  @classmethod
  def is_valid_key(cls, key):
    return cls._KEY_RE.match(key)

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
      return sum(len(t) for t in k2t.values())

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
    with temporary_dir() as target_tmpdir:
      content_store_paths = []
      for key in key_to_target_paths:
        content_store_paths.append(self.content_store_path_from_key(key))

      self.raw_get(content_store_paths, target_tmpdir)

      for key, target_paths in key_to_target_paths.items():
        target_path_tmp = os.path.join(target_tmpdir, os.path.basename(self.content_store_path_from_key(key)))
        actual_sha = ContentStore.sha(target_path_tmp)
        key_sha = self.sha_from_key(key)
        if key_sha != actual_sha:
          raise GitShedError('Content sha mismatch for {0}! Expected {1} but got {2}.'.format(
            target_path_tmp, key, actual_sha))
        actual_mode = ContentStore.mode(target_path_tmp)
        key_mode = self.mode_from_key(key)

        if key_mode != actual_mode:
          raise GitShedError('File permission mismatch for {0}! Expected {1} but got {2}.'.format(
            target_path_tmp, key_mode, actual_mode))

        for target_path in target_paths:
          safe_makedirs(os.path.dirname(target_path))
        for target_path in target_paths[:-1]:
          shutil.copy(target_path_tmp, target_path)
        shutil.move(target_path_tmp, target_paths[-1])

  def put(self, src_paths):
    """Puts the content of multiple files into this content_store.

    :param src_paths: Iterable source paths to put into this content store.
    :returns An iterable of keys, one for each source path.
    """
    if not src_paths:
      return []

    ret = []

    # We bucket src_paths by the content store directory they map to, as we can put multiple
    # contents to a single directory in a single call. Note that currently all content store
    # entries are in a single directory (see content_store_path_from_key above), so this step is
    # unneeded, but we do it anyway for futureproofing.
    # "work" here is a list of pairs of (src_path, content store basename).
    cs_dir_to_work = defaultdict(list)

    # If multiple files have the same content we don't need to put them multiple times.
    # However we do count how many user files a single content store path stands in for in this
    # put operation, so we can show users a progress bar with the numbers they expect.
    cardinality = defaultdict(lambda: 0)  # Map of content store path -> number of user files.

    for src_path in src_paths:
      key = ContentStore.key(src_path)
      ret.append(key)
      cs_path = self.content_store_path_from_key(key)
      if cs_path not in cardinality:
        cs_dir, _, cs_basename = cs_path.rpartition('/')
        cs_dir_to_work[cs_dir].append((src_path, cs_basename))
      cardinality[cs_path] += 1

    n = len(src_paths)  # Total number of files to put.
    progress = Progress(n)
    progress.update_bar()

    pool = ThreadPool(self._put_concurrency)
    def do_put(chunk):
      cs_dir, work = chunk
      n = sum(cardinality['{0}/{1}'.format(cs_dir, cs_basename)] for (_, cs_basename) in work)
      self._put_chunk(cs_dir, work)
      progress.increment(n)

    chunks = []
    for cs_dir, work in cs_dir_to_work.items():
      chunks.extend((cs_dir, work[i:i+self._chunk_size]) for i in range(0, len(work), self._chunk_size))

    pool.map(do_put, chunks)
    print('')
    return ret

  def _put_chunk(self, cs_dir, work):
    with temporary_dir() as tmpdir:
      tmp_src_paths = []
      for src_path, cs_basename in work:
        tmp_src_path = os.path.join(tmpdir, cs_basename)
        os.link(src_path, tmp_src_path)
        # Files in gitshed must be read-only.
        make_read_only(tmp_src_path)
        tmp_src_paths.append(tmp_src_path)
      self.raw_put(tmp_src_paths, cs_dir)

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
    content_store_dir_suffix = self.content_store_path_from_key(self._random_string())
    content_store_dir = 'GITSHED_CLIENT_CHECK_DELETABLE/{0}'.format(content_store_dir_suffix)
    content_store_path = '{0}/GITSHED_CLIENT_CHECK_KEY'.format(content_store_dir)
    if self.raw_has(content_store_path):
      raise GitShedError('Test key unexpectedly found.')
    with temporary_dir() as tmpdir:
      tmpfile = os.path.join(tmpdir, 'GITSHED_CLIENT_CHECK_KEY')
      content = b'FAKE FILE CONTENT.'
      with open(tmpfile, 'w') as outfile:
        outfile.write(content)
      self.raw_put([tmpfile], content_store_dir)
      if not self.raw_has(content_store_path):
        raise GitShedError('Test key not found. Content store write failed?')
      roundtripped_tmpdir = os.path.join(tmpdir, 'roundtripped')
      os.mkdir(roundtripped_tmpdir)
      self.raw_get([content_store_path], roundtripped_tmpdir)
      with open(os.path.join(roundtripped_tmpdir, 'GITSHED_CLIENT_CHECK_KEY'), 'r') as infile:
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

  def raw_put(self, src_paths, content_store_dir):
    """Puts the contents of files into the content store.

    Subclasses must implement.

    :param src_paths: Files to put into the content store.
    :param content_store_dir: Put the content into this directory in the content store, using
                              the basename of each file as its content's name in that directory.
    """
    raise NotImplementedError()

  def raw_has(self, content_store_path):
    """Checks for the existence of content at a logical path in this content store.

    Subclasses must implement.

    :param content_store_path: Check for content at this content store path.
    """
    raise NotImplementedError()
