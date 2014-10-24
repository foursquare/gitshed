# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

import json
import os
import re
import shutil
import sys

from gitshed.error import GitShedError
from gitshed.local_content_store import LocalContentStore
from gitshed.remote_content_store import RSyncedRemoteContentStore
from gitshed.repo import GitRepo
from gitshed.util import run_cmd_str, safe_makedirs, safe_rmtree


class GitShed(object):
  """The main git shed functionality.

  A managed file is represented as a versioned symlink in the repo. The symlink points to a file
  outside the repo, in the "shed" (<workspace>/.gitshed/files).

  The contents of files are read from a (typically remote) content store, and written to those
  symlinked files.

  A file can be in two states:
  - Synced: The symlink points to a file containing the content.
  - Unsynced: The file's content isn't present on the local system, so the symlink is dangling.

  The 'sync' operation fetches the content from the content store and writes it to the
  symlink's target.

  The 'manage <file>' operation replaces <file> with a symlink and writes the contents to
  the content store.

  Synced symlinks are safe to commit, as their contents are available in the content store for
  remote repos to access. Unsynced symlinks are not safe to commit. A git hook is available
  to verify that this doesn't happen.
  """
  @classmethod
  def from_config(cls, config_file_path):
    """Creates a GitShed instance from a config file.

    Assumes that the cwd is a git repo.

    :param config_file_path: The path to the config file to read.
    """
    class MissingConfigKeyError(GitShedError):
      """Thrown when an expected config key is not present."""
      def __init__(self, key_error):
        """Wrap the specified KeyError instance."""
        super(MissingConfigKeyError, self).__init__(
          'Invalid content store config at {0}. Unknown key: {1}'.format(config_file_path, key_error.args[0]))

    repo = GitRepo(os.getcwd())
    try:
      with open(config_file_path, 'r') as infile:
        config = json.load(infile)
    except IOError:
      raise GitShedError('No config file found at {0}'.format(config_file_path))
    except ValueError as e:
      raise GitShedError('Invalid content store config at {0}: {1}'.format(config_file_path, e))

    try:
      exclude = config.get('exclude')
      concurrency = config.get('concurrency', {})
      content_store_cfg = config['content_store']
    except KeyError as e:
      raise MissingConfigKeyError(e)

    if 'remote' in content_store_cfg:
      try:
        rcfg = content_store_cfg['remote']
        host = rcfg['host']
        root_path = rcfg['root_path']
        root_url = rcfg['root_url']
        timeout_secs = rcfg.get('timeout_secs', 5)
      except KeyError as e:
        raise MissingConfigKeyError(e)
      content_store = RSyncedRemoteContentStore(host, root_path, root_url,
                                                timeout_secs,
                                                concurrency.get('get'),
                                                concurrency.get('put'))
    elif 'local' in content_store_cfg:
      try:
        root = content_store_cfg['local']['root']
      except KeyError as e:
        raise MissingConfigKeyError(e)
      content_store = LocalContentStore(root,
                                        concurrency.get('get'),
                                        concurrency.get('put'))
    else:
      raise GitShedError('No content store specified in config at {0}'.format(config_file_path))

    return cls(repo, content_store, exclude=exclude)


  def __init__(self, git_repo, content_store, exclude=None):
    super(GitShed, self).__init__()
    self._git_repo = git_repo
    self._exclude = exclude or []
    if '.git' not in self._exclude:
      self._exclude.append('.git')
    if '.gitshed' not in self._exclude:
      self._exclude.append('.gitshed')
    self._content_store = content_store
    self._shed_relpath = self._git_repo.relpath(os.path.join('.gitshed', 'files'))
    safe_makedirs(self._shed_relpath)

  @property
  def git_repo(self):
    return self._git_repo

  def get_status(self):
    """Returns a pair of the total number of files in gitshed and the number of unsynced files."""
    all_symlinks = self._find_all_symlinks()
    broken_symlinks = self._find_broken_symlinks(all_symlinks)
    n = len(all_symlinks)
    b = len(broken_symlinks)
    return n, b

  def status(self, out=sys.stdout):
    """Prints a succinct status message."""
    n, b = self.get_status()
    out.write('{0} files in gitshed. {1} synced. {2} need syncing.\n'.format(n, n - b, b))
    if b:
      out.write('Use "git shed unsynced" to list unsynced files.\n')
      out.write('Use "git shed sync <file_glob>" to sync specific files.\n')
      out.write('Use "git shed sync" to sync all files.\n')

  def synced(self, out=sys.stdout):
    """Prints all synced files."""
    all_symlinks = self._find_all_symlinks()
    broken_symlinks = self._find_broken_symlinks(all_symlinks)
    valid_symlinks = sorted(set(all_symlinks) - set(broken_symlinks))
    for symlink in valid_symlinks:
      out.write(symlink)
      out.write('\n')

  def unsynced(self, out=sys.stdout):
    """Prints all unsynced files."""
    all_symlinks = self._find_all_symlinks()
    broken_symlinks = self._find_broken_symlinks(all_symlinks)
    for symlink in broken_symlinks:
      out.write(symlink)
      out.write('\n')

  def sync_all(self):
    """Syncs all unsynced files."""
    links = self._find_all_symlinks()
    self.sync(links)

  def resync_all(self):
    """Resyncs all files.

    Removes the existing versions from the shed, and refetches them from the content store.
    """
    # Convert to an abspath first, to verify that self._shed_relpath is under the git repo as expected.
    # This is an extra safety check in case of bugs.
    gitshed_abspath = self._git_repo.abspath(self._shed_relpath)
    safe_rmtree(gitshed_abspath)
    self.sync_all()

  def sync(self, paths):
    """Syncs the specified files.

    A no-op for paths that aren't unsynced files managed by gitshed.

    :param paths: The files to sync.
    """
    unsynced_paths = [p for p in paths if os.path.islink(p) and not os.path.exists(p)]
    args = []
    for path in unsynced_paths:
      target_path = self._get_gitshed_path(path)
      key = self._get_key_from_versioned_path(target_path)
      args.append((key, target_path))
    self._content_store.multi_get(args)

  def resync(self, paths):
    """Resyncs the specified files.

    Removes the existing versions from the shed, and refetches them from the content store.

    ":param paths: The files to resync.
    """
    for p in paths:
      if os.path.islink(p):
        gitshed_path = self._get_gitshed_path(p)
        if gitshed_path:
          os.unlink(gitshed_path)
    self.sync(paths)

  def manage(self, paths):
    """Puts files under management by gitshed.

    For each file:
      - Copies it into the shed.
      - Uploads its contents to the content store.
      - Replaces it with a symlink.

    :param paths: Put these files under management.
    """
    relpaths = []
    for path in paths:
      relpath = self._git_repo.relpath(path)
      # No-op if this path is already under our management.
      if not self._is_managed(relpath):
        if os.path.islink(relpath):
          raise GitShedError('Path is an unmanaged symlink: {0}'.format(relpath))
        if os.path.isdir(relpath):
          raise GitShedError('Path is a directory: {0}'.format(relpath))
        if not os.path.isfile(relpath):
          raise GitShedError('File not found: {0}'.format(relpath))
        relpaths.append(relpath)

    # Upload everything to the content store.
    keys = self._content_store.multi_put(relpaths)

    # Move the files into the shed.
    for key, relpath in zip(keys, relpaths):
      versioned_relpath = self._create_versioned_path(relpath, key)
      target_abspath = os.path.abspath(os.path.join(self._shed_relpath, versioned_relpath))
      if os.path.exists(target_abspath):
        raise GitShedError("Shed path already exists: {0}. "
                           "Delete it manually, but only if you're sure it's safe to do so.")

      safe_makedirs(os.path.dirname(target_abspath))
      shutil.move(relpath, target_abspath)
      # We want the symlink to be relative, so it's portable.
      rel_link = os.path.relpath(target_abspath, os.path.abspath(os.path.dirname(relpath)))
      os.symlink(rel_link, relpath)

  def verify_setup(self):
    """Verifies that the repo is set up properly for gitshed use."""
    if not self._git_repo.is_ignored(self._shed_relpath):
      relpath = self._shed_relpath
      prompt = '{0} is not in your .gitignore file. Add it? [Y/n] '.format(self._shed_relpath)
      yn = raw_input(prompt)
      if yn.lower() not in ['y', 'yes']:
        raise GitShedError('{0} must be in your .gitignore file.'.format(self._shed_relpath))
      with open('.gitignore', 'a') as outfile:
        outfile.write('\n{0}\n'.format(relpath))
    self._content_store.verify_setup()

  _KEY_RE = re.compile(r'^[0-9a-f]{40}$')  # Matches exactly 40 hex digits.

  @classmethod
  def is_valid_key(cls, key):
    return cls._KEY_RE.match(key)

  def _get_gitshed_path(self, path):
    """If path is a symlink into the gitshed, returns the path it links to, relative to the repo root.

    Returns None otherwise.
    """
    if os.path.islink(path):
      target_path = self._git_repo.relpath(os.path.join(os.path.dirname(path), os.readlink(path)))
      if target_path.startswith(self._shed_relpath):
        return target_path
    return None

  def _create_versioned_path(self, path, key):
    """Adds a key to a path.

    Returns the path with the key prepended to the file name.

    :param path: The unversioned path.
    :param key: The key to add to the path.
    """
    if not self.is_valid_key(key):
      raise GitShedError('Invalid version string: {0}'.format(key))
    # We prefix the file name with the key, rather than suffixing it, so that the file extension
    # is preserved. This prevents programs that interpret the extension from getting confused.
    (dirname, filename) = os.path.split(path)
    return os.path.join(dirname, '{0}.{1}'.format(key, filename))

  def _get_key_from_versioned_path(self, path):
    """Gets a key out of a path created by _create_versioned_path.

    :param path: The versioned path.
    """
    key, sep, _ = os.path.basename(path).partition('.')
    if not sep or not self.is_valid_key(key):
      raise GitShedError('No version in path {0}'.format(path))
    return key

  def _generate_find_command(self):
    """Constructs a UNIX 'find' command line suitable for use by _find_all_symlinks.

    See `man find` for further information.
    """
    # Only look for symlinks into the shed.
    # E.g., -lname "*.gitshed/files/*"
    shed_predicate  = '-lname "{shed_relpath}"'.format(shed_relpath=os.path.join('*', self._shed_relpath, '*'))

    # Don't descend into the directories we've excluded. This isn't required for correctness,
    # as presumably these directories won't contain symlinks into the shed. But it improves
    # the performance of the find command.
    # E.g., -path "./.pants.d" -o -path "./.pants.bootstrap"
    exclude_strs = []
    for ex in self._exclude:
      exclude_strs.append('-path "./{excluded_relpath}"'.format(excluded_relpath=ex))
    exclude_predicate = ' -o '.join(exclude_strs)

    # Put it all together.
    # E.g.,
    #
    # find . \( -path "./.pants.d" -o -path "./.pants.bootstrap" \) -prune -o -lname "*/.gitshed/files/*" -print
    #
    # Read this as: find from the cwd, prune these paths from the search tree, and print any
    # symlinks whose targets match this pattern.
    cmd_str = 'find . \( {prune_paths} \) -prune -o {print_paths} -print'.format(
      prune_paths=exclude_predicate,
      print_paths=shed_predicate)

    return cmd_str

  def _find_all_symlinks(self):
    """Finds all symlinks in the repo that point to files in the shed.

    These correspond to all the files managed by gitshed.
    """
    cmd_str = self._generate_find_command()

    # Actually run the cmd.
    retcode, stdout, stderr = run_cmd_str(cmd_str)
    if retcode:
      raise GitShedError('Command failed: {0}.\nstderr: {1}'.format(cmd_str, stderr))
    return filter(None, stdout.split('\n'))

  def _find_broken_symlinks(self, all_symlinks):
    """Detects broken symlinks.

    These correspond to all the unsynced files managed by gitshed.

    :param all_symlinks: Detect from among these symlinks.
    """
    broken_symlinks = []
    for symlink in all_symlinks:
      if not os.path.exists(symlink):  # Returns False for broken symlinks.
        broken_symlinks.append(symlink)
    return broken_symlinks

  def _is_managed(self, relpath):
    """Is a path a symlink into the shed?

    :param relpath: The path to check, relative to the git repo root.
    """
    return self._get_gitshed_path(relpath) is not None

