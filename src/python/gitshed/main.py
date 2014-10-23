#!/usr/local/bin/python
# coding=utf-8

from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from contextlib import contextmanager
from functools import update_wrapper
import glob
import os

import click
import sys

from gitshed.gitshed import GitShed


def gitshed_instance():
  """Returns a GitShed instance to work with."""
  config_file_path = os.path.join('.gitshed', 'config.json')
  return GitShed.from_config(config_file_path)

_verbose = False


@contextmanager
def exception_handling():
  """A context that handles exceptions based on the value of the --verbose flag."""
  try:
    yield
  except Exception as e:
    if _verbose:  # Raise the exception so we see a full stack trace.
      raise
    else:  # Delegate exception handling to click, which will suppress the stack trace.
      raise click.ClickException(str(e))


def path_glob_args(func):
  """A useful decorator that expands path glob options into a list of matched paths.

  E.g.,

  @path_glob_args
  def func(paths):
    ...

  The decorated func will take 'argfile' and path_globs' arguments and pass the expanded paths
  to func(). Will pass None to func if both arguments were unspecified. This allows func to
  distinguish no arguments from arguments that evaluate to an empty list of paths.
  """
  @click.option('-f', '--argfile', type=click.File('r'),
                help='Manage the paths listed in this file, one per line.')
  @click.argument('path_globs', nargs=-1)
  @click.pass_context
  def path_glob_func(ctx, argfile, path_globs):
    with exception_handling():
      if not argfile and not path_globs:
        paths = None
      else:
        paths = [path for path_glob in path_globs for path in glob.glob(path_glob)]
        if argfile:
          paths.extend(argfile.read().splitlines())
          argfile.close()
      return ctx.invoke(func, paths)

  return update_wrapper(path_glob_func, func)


# The click subcommands, all under the 'gitshed' main command.

@click.group()
@click.option('-v', '--verbose/--no-verbose', default=False, help='Show detailed run information.')
def gitshed(verbose):
  global _verbose
  _verbose = verbose


@click.command()
def status():
  with exception_handling():
    gitshed_instance().status()


@click.command()
def synced():
  with exception_handling():
    gitshed_instance().synced()


@click.command()
def unsynced():
  with exception_handling():
    gitshed_instance().unsynced()


@click.command()
@path_glob_args
def manage(paths):
  if paths:
    with exception_handling():
      gb = gitshed_instance()
      gb.manage(paths)


@click.command()
@path_glob_args
def sync(paths):
  with exception_handling():
    gb = gitshed_instance()
    if paths is None:
      gb.sync_all()
    elif paths:
      gb.sync(paths)


@click.command()
@path_glob_args
def resync(paths):
  with exception_handling():
    gb = gitshed_instance()
    if paths is None:
      gb.resync_all()
    elif paths:
      gb.resync(paths)


@click.command()
def setup():
  with exception_handling():
    gitshed_instance().verify_setup()


gitshed.add_command(status)
gitshed.add_command(synced)
gitshed.add_command(unsynced)
gitshed.add_command(manage)
gitshed.add_command(sync)
gitshed.add_command(resync)
gitshed.add_command(setup)


if __name__ == '__main__':
  # A hacky way to make the usage message more useful.
  # TODO: Is there a more legit way to do this in click?
  sys.argv[0] = 'git shed'
  gitshed()
