import os
import subprocess

from .base import BaseFactory

class FilesystemFactory(BaseFactory):
    """
    Wraps executable files on a filesystem in python's subprocess module.

    Any extra __init__ kwargs are passed to subprocess.Popen, ie. pass either
      stdout=subprocess.PIPE to capture it,
      stdout=subprocess.DEVNULL to discard it,
      leave it as None (default) to not touch it

    The wrapper return value is a subprocess.CompletedProcess instance,
    it raises subprocess.CalledProcessError if the executable fails.

    Arguments:
        follow - recursively follow symlinks to directories
    """
    def __init__(self, follow=False, **kwargs):
        super().__init__()
        self.follow_symlinks = follow
        self.extra_args = kwargs

    def wrap_executable(self, dirpath, fname):
        full = os.path.join(dirpath, fname)
        def run_exec():
            p = subprocess.run([fname],
                               cwd=dirpath,
                               executable=os.path.abspath(full),
                               check=True,
                               **self.extra_args)
            return p
        self.callpath_burn(run_exec, fname, path=dirpath.strip('/').split('/'))
        return run_exec

    def valid_executable(self, dirpath, fname):
        full = os.path.join(dirpath, fname)
        if os.access(full, os.X_OK):
            return True

    def __call__(self, startpath):
        for data in os.walk(startpath, followlinks=self.follow_symlinks,
                            topdown=True):
            dirpath, dirs, files = data
            # ignore hidden, works when topdown=True
            files[:] = [f for f in files if not f[0] == '.']
            dirs[:] = [d for d in dirs if not d[0] == '.']
            for fname in files:
                if self.valid_executable(dirpath, fname):
                    yield self.wrap_executable(dirpath, fname)
