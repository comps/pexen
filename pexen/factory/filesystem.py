import os
import subprocess

from .base import BaseFactory

class FilesystemFactory(BaseFactory):
    """
    Wraps executable files on a filesystem in python's subprocess module.

    The wrapper return value is a subprocess.CompletedProcess instance.

    Arguments:
        follow - recursively follow symlinks to directories
        capture - intercept stdout/stderr, returning it in CompletedProcess
    """
    def __init__(self, follow=False, capture=True):
        super().__init__()
        self.follow_symlinks = follow
        self.capture_output = capture

    def wrap_executable(self, path):
        def run_exec():
            p = subprocess.run([os.path.basename(path)],
                               cwd=os.path.dirname(path),
                               executable=os.path.abspath(path),
                               capture_output=self.capture_output,
                               check=True)
            return p
        return run_exec

    def __call__(self, startpath):
        for data in os.walk(startpath, followlinks=self.follow_symlinks,
                            topdown=True):
            dirpath, dirs, files = data
            # ignore hidden, works when topdown=True
            files[:] = [f for f in files if not f[0] == '.']
            dirs[:] = [d for d in dirs if not d[0] == '.']
            for fname in files:
                fullpath = os.path.join(dirpath, fname)
                if os.access(fullpath, os.X_OK):
                    yield self.wrap_executable(fullpath)
