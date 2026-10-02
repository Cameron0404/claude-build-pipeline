#!/usr/bin/env python3
"""The one atomic file write for the pipeline. Stdlib only, Python 3.9.

    import atomicio
    atomicio.write_text(path, text)          # utf-8
    atomicio.write_bytes(path, data)

A reader sees the old file or the new one, never half of either, and after a power loss the
file is one of the two, never empty. The steps, and why each is there:

  1. a temp file from mkstemp in the SAME directory, so the rename cannot cross a filesystem,
     and a unique name, so two processes writing the same file at once cannot collide on a
     shared `path.with_suffix(".tmp")`
  2. write, flush, fsync the temp file: without this the rename is ordered but the data is
     not, and a crash can leave the new name pointing at an empty file
  3. the existing file's mode is kept (mkstemp creates 0600); a new file gets 0644
  4. os.replace, the atomic step
  5. fsync the directory, so the rename itself survives the crash; best effort, because not
     every filesystem lets a directory be opened

The temp name starts with a dot and ends `.tmp`, so a glob for `*.md` or `*.json` never
picks up a write in flight. On any failure the temp file is removed and the error raised;
the old file is untouched.
"""

from __future__ import annotations

import os
import tempfile


def write_bytes(path, data: bytes) -> None:
    path = os.fspath(path)
    d = os.path.dirname(path) or "."
    try:
        mode = os.stat(path).st_mode & 0o777
    except FileNotFoundError:
        mode = 0o644
    fd, tmp = tempfile.mkstemp(prefix="." + os.path.basename(path) + ".", suffix=".tmp", dir=d)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    try:
        dfd = os.open(d, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dfd)
    except OSError:
        pass
    finally:
        os.close(dfd)


def write_text(path, text: str, encoding: str = "utf-8") -> None:
    write_bytes(path, text.encode(encoding))
