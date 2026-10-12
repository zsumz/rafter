"""Child protocol for an isolated-filesystem stall qualification."""
import hashlib
import json
import os
import sys
import time

BASELINE = b'B' * 4096
UPDATED = b'R' * 4096


def emit(event, **fields):
    print(json.dumps(dict(event=event, pid=os.getpid(),
                          monotonic_ns=time.monotonic_ns(), **fields)), flush=True)


def write_all(fd, payload):
    remaining = memoryview(payload)
    while remaining:
        written = os.write(fd, remaining)
        if written == 0:
            raise OSError('probe write made no progress')
        remaining = remaining[written:]


def main():
    if len(sys.argv) != 2:
        raise SystemExit('usage: fsync_probe.py NEW_PROBE_FILE')
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_CLOEXEC', 0)
    fd = os.open(sys.argv[1], flags, 0o600)
    try:
        write_all(fd, BASELINE)
        os.fsync(fd)
        identity = os.fstat(fd)
        emit('ready', device=identity.st_dev, inode=identity.st_ino)
        # The controller suspends only its owned mapping before sending this.
        if sys.stdin.readline(64) != 'go\n':
            raise SystemExit('probe requires exactly one go command')
        os.lseek(fd, 0, os.SEEK_SET)
        write_all(fd, UPDATED)
        emit('before_fsync')
        os.fsync(fd)
        emit('synced', sha256=hashlib.sha256(UPDATED).hexdigest())
    finally:
        os.close(fd)


if __name__ == '__main__':
    main()
