"""Bounded child protocol for a filesystem stall; resume precedes child cleanup."""
import hashlib
import json
import math
import os
import selectors
import subprocess
import sys
import time


EOF = object()


class Events:
    def __init__(self, pipe):
        self.pipe = pipe
        self.buffer = b''
        self.selector = selectors.DefaultSelector()
        self.selector.register(pipe, selectors.EVENT_READ)

    def next(self, timeout):
        deadline = time.monotonic() + timeout
        while True:
            if b'\n' in self.buffer:
                line, self.buffer = self.buffer.split(b'\n', 1)
                if len(line) > 2048:
                    raise RuntimeError('oversized probe event')
                return json.loads(line)
            if len(self.buffer) > 2048:
                raise RuntimeError('oversized probe event')
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise TimeoutError('probe event deadline expired')
            data = os.read(self.pipe.fileno(), 2049)
            if not data:
                if self.buffer:
                    raise RuntimeError('unterminated probe event')
                return EOF
            self.buffer += data

    def close(self):
        self.selector.close()


def cleanup_child(volume, child, events, artifacts, wait_seconds):
    errors = []
    # A kernel-blocked process may not be reapable until storage resumes.
    try:
        volume.resume()
    except BaseException as error:
        errors.append('resume: ' + repr(error))
    try:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=wait_seconds)
    except BaseException as error:
        errors.append('reap: ' + repr(error))
    events.close()
    child.stdin.close()
    child.stdout.close()
    receipt = dict(pid=child.pid, returncode=child.returncode, errors=errors,
                   status='failed' if errors else 'passed')
    (artifacts / 'probe-cleanup.json').write_text(json.dumps(receipt, indent=2) + '\n')
    if errors:
        raise RuntimeError('probe cleanup incomplete: ' + '; '.join(errors))


def run_probe(volume, script, artifacts, hold_seconds=3.0, wait_seconds=15.0):
    """Require fsync to stay pending until the owned mapping is resumed."""
    if any(not math.isfinite(x) or x <= 0 for x in (hold_seconds, wait_seconds)):
        raise ValueError('probe deadlines must be positive and finite')
    events = []
    path = volume.mount / 'fsync-probe'
    with (artifacts / 'probe.stderr').open('w') as error_log:
        child = subprocess.Popen([sys.executable, str(script), str(path)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=error_log)
        pending = Events(child.stdout)

        def expect(name):
            event = pending.next(wait_seconds)
            if not isinstance(event, dict) or event.get('event') != name:
                raise RuntimeError(f'expected probe event {name}: {event!r}')
            if event.get('pid') != child.pid:
                raise RuntimeError('probe event names another process')
            events.append(event)
            return event

        try:
            ready = expect('ready')
            identity = path.stat()
            if (ready.get('device'), ready.get('inode')) != (identity.st_dev, identity.st_ino):
                raise RuntimeError('probe opened a different file')
            volume.suspend()
            events.append({'event': 'mapping_suspended', 'monotonic_ns': time.monotonic_ns()})
            child.stdin.write(b'go\n')
            child.stdin.flush()
            expect('before_fsync')
            still_pending = object()
            try:
                observed = pending.next(hold_seconds)
            except TimeoutError:
                observed = still_pending
            if observed is not still_pending or child.poll() is not None:
                raise RuntimeError(f'probe did not remain blocked: {observed!r}')
            volume.resume()
            events.append({'event': 'mapping_resumed', 'monotonic_ns': time.monotonic_ns()})
            synced = expect('synced')
            if child.wait(timeout=wait_seconds) != 0:
                raise RuntimeError('probe did not exit successfully after resume')
            if pending.next(wait_seconds) is not EOF:
                raise RuntimeError('probe emitted data after its completion event')
            payload = path.read_bytes()
            if payload != b'R' * 4096 or synced.get('sha256') != hashlib.sha256(payload).hexdigest():
                raise RuntimeError('synced probe content differs from its receipt')
            return events
        finally:
            # Log-write failure must not skip releasing the suspended mapping.
            try:
                (artifacts / 'probe-events.json').write_text(json.dumps(events, indent=2) + '\n')
            finally:
                cleanup_child(volume, child, pending, artifacts, wait_seconds)
