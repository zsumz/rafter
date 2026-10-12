"""Portable protocol controls; these do not claim a Linux kernel I/O stall."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from io_stall_controller import cleanup_child, run_probe

PROBE = Path(__file__).resolve().parents[1] / 'io_stall_probe.py'
MODEL = '''import hashlib,json,os,sys,time
from pathlib import Path
p=Path(sys.argv[1]);p.write_bytes(b'B'*4096);s=p.stat()
def emit(event, **kw):
 print(json.dumps(dict(event=event,pid=os.getpid(),**kw)),flush=True)
emit('ready', device=s.st_dev, inode=s.st_ino)
assert sys.stdin.readline()=='go\\n'
emit('before_fsync')
while not p.with_name('release').exists(): time.sleep(0.005)
p.write_bytes(b'R'*4096)
emit('synced',sha256=hashlib.sha256(p.read_bytes()).hexdigest())
'''


class Gate:
    def __init__(self, mount):
        self.mount = mount
        self.resumes = 0
        self.fail_resume = False

    def suspend(self):
        pass

    def resume(self):
        self.resumes += 1
        if self.fail_resume:
            raise RuntimeError('injected resume failure')
        (self.mount / 'release').touch()


class ProbeProtocol(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rafter-probe-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.mount = self.root / 'mount'
        self.mount.mkdir()
        self.artifacts = self.root / 'evidence'
        self.artifacts.mkdir()
        self.script = self.root / 'model.py'
        self.script.write_text(MODEL)
        self.volume = Gate(self.mount)

    def run_model(self):
        return run_probe(self.volume, self.script, self.artifacts, .08, 2.0)

    def assert_reaped(self):
        receipt = json.loads((self.artifacts / 'probe-cleanup.json').read_text())
        self.assertIsNotNone(receipt['returncode'])
        with self.assertRaises(ProcessLookupError):
            os.kill(receipt['pid'], 0)
        return receipt

    def test_modeled_order_requires_release_then_success(self):
        events = self.run_model()
        self.assertEqual([e['event'] for e in events],
                         ['ready', 'mapping_suspended', 'before_fsync', 'mapping_resumed', 'synced'])
        self.assertEqual(self.assert_reaped()['status'], 'passed')
        self.assertEqual((self.mount / 'fsync-probe').read_bytes(), b'R' * 4096)

    def test_real_fsync_without_stall_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'did not remain blocked'):
            run_probe(self.volume, PROBE, self.artifacts, .25, 2.0)
        self.assert_reaped()

    def test_malformed_streams_and_forged_identity_fail(self):
        variants = {
            'pid': MODEL.replace('pid=os.getpid()', 'pid=0'),
            'inode': MODEL.replace('inode=s.st_ino', 'inode=0'),
            'oversize': "print('x'*3000,flush=True)",
            'unterminated': "print('{}',end='',flush=True)",
            'extra-null': MODEL + "print('null',flush=True)\n",
            'extra-event': MODEL + "emit('unexpected')\n",
            'string-pending': MODEL.replace("emit('before_fsync')", "emit('before_fsync');print(json.dumps('pending'),flush=True)"),
            'bad-hash': MODEL.replace('hashlib.sha256(p.read_bytes()).hexdigest()', "'wrong'"),
            'bad-data': MODEL.replace("p.write_bytes(b'R'*4096)", "p.write_bytes(b'X'*4096)"),
            'unsuccessful-exit': MODEL + 'sys.exit(23)\n',
        }
        for name, code in variants.items():
            with self.subTest(case=name):
                self.script.write_text(code)
                for path in self.mount.iterdir():
                    path.unlink()
                with self.assertRaises((RuntimeError, ValueError, TimeoutError)):
                    self.run_model()
                self.assert_reaped()

    def test_resume_error_does_not_skip_reaping_or_receipt(self):
        self.volume.fail_resume = True
        with self.assertRaisesRegex(RuntimeError, 'cleanup incomplete'):
            self.run_model()
        receipt = self.assert_reaped()
        self.assertEqual(receipt['status'], 'failed')
        self.assertTrue(any('resume' in error for error in receipt['errors']))

    def test_cleanup_reports_kernel_blocked_child_without_claiming_reap(self):
        order = []
        volume, child, events = Mock(), Mock(), Mock()
        def resume():
            order.append('resume')
            raise RuntimeError('cannot resume')
        volume.resume.side_effect = resume
        child.pid, child.returncode = 123, None
        child.poll.return_value = None
        child.kill.side_effect = lambda: order.append('kill')
        child.wait.side_effect = subprocess.TimeoutExpired('probe', .1)
        with self.assertRaisesRegex(RuntimeError, 'cleanup incomplete'):
            cleanup_child(volume, child, events, self.artifacts, .1)
        self.assertEqual(order, ['resume', 'kill'])
        receipt = json.loads((self.artifacts / 'probe-cleanup.json').read_text())
        self.assertEqual(len(receipt['errors']), 2)
        self.assertIsNone(receipt['returncode'])
        events.close.assert_called_once()
        child.stdout.close.assert_called_once()

    def test_deadlines_validated_before_any_effect(self):
        for value in [0, -1, float('nan'), float('inf')]:
            for hold, wait in [(value, 1), (1, value)]:
                with self.assertRaises(ValueError):
                    run_probe(self.volume, PROBE, self.artifacts, hold, wait)
        self.assertEqual(list(self.artifacts.iterdir()), [])
        self.assertEqual(self.volume.resumes, 0)

    def test_real_probe_protocol_and_existing_file_preservation(self):
        path = self.mount / 'probe'
        for command, success in [('go\n', True), ('wrong\n', False), ('', False)]:
            result = subprocess.run([sys.executable, str(PROBE), str(path)],
                                    input=command, text=True, capture_output=True, timeout=2)
            self.assertEqual(result.returncode == 0, success)
            self.assertEqual(path.read_bytes(), (b'R' if success else b'B') * 4096)
            if success:
                events = [json.loads(line) for line in result.stdout.splitlines()]
                self.assertEqual([e['event'] for e in events], ['ready', 'before_fsync', 'synced'])
                self.assertEqual(events[-1]['sha256'], hashlib.sha256(b'R' * 4096).hexdigest())
            path.unlink()
        path.write_bytes(b'preserve')
        result = subprocess.run([sys.executable, str(PROBE), str(path)],
                                input='go\n', text=True, capture_output=True, timeout=2)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(path.read_bytes(), b'preserve')


if __name__ == '__main__':
    unittest.main()
