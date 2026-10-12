"""Crash ordering and retained recovery inputs; no kernel stall or repair is simulated."""
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from io_stall_counter import CounterWorkload, preserve_crash_inputs


class CrashQualification(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rafter-stall-crash-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.mount = self.root / 'mount'
        self.data = self.mount / 'data'
        self.data.mkdir(parents=True)
        self.artifacts = self.root / 'evidence'
        self.artifacts.mkdir()
        for node in (1, 2, 3):
            folder = self.data / f'node-{node}'
            folder.mkdir()
            (folder / 'counter').write_bytes(bytes([node]) * 5000)
            (folder / '.rafter-storage.lock').write_bytes(b'')
        self.counter = CounterWorkload.__new__(CounterWorkload)
        self.counter.mount, self.counter.artifacts = self.mount, self.artifacts
        self.counter.events = []
        self.counter.cluster = Mock()
        self.cluster = self.counter.cluster
        self.cluster.children = {node: Mock(pid=node + 100) for node in (1, 2, 3)}
        for child in self.cluster.children.values():
            child.poll.return_value = None

    def test_kill_requests_do_not_wait_before_resume(self):
        self.counter.kill_while_stalled()
        for child in self.cluster.children.values():
            child.kill.assert_called_once()
            child.wait.assert_not_called()
        self.assertEqual([e['event'] for e in self.counter.events], ['kill_requested'] * 3)

    def test_missing_or_already_exited_process_refuses_before_killing(self):
        children = dict(self.cluster.children)
        del self.cluster.children[3]
        with self.assertRaises(AssertionError):
            self.counter.kill_while_stalled()
        self.cluster.children = children
        children[3].poll.return_value = 0
        with self.assertRaises(AssertionError):
            self.counter.kill_while_stalled()
        for child in children.values():
            child.kill.assert_not_called()

    def test_real_child_signals_and_input_copy_precede_restart(self):
        children = {node: subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    for node in (1, 2, 3)}
        self.cluster.children = dict(children)
        def restart(node):
            self.assertTrue((self.artifacts / 'pre-recovery-data.json').is_file())
            self.assertTrue(all(child.returncode == -signal.SIGKILL for child in children.values()))
        self.cluster.start.side_effect = restart
        try:
            self.counter.kill_while_stalled()
            # Represents the caller's resume boundary; only now may reaping start.
            self.counter.recover_crashed()
        finally:
            for child in children.values():
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=5)
        self.assertEqual(self.cluster.start.call_count, 3)
        names = [event['event'] for event in self.counter.events]
        self.assertEqual(names, ['kill_requested'] * 3 + ['crash_reaped'] * 3 +
                         ['crash_inputs_saved', 'strict_recovery_started'])

    def test_unexpected_exit_preserves_cleanup_ownership_and_does_not_restart(self):
        self.cluster.children[1].wait.return_value = 0
        with self.assertRaisesRegex(AssertionError, 'did not exit from SIGKILL'):
            self.counter.recover_crashed()
        self.assertIn(1, self.cluster.children)
        self.cluster.start.assert_not_called()
        self.assertFalse((self.artifacts / 'pre-recovery-data').exists())

    def test_copy_preserves_every_byte_and_manifest_hash(self):
        before = {str(p.relative_to(self.data)): p.read_bytes() for p in self.data.rglob('*') if p.is_file()}
        manifest = preserve_crash_inputs(self.data, self.artifacts)
        self.assertEqual(set(before), set(manifest))
        self.assertEqual(manifest, json.loads((self.artifacts / 'pre-recovery-data.json').read_text()))
        for name, content in before.items():
            self.assertEqual((self.data / name).read_bytes(), content)
            self.assertEqual((self.artifacts / 'pre-recovery-data' / name).read_bytes(), content)
            self.assertEqual(manifest[name], dict(bytes=len(content), sha256=hashlib.sha256(content).hexdigest()))

    def test_links_are_refused_without_reading_external_contents(self):
        outside = self.root / 'outside'
        outside.write_text('preserve')
        (self.data / 'outside-link').symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, 'non-regular'):
            preserve_crash_inputs(self.data, self.artifacts)
        self.assertEqual(outside.read_text(), 'preserve')
        self.assertFalse((self.artifacts / 'pre-recovery-data/outside-link').exists())

    def test_changed_source_cannot_produce_successful_manifest(self):
        import shutil
        original = shutil.copyfile
        def copy_then_change(source, destination):
            result = original(source, destination)
            source.write_bytes(b'changed')
            return result
        with patch('io_stall_counter.shutil.copyfile', side_effect=copy_then_change):
            with self.assertRaisesRegex(RuntimeError, 'changed before recovery'):
                preserve_crash_inputs(self.data, self.artifacts)
        self.assertFalse((self.artifacts / 'pre-recovery-data.json').exists())

    def test_existing_artifact_and_empty_inventory_refused(self):
        (self.artifacts / 'pre-recovery-data').mkdir()
        with self.assertRaises(FileExistsError):
            preserve_crash_inputs(self.data, self.artifacts)
        (self.artifacts / 'pre-recovery-data').rmdir()
        empty = self.root / 'empty'
        empty.mkdir()
        with self.assertRaisesRegex(RuntimeError, 'empty'):
            preserve_crash_inputs(empty, self.artifacts)

    def test_cli_crash_mode_requires_counter_before_any_effect(self):
        script = Path(__file__).resolve().parents[1] / 'check-io-stall'
        output = self.root / 'must-not-exist'
        result = subprocess.run([sys.executable, str(script), '--crash-while-stalled',
                                 '--artifacts', str(output)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('requires --counter-binary', result.stderr)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
