"""Receipts must reject failed probes, cleanup, or changed source inputs."""
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
loader = importlib.machinery.SourceFileLoader('io_stall_runner', str(SCRIPTS / 'check-io-stall'))
spec = importlib.util.spec_from_loader(loader.name, loader)
runner = importlib.util.module_from_spec(spec)
loader.exec_module(runner)


class QualificationReceipt(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rafter-stall-receipt-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.volume = Mock()
        self.volume.root = self.root / 'owned'
        self.volume.image = self.volume.root / 'image'
        self.volume.name = self.volume.uuid = 'owned-test'
        self.volume.loop = '/dev/loop123'
        self.volume.sectors = 262144
        self.snapshot = dict(head='abc', sha256={'source': '123'})

    def qualify(self, defect=None):
        with patch.object(runner, 'Commands') as commands, \
                patch.object(runner, 'DisposableVolume', return_value=self.volume), \
                patch.object(runner, 'run_probe', return_value=['modeled']) as probe, \
                patch.object(runner, 'source_snapshot', return_value=self.snapshot) as source, \
                patch.object(runner.tempfile, 'mkdtemp', return_value=str(self.root / 'temporary')):
            if defect == 'probe':
                probe.side_effect = RuntimeError('probe failed')
            elif defect == 'cleanup':
                self.volume.close.side_effect = RuntimeError('cleanup failed')
            elif defect == 'source':
                source.side_effect = [self.snapshot, dict(head='changed')]
            elif defect == 'setup':
                self.volume.create.side_effect = RuntimeError('setup failed')
            elif defect == 'preflight':
                commands.return_value.side_effect = RuntimeError('preflight failed')
            receipt = runner.qualify(self.root / (defect or 'success'))
        retained = json.loads((self.root / (defect or 'success') / 'receipt.json').read_text())
        self.assertEqual(receipt, retained)
        return receipt

    def test_success_requires_probe_cleanup_and_source_preservation(self):
        receipt = self.qualify()
        self.assertEqual(receipt['status'], 'passed')
        self.assertTrue(receipt['source_preserved'])
        self.assertEqual(receipt['volume_cleanup'], 'passed')
        self.volume.close.assert_called_once()

    def test_each_failure_remains_a_failed_receipt(self):
        for defect in ['probe', 'cleanup', 'source', 'setup', 'preflight']:
            with self.subTest(defect=defect):
                self.volume.close.reset_mock(side_effect=True)
                self.volume.create.reset_mock(side_effect=True)
                receipt = self.qualify(defect)
                self.assertEqual(receipt['status'], 'failed')
                self.assertTrue(receipt['errors'])
                if defect != 'preflight':
                    self.volume.close.assert_called_once()

    def test_counter_cleanup_precedes_volume_release(self):
        from io_stall_counter import CounterWorkload
        binary = self.root / 'binary'
        binary.write_bytes(b'fixture')
        for fail in [False, True]:
            with self.subTest(cleanup_failure=fail):
                order = []
                self.volume.close.reset_mock(side_effect=True)
                self.volume.resume.side_effect = lambda: order.append('resume')
                self.volume.close.side_effect = lambda: order.append('volume')
                counter = Mock(spec=CounterWorkload)
                def close():
                    order.append('counter')
                    if fail:
                        raise RuntimeError('unreaped counter')
                counter.close.side_effect = close
                with patch.object(runner, 'Commands'), \
                        patch.object(runner, 'DisposableVolume', return_value=self.volume), \
                        patch.object(runner, 'run_probe', return_value=['modeled']), \
                        patch.object(runner, 'source_snapshot', return_value=self.snapshot), \
                        patch.object(runner.tempfile, 'mkdtemp', return_value=str(self.root)), \
                        patch('io_stall_counter.CounterWorkload', return_value=counter):
                    receipt = runner.qualify(self.root / f'counter-{fail}', binary)
                self.assertEqual(order, ['resume', 'counter'] if fail else ['resume', 'counter', 'volume'])
                self.assertEqual(receipt['status'], 'failed' if fail else 'passed')
                self.assertTrue(receipt['binary_preserved'])
                counter.start.assert_called_once()
                counter.finish.assert_called_once()

    def test_changed_counter_binary_fails_receipt(self):
        binary = self.root / 'binary'
        binary.write_bytes(b'before')
        counter = Mock()
        counter.finish.side_effect = lambda: binary.write_bytes(b'after')
        with patch.object(runner, 'Commands'), \
                patch.object(runner, 'DisposableVolume', return_value=self.volume), \
                patch.object(runner, 'run_probe', return_value=['modeled']), \
                patch.object(runner, 'source_snapshot', return_value=self.snapshot), \
                patch.object(runner.tempfile, 'mkdtemp', return_value=str(self.root)), \
                patch('io_stall_counter.CounterWorkload', return_value=counter):
            receipt = runner.qualify(self.root / 'changed-binary', binary)
        self.assertEqual(receipt['status'], 'failed')
        self.assertFalse(receipt['binary_preserved'])

    def test_leader_only_owns_every_volume_and_selects_the_observed_leader(self):
        from io_stall_majority import MajorityCounterWorkload
        binary = self.root / 'majority-binary'
        binary.write_bytes(b'fixture')
        for defect in [None, 'second_create', 'first_close', 'first_close_diagnostic',
                       'counter_close', 'second_resume']:
            with self.subTest(defect=defect):
                order, volumes = [], []
                for node in (1, 2, 3):
                    volume = Mock(root=self.root / f'volume-{node}', image=self.root / f'image-{node}',
                                  uuid=f'uuid-{node}', sectors=262144, loop=f'/dev/loop{node}')
                    volume.name = f'mapping-{node}'
                    volume.create.side_effect = lambda n=node: order.append(f'create-{n}')
                    volume.resume.side_effect = lambda n=node: order.append(f'resume-{n}')
                    volume.close.side_effect = lambda n=node: order.append(f'close-{n}')
                    volumes.append(volume)
                counter = Mock(spec=MajorityCounterWorkload)
                counter.leader = 2
                counter.close.side_effect = lambda: order.append('counter-close')
                if defect == 'second_create':
                    volumes[1].create.side_effect = RuntimeError('partial create')
                elif defect in ('first_close', 'first_close_diagnostic'):
                    volumes[0].close.side_effect = RuntimeError('first close failed')
                elif defect == 'counter_close':
                    counter.close.side_effect = RuntimeError('unreaped counter')
                elif defect == 'second_resume':
                    volumes[1].resume.side_effect = RuntimeError('resume failed')
                write = Path.write_text
                def write_artifact(path, *args, **kwargs):
                    if defect == 'first_close_diagnostic' and path.name == 'cleanup-error-1.txt':
                        raise OSError('diagnostic store unavailable')
                    return write(path, *args, **kwargs)
                with patch.object(Path, 'write_text', write_artifact), \
                        patch.object(runner, 'Commands'), \
                        patch.object(runner, 'DisposableVolume', side_effect=volumes), \
                        patch.object(runner, 'run_probe', return_value=['modeled']) as probe, \
                        patch.object(runner, 'source_snapshot', return_value=self.snapshot), \
                        patch.object(runner.tempfile, 'mkdtemp', return_value=str(self.root)), \
                        patch('io_stall_majority.MajorityCounterWorkload', return_value=counter):
                    receipt = runner.qualify(self.root / f'majority-{defect}', binary, leader_only=True)
                self.assertEqual(receipt['status'], 'passed' if defect is None else 'failed')
                if defect == 'second_create':
                    probe.assert_not_called()
                    counter.close.assert_not_called()
                    volumes[0].close.assert_called_once()
                    volumes[1].close.assert_called_once()
                    volumes[2].close.assert_not_called()
                    self.assertEqual(len(receipt['volumes']), 2)
                    continue
                self.assertIs(probe.call_args.args[0], volumes[1])
                self.assertEqual(receipt['fault_node'], 2)
                self.assertEqual(receipt['volume']['uuid'], 'uuid-2')
                self.assertEqual(len(receipt['volumes']), 3)
                for volume in volumes:
                    volume.resume.assert_called_once()
                    if defect == 'counter_close':
                        volume.close.assert_not_called()
                    else:
                        volume.close.assert_called_once()
                if defect != 'counter_close':
                    self.assertLess(order.index('resume-3'), order.index('counter-close'))
                    self.assertLess(order.index('counter-close'), order.index('close-3'))
                if defect in ('first_close', 'first_close_diagnostic'):
                    self.assertEqual(receipt['volume_cleanup'], 'failed')
                    self.assertEqual(receipt['volumes'][2]['cleanup'], 'passed')
                if defect == 'first_close_diagnostic':
                    self.assertTrue(any('diagnostic store unavailable' in error for error in receipt['errors']))

    def test_leader_only_arguments_refuse_before_creating_evidence(self):
        for arguments in [dict(leader_only=True), dict(counter_binary=self.root / 'missing',
                                                     leader_only=True, crash_while_stalled=True)]:
            destination = self.root / 'must-not-create'
            with self.assertRaises(ValueError):
                runner.qualify(destination, **arguments)
            self.assertFalse(destination.exists())

    def test_vm_selection_required_before_side_effects(self):
        artifacts = self.root / 'must-not-exist'
        result = subprocess.run([sys.executable, str(SCRIPTS / 'check-io-stall'),
                                 '--artifacts', str(artifacts)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('disposable Linux VM', result.stderr)
        self.assertFalse(artifacts.exists())


if __name__ == '__main__':
    unittest.main()
