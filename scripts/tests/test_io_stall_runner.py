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

    def test_vm_selection_required_before_side_effects(self):
        artifacts = self.root / 'must-not-exist'
        result = subprocess.run([sys.executable, str(SCRIPTS / 'check-io-stall'),
                                 '--artifacts', str(artifacts)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('disposable Linux VM', result.stderr)
        self.assertFalse(artifacts.exists())


if __name__ == '__main__':
    unittest.main()
