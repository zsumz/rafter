"""Refuse ambiguous filesystem evidence and preserve failed capture diagnostics."""
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

loader = importlib.machinery.SourceFileLoader('storage_environment', str(Path(__file__).resolve().parents[1] / 'record-storage-environment'))
spec = importlib.util.spec_from_loader(loader.name, loader)
capture = importlib.util.module_from_spec(spec)
loader.exec_module(capture)


class StorageEnvironment(unittest.TestCase):
    def setUp(self):
        self.device = os.makedev(8, 3)
        self.stat = Mock(return_value=SimpleNamespace(st_dev=self.device))
        self.linux = {'target': '/mnt/test data', 'source': '/dev/test', 'fstype': 'ext4',
                      'options': 'rw,relatime', 'maj:min': f'{os.major(self.device)}:{os.minor(self.device)}'}
        self.df = 'Filesystem 512-blocks Used Available Capacity Mounted on\n/dev/disk3s5 100 30 70 30% /System/Volumes/Data\n'
        self.mounts = '/dev/disk3s1 on / (apfs, local, read-only)\n/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled)\n'

    def linux_result(self, rows):
        return capture.linux_filesystem(json.dumps({'filesystems': rows}), self.device, self.stat)

    def test_linux_mount_keeps_spaces_and_native_options(self):
        result = self.linux_result([self.linux])
        self.assertEqual(result['mountpoint'], '/mnt/test data')
        self.assertEqual(result['options'], ['rw', 'relatime'])
        self.stat.assert_called_once_with('/mnt/test data')

    def test_multiple_or_missing_linux_mounts_are_not_guessed(self):
        for rows in ([], [self.linux, self.linux]):
            with self.assertRaisesRegex(ValueError, 'one containing'):
                self.linux_result(rows)

    def test_linux_requires_complete_fields_and_exact_device(self):
        for key in self.linux:
            row = dict(self.linux)
            del row[key]
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                self.linux_result([row])
        with self.assertRaisesRegex(ValueError, 'does not match'):
            self.linux_result([dict(self.linux, **{'maj:min': '8:4'})])

    def test_linux_rejects_ambiguous_mount_tree_and_device_change(self):
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            self.linux_result([dict(self.linux, children=[self.linux])])
        self.stat.return_value.st_dev += 1
        with self.assertRaisesRegex(ValueError, 'changed'):
            self.linux_result([self.linux])

    def test_macos_uses_df_device_for_apfs_firmlink_volume(self):
        result = capture.darwin_filesystem(self.df, self.mounts, self.device, self.stat)
        self.assertEqual(result, {'source': '/dev/disk3s5', 'mountpoint': '/System/Volumes/Data',
                                  'filesystem': 'apfs', 'options': ['local', 'journaled']})
        self.stat.assert_called_once_with('/System/Volumes/Data')

    def test_macos_mountpoint_spaces_are_preserved(self):
        result = capture.darwin_filesystem(self.df.replace('/System/Volumes/Data', '/Volumes/Test Data'),
            self.mounts.replace('/System/Volumes/Data', '/Volumes/Test Data'), self.device, self.stat)
        self.assertEqual(result['mountpoint'], '/Volumes/Test Data')

    def test_macos_missing_duplicate_or_foreign_mount_is_rejected(self):
        for mounts in ('', self.mounts + self.mounts, self.mounts.replace('disk3s5', 'disk4s5')):
            with self.assertRaisesRegex(ValueError, 'unique matching'):
                capture.darwin_filesystem(self.df, mounts, self.device, self.stat)
        self.stat.return_value.st_dev += 1
        with self.assertRaisesRegex(ValueError, 'does not match'):
            capture.darwin_filesystem(self.df, self.mounts, self.device, self.stat)

    def test_malformed_df_rows_are_rejected(self):
        for df in ('header\n', self.df + 'another volume\n', 'header\nnot a volume\n'):
            with self.assertRaises(ValueError):
                capture.darwin_filesystem(df, self.mounts, self.device, self.stat)

    def test_failed_capture_retains_error_and_partial_observations(self):
        def fail(report, paths):
            report['commands'].append({'partial': True})
            raise OSError('fixture query failed')
        with tempfile.TemporaryDirectory() as temporary, patch.object(capture, 'collect', side_effect=fail):
            output = Path(temporary) / 'receipt.json'
            with self.assertRaises(OSError):
                capture.capture(output, [])
            report = json.loads(output.read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['commands'], [{'partial': True}])
            self.assertIn('fixture query failed', report['error'])

    def test_existing_evidence_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(capture, 'collect') as collect:
            output = Path(temporary) / 'receipt.json'
            output.write_text('original')
            with self.assertRaises(FileExistsError):
                capture.capture(output, [])
            collect.assert_not_called()
            self.assertEqual(output.read_text(), 'original')


if __name__ == '__main__':
    unittest.main()
