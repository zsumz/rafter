"""Resource acquisition and cleanup refuse foreign identities and ambiguous success."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from io_stall_volume import DisposableVolume
from scripts.tests.io_stall_fixture import FakeHost


class VolumeOwnership(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rafter-volume-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.platform = patch('io_stall_volume.sys.platform', 'linux')
        self.platform.start()
        self.addCleanup(self.platform.stop)

    def volume(self, name='volume'):
        host = FakeHost()
        return host, DisposableVolume(self.root / name, host)

    def test_cleanup_order_and_idempotence(self):
        host, volume = self.volume()
        volume.create()
        volume.suspend()
        self.assertTrue(host.suspended)
        volume.close()
        self.assertTrue(host.empty())
        cleanup = [c[0] if c[0] == 'umount' else c[1] for c in host.commands
                   if c[0] == 'umount' or c[:2] in [
                       ['dmsetup', 'resume'], ['dmsetup', 'remove'], ['losetup', '--detach']]]
        self.assertEqual(cleanup, ['resume', 'umount', 'remove', '--detach'])
        count = len(host.commands)
        volume.close()
        self.assertEqual(len(host.commands), count)

    def test_partial_setup_before_and_after_effect(self):
        for command in [('mkfs.ext4', '-q'), ('losetup', '--find'),
                        ('dmsetup', 'create'), ('mount', '-t')]:
            for late in [False, True]:
                with self.subTest(command=command, after_effect=late):
                    host, volume = self.volume(f'{command[0]}-{late}')
                    host.failure, host.after_effect = command, late
                    with self.assertRaises(subprocess.SubprocessError):
                        volume.create()
                    self.assertTrue(host.empty())

    def test_timed_out_suspend_is_resumed(self):
        host, volume = self.volume()
        volume.create()
        host.failure, host.after_effect = ('dmsetup', 'suspend'), True
        with self.assertRaises(subprocess.TimeoutExpired):
            volume.suspend()
        self.assertTrue(host.suspended)
        volume.close()
        self.assertTrue(host.empty())
        self.assertFalse(host.suspended)

    def test_foreign_identity_never_mutated(self):
        for defect in ['uuid', 'table', 'loop', 'mount']:
            with self.subTest(defect=defect):
                host, volume = self.volume(defect)
                volume.create()
                if defect in ['uuid', 'table']:
                    saved = host.mapping[defect]
                    host.mapping[defect] = 'foreign'
                elif defect == 'loop':
                    saved, host.loop = host.loop, '/foreign/backing.img'
                else:
                    host.foreign_mount = True
                start = len(host.commands)
                with self.assertRaises(RuntimeError):
                    volume.suspend()
                with self.assertRaises(RuntimeError):
                    volume.close()
                self.assertFalse(any(c[0] == 'umount' or c[:2] in [
                    ['dmsetup', 'suspend'], ['dmsetup', 'resume'], ['dmsetup', 'remove'],
                    ['losetup', '--detach']] for c in host.commands[start:]))
                if defect in ['uuid', 'table']:
                    host.mapping[defect] = saved
                elif defect == 'loop':
                    host.loop = saved
                else:
                    host.foreign_mount = False
                volume.close()
                self.assertTrue(host.empty())

    def test_failed_cleanup_keeps_lower_resources(self):
        for command in [('dmsetup', 'resume'), ('umount', None), ('dmsetup', 'remove')]:
            with self.subTest(command=command):
                host, volume = self.volume(command[1] or command[0])
                volume.create()
                volume.suspend()
                host.failure = (command[0], str(volume.mount)) if command[0] == 'umount' else command
                start = len(host.commands)
                with self.assertRaises(subprocess.SubprocessError):
                    volume.close()
                self.assertFalse(any(c[:2] == ['losetup', '--detach']
                                     for c in host.commands[start:]))
                volume.close()
                self.assertTrue(host.empty())

    def test_successful_command_without_release_is_refused(self):
        for operation in ['umount', 'remove', '--detach']:
            with self.subTest(operation=operation):
                host, volume = self.volume(operation)
                volume.create()
                def no_effect(args, check=True):
                    command = args[2:] if args[:2] == ['sudo', '-n'] else args
                    if command[0] == operation or command[:2] in [
                            ['dmsetup', operation], ['losetup', operation]]:
                        return subprocess.CompletedProcess(args, 0, stdout='', stderr='')
                    return host(args, check)
                volume.run = no_effect
                with self.assertRaisesRegex(RuntimeError, 'still exists|remains associated'):
                    volume.close()
                self.assertFalse(host.empty())
                volume.run = host
                volume.close()
                self.assertTrue(host.empty())

    def test_existing_directory_refused(self):
        host, volume = self.volume()
        volume.root.mkdir()
        marker = volume.root / 'preserve'
        marker.write_text('retained')
        with self.assertRaises(FileExistsError):
            volume.create()
        self.assertFalse(host.commands)
        self.assertEqual(marker.read_text(), 'retained')

    def test_unsupported_platform_has_no_effect(self):
        with patch('io_stall_volume.sys.platform', 'darwin'):
            host, volume = self.volume()
            with self.assertRaises(RuntimeError):
                volume.create()
            self.assertFalse(host.commands)
            self.assertFalse(volume.root.exists())


if __name__ == '__main__':
    unittest.main()
