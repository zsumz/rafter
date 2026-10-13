"""Resource receipts reject missing/stale observations and retain raw inputs."""
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from io_stall_resources import parse_stat, parse_status, sample_children, sample_process

STAT = '123 (worker ) x) R 1 1 1 0 -1 4194304 10 0 0 0 123 45 0 0 20 0 4 0 678 1048576 20\n'
STATUS = 'Name:\tworker\nPid:\t123\nVmRSS:\t80 kB\nVmHWM:\t96 kB\nVmSwap:\t4 kB\nThreads:\t4\n'


class ResourceReceipts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='rafter-proc-controls-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.proc = self.root / '123'
        (self.proc / 'fd').mkdir(parents=True)
        for name in ('0', '1', '2', '9'):
            (self.proc / 'fd' / name).touch()
        (self.proc / 'stat').write_text(STAT)
        (self.proc / 'status').write_text(STATUS)
        self.child = Mock(pid=123)
        self.child.poll.return_value = None

    def sample(self):
        return sample_process(self.child, self.root)

    def test_units_identity_cpu_and_actual_descriptor_count(self):
        with patch('io_stall_resources.os.sysconf', return_value=100):
            result = self.sample()
        for key, value in dict(pid=123, start_ticks=678, user_ticks=123, system_ticks=45,
                               rss_bytes=81920, rss_high_water_bytes=98304,
                               swap_bytes=4096, threads=4, descriptors=4,
                               ticks_per_second=100).items():
            self.assertEqual(result[key], value)
        self.assertLessEqual(result['started_ns'], result['finished_ns'])
        self.assertEqual(result['raw']['stat_before'], STAT)
        self.assertEqual(result['raw']['stat_after'], STAT)
        self.assertEqual(result['raw']['status'], STATUS)
        self.assertEqual(result['raw']['descriptor_names'], ['0', '1', '2', '9'])

    def test_stat_comm_parentheses_do_not_shift_counter_positions(self):
        self.assertEqual(parse_stat(STAT, 123),
                         dict(start_ticks=678, user_ticks=123, system_ticks=45))
        for raw in [STAT.replace('123 (', '124 (', 1), '123 (name) R 1',
                    STAT.replace(' 678 ', ' -1 '), STAT.replace(' 45 ', ' nan '),
                    STAT.replace(' 678 ', ' ６７８ ')]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_stat(raw, 123)

    def test_missing_duplicate_units_and_negative_counters_refuse(self):
        for raw in [STATUS.replace('VmRSS:\t80 kB\n', ''), STATUS + 'VmRSS: 1 kB\n',
                    STATUS.replace('80 kB', '80 MB'), STATUS.replace('80 kB', '-1 kB'),
                    STATUS.replace('80 kB', '80'), STATUS.replace('Pid:\t123', 'Pid:\t124'),
                    STATUS.replace('Threads:\t4', 'Threads:\t0'),
                    STATUS.replace('Threads:\t4', 'Threads:\t4 kB')]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_status(raw, 123)

    def test_exited_or_replaced_child_never_produces_successful_sample(self):
        self.child.poll.return_value = 0
        with self.assertRaisesRegex(RuntimeError, 'live owned child'):
            self.sample()
        self.child.poll.side_effect = [None, 0]
        with self.assertRaisesRegex(RuntimeError, 'exited or changed'):
            self.sample()
        self.child.poll.side_effect = None
        self.child.poll.return_value = None
        with patch.object(Path, 'read_text', side_effect=[STAT, STATUS, STAT.replace(' 678 ', ' 679 ')]):
            with self.assertRaisesRegex(RuntimeError, 'exited or changed'):
                self.sample()

    def test_missing_proc_inputs_fail_instead_of_reporting_zero(self):
        (self.proc / 'status').unlink()
        with self.assertRaises(FileNotFoundError):
            self.sample()

    def test_unreadable_proc_inputs_fail(self):
        with patch.object(Path, 'read_text', side_effect=PermissionError('denied')):
            with self.assertRaises(PermissionError):
                self.sample()

    def test_bad_descriptor_or_tick_rate_refuses(self):
        with patch('io_stall_resources.os.sysconf', return_value=0):
            with self.assertRaisesRegex(RuntimeError, 'clock tick'):
                self.sample()
        (self.proc / 'fd' / 'unexpected').touch()
        with self.assertRaisesRegex(ValueError, 'descriptor'):
            self.sample()

    def test_all_three_owned_nodes_are_required(self):
        for nodes in [{}, {1: self.child}, {1: self.child, 2: self.child, 4: self.child}]:
            with self.assertRaisesRegex(RuntimeError, 'all three'):
                sample_children(nodes)
        with self.assertRaisesRegex(RuntimeError, 'distinct owned processes'):
            sample_children({node: self.child for node in (1, 2, 3)})
        children = {node: Mock(pid=100 + node) for node in (3, 1, 2)}
        with patch('io_stall_resources.sample_process', side_effect=lambda child: child.pid) as sample:
            self.assertEqual(sample_children(children), {1: 101, 2: 102, 3: 103})
            self.assertEqual([c.args[0].pid for c in sample.call_args_list], [101, 102, 103])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'native procfs requires Linux')
    def test_live_owned_child_has_real_resource_receipt(self):
        child = subprocess.Popen([sys.executable, '-u', '-c',
            'import sys; payload=bytearray(16*1024*1024); payload[::4096]=b"x"*4096; '
            'print("ready"); sys.stdin.read(1)'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertTrue(select.select([child.stdout], [], [], 10)[0], 'child did not initialize')
            self.assertEqual(child.stdout.readline(), b'ready\n')
            sample = sample_process(child)
            self.assertGreaterEqual(sample['rss_bytes'], 16 * 1024 * 1024)
            self.assertGreaterEqual(sample['descriptors'], 3)
            self.assertGreaterEqual(sample['threads'], 1)
            child.communicate(b'x', timeout=10)
            self.assertEqual(child.returncode, 0)
            with self.assertRaises(RuntimeError):
                sample_process(child)
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)


if __name__ == '__main__':
    unittest.main()
