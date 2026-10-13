"""Healthy-majority progress must precede resume and survive recovery."""
from pathlib import Path
import sys
import tempfile
import unittest
import urllib.error
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from io_stall_counter import CounterWorkload
from io_stall_majority import MajorityCounterWorkload


class MajorityOracle(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rafter-majority-oracle-')
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.counter = MajorityCounterWorkload.__new__(MajorityCounterWorkload)
        self.counter.root = self.counter.artifacts = root
        self.counter.events = []
        self.counter.resource_sample = Mock(return_value={"portable": "fixture"})
        self.counter.leader = 1
        self.counter.baseline = dict(term=6, applied=2)
        self.counter.unknown = 1
        self.counter.held_acknowledged = 105
        self.counter.cluster = self.cluster = Mock()
        self.cluster.children = {node: Mock() for node in (1, 2, 3)}
        self.cluster.wait.return_value = {node: dict(retained_writes=0, retained_reads=0)
                                          for node in self.cluster.children}

    def view(self, role='Leader', term=7):
        return dict(role=role, term=term, ready=True)

    def test_higher_term_and_quorum_read_only_from_healthy_replicas(self):
        self.cluster.request.side_effect = [self.view(), self.view('Follower'), {'value': 5}]
        self.assertEqual(self.counter.healthy_leader(), dict(node=2, term=7, value=5))
        self.assertEqual([call.args for call in self.cluster.request.call_args_list],
                         [(2, '/metrics'), (3, '/metrics'), (2, '/value')])
        self.cluster.request.reset_mock()
        self.cluster.request.side_effect = [self.view(), self.view('Follower'),
                                            urllib.error.URLError('quorum read refused')]
        with self.assertRaises(urllib.error.URLError):
            self.counter.healthy_leader()

    def test_old_term_ambiguous_leaders_and_impossible_values_refuse(self):
        for views in [[self.view(term=6), self.view('Follower')],
                      [self.view(), self.view()]]:
            self.cluster.request.side_effect = views
            self.assertIsNone(self.counter.healthy_leader())
        for value in (4, 7):
            self.cluster.request.side_effect = [self.view(), self.view('Follower'), {'value': value}]
            with self.assertRaises(AssertionError):
                self.counter.healthy_leader()

    def held(self):
        self.counter.refused = Mock(return_value=({'write_may_have_committed': True,
                                                   'error': 'timed out'}, 5))
        self.cluster.wait.return_value = dict(node=2, term=7, value=5)
        self.cluster.request.side_effect = [{'value': 105}, {'value': 105},
            dict(applied=2, retained_writes=1, retained_write_bytes=8)]

    def test_fresh_write_and_quorum_read_complete_during_hold(self):
        self.held()
        self.counter.while_stalled()
        self.assertEqual(self.counter.held_acknowledged, 105)
        self.assertEqual(self.counter.events[-1]['event'], 'majority_acknowledged_while_held')
        self.assertEqual(self.counter.events[-1]['uncertain_writes'], 1)
        self.counter.refused.assert_called_once_with('/add/1')

    def test_missing_acknowledgement_or_old_leader_progress_fails(self):
        for responses in [[{'value': 104}], [{'value': 107}], [{'value': 105}, {'value': 104}],
                          [{'value': 105}, {'value': 105}, dict(applied=3)],
                          [{'value': 105}, {'value': 105},
                           dict(applied=2, retained_writes=9, retained_write_bytes=8)]]:
            with self.subTest(responses=responses):
                self.held()
                self.cluster.request.side_effect = responses
                with self.assertRaises(AssertionError):
                    self.counter.while_stalled()

    def test_majority_acknowledgement_survives_resume_and_cold_recovery(self):
        self.cluster.request.side_effect = [{'value': 105}, {'value': 106},
                                            {'value': 106}, {'value': 107}]
        self.counter.finish()
        self.assertEqual(self.counter.events[-1]['preserved'], 106)
        self.assertEqual(self.counter.events[-1]['next_write'], 107)

    def test_lost_ack_extra_increment_and_cold_loss_fail(self):
        for values in [[104], [107], [105, 107], [105, 106, 105], [105, 106, 106, 108]]:
            with self.subTest(values=values):
                self.cluster.request.side_effect = [{'value': value} for value in values]
                with self.assertRaises(AssertionError):
                    self.counter.finish()

    def test_different_directories_on_one_filesystem_are_not_isolation(self):
        self.counter.mounts = {node: self.counter.root / f'mount-{node}' for node in (1, 2, 3)}
        self.counter.mount = self.counter.mounts[1]
        for node, mount in self.counter.mounts.items():
            (mount / 'data' / f'node-{node}').mkdir(parents=True)
        self.counter.initialize = Mock()
        with patch.object(CounterWorkload, 'prepare_data', side_effect=lambda:
                          (self.counter.root / 'data').symlink_to(self.counter.mount / 'data')):
            with self.assertRaisesRegex(AssertionError, 'share a filesystem'):
                self.counter.prepare_data()
        self.cluster.start.assert_not_called()


if __name__ == '__main__':
    unittest.main()
