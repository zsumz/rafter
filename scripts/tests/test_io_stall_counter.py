"""Workload oracles reject false success, unbounded uncertainty and lost children."""
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from io_stall_counter import CounterWorkload


class CounterOracle(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rafter-stall-oracle-')
        self.addCleanup(self.temporary.cleanup)
        self.counter = CounterWorkload.__new__(CounterWorkload)
        self.counter.root = self.counter.artifacts = Path(self.temporary.name)
        self.counter.events = []
        self.counter.leader = 1
        self.counter.baseline = {'applied': 2}
        self.counter.unknown = 0
        self.counter.cluster = Mock()
        self.cluster = self.counter.cluster
        self.cluster.children = {}
        self.cluster.logs = []

    def held_responses(self):
        self.counter.refused = Mock(side_effect=[
            *[({'write_may_have_committed': True, 'error': 'timed out'}, 5) for _ in range(8)],
            ({'write_may_have_committed': False, 'error': 'write admission is full'}, .01),
            ({'error': 'timed out'}, 5)])
        self.cluster.request.return_value = {'applied': 2, 'pending_writes': 0}

    def test_stale_metrics_do_not_masquerade_as_admission_counts(self):
        self.held_responses()
        self.counter.while_stalled()
        self.assertEqual(self.counter.events[-1]['event'], 'held')
        self.assertEqual(self.counter.refused.call_count, 10)
        self.assertEqual(self.cluster.request.call_count, 9)
        self.assertEqual(self.counter.unknown, 8)

    def test_queued_cancellations_may_release_unsubmitted_capacity(self):
        self.held_responses()
        self.counter.refused.side_effect = [
            *[({'write_may_have_committed': True, 'error': 'timed out'}, 5) for _ in range(9)],
            ({'error': 'timed out'}, 5)]
        self.counter.while_stalled()
        self.assertEqual(self.counter.unknown, 9)
        self.assertEqual(self.counter.events[-1]['not_appended'], 0)

    def test_missing_stall_acknowledgement_rejected(self):
        self.cluster.request.return_value = {'value': 6}
        with self.assertRaisesRegex(AssertionError, 'unexpectedly acknowledged'):
            self.counter.while_stalled()

    def test_only_a_typed_http_503_can_count_as_refusal(self):
        self.cluster.request.side_effect = urllib.error.HTTPError(
            'http://local', 503, 'refused', {}, io.BytesIO(b'{"error":"timed out"}'))
        body, _ = self.counter.refused('/value', 'GET')
        self.assertEqual(body['error'], 'timed out')
        self.cluster.request.side_effect = TimeoutError('HTTP server wedged')
        with self.assertRaises(TimeoutError):
            self.counter.refused('/value', 'GET')

    def test_premature_application_progress_rejected(self):
        self.held_responses()
        self.cluster.request.return_value = {'applied': 3}
        with self.assertRaises(AssertionError):
            self.counter.while_stalled()

    def test_wrong_or_slow_full_queue_refusal_rejected(self):
        for error, elapsed in [({'write_may_have_committed': True, 'error': 'write admission is full'}, .1),
                               ({'write_may_have_committed': False, 'error': 'not leader'}, .1),
                               ({'write_may_have_committed': False, 'error': 'write admission is full'}, 3)]:
            with self.subTest(error=error, elapsed=elapsed):
                self.counter.unknown = 0
                self.held_responses()
                self.counter.refused.side_effect = [
                    *[({'write_may_have_committed': True, 'error': 'timed out'}, 5) for _ in range(8)],
                    (error, elapsed)]
                with self.assertRaises(AssertionError):
                    self.counter.while_stalled()

    def test_bounded_unknown_writes_and_cold_recovery(self):
        for unknown in [0, 3, 8]:
            with self.subTest(unknown=unknown):
                self.counter.unknown = 8
                final = 105 + unknown
                self.cluster.request.side_effect = [
                    {'value': 5 + unknown}, {'value': final},
                    {'value': final}, {'value': final + 1}]
                self.counter.finish()
                self.assertEqual(self.counter.events[-1]['preserved'], final)
                self.assertEqual(self.counter.events[-1]['event'], 'cold_recovery')

    def test_lost_baseline_extra_writes_and_wrong_recovery_fail(self):
        for values in [[4], [14], [5, 104], [5, 114], [5, 105, 104], [5, 105, 105, 107]]:
            with self.subTest(values=values):
                self.counter.unknown = 8
                self.cluster.request.side_effect = [{'value': value} for value in values]
                with self.assertRaises(AssertionError):
                    self.counter.finish()

    def test_cleanup_attempts_every_child_and_retains_unreaped_ownership(self):
        children = {node: Mock(pid=100 + node, returncode=None) for node in [1, 2, 3]}
        self.cluster.children = dict(children)
        for child in children.values():
            child.poll.return_value = None
        children[1].wait.side_effect = subprocess.TimeoutExpired('counter', 15)
        with self.assertRaisesRegex(RuntimeError, 'reap node 1'):
            self.counter.close()
        self.assertEqual(set(self.cluster.children), {1})
        for child in children.values():
            child.kill.assert_called_once()
            child.wait.assert_called_once()
        self.assertEqual(self.counter.events[-1]['status'], 'failed')
        retained = json.loads((self.counter.artifacts / 'counter-events.json').read_text())
        self.assertEqual(len(retained[-1]['processes']), 3)


if __name__ == '__main__':
    unittest.main()
