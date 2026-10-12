"""Exercise the existing TLS counter while all three data directories are stalled."""
import importlib.util
import json
from pathlib import Path
import subprocess
import time
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / 'docs/examples/counter'
spec = importlib.util.spec_from_file_location('stall_counter_smoke', PROJECT / 'smoke.py')
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class CounterWorkload:
    def __init__(self, binary, root, mount, artifacts):
        self.root, self.mount, self.artifacts = root, mount, artifacts
        self.cluster = smoke.Cluster(binary, root)
        self.events = []
        self.leader = None
        self.baseline = None
        self.unknown = 0

    def record(self, event, **fields):
        self.events.append(dict(event=event, monotonic_ns=time.monotonic_ns(), **fields))
        (self.artifacts / 'counter-events.json').write_text(json.dumps(self.events, indent=2) + '\n')

    def start(self):
        self.root.mkdir()
        with (self.root / 'certificates.log').open('w') as log:
            subprocess.run(['sh', str(PROJECT / 'certs.sh')], cwd=self.root,
                           stdout=log, stderr=log, check=True, timeout=60)
        result = subprocess.run([str(self.cluster.binary), 'init'], cwd=self.mount,
                                env=self.cluster.env, capture_output=True, text=True, timeout=15)
        (self.artifacts / 'counter-init.stdout').write_text(result.stdout)
        (self.artifacts / 'counter-init.stderr').write_text(result.stderr)
        result.check_returncode()
        (self.root / 'data').symlink_to(self.mount / 'data', target_is_directory=True)
        for node in (1, 2, 3):
            self.cluster.start(node)
        self.leader = self.cluster.leader()
        assert self.cluster.request(self.leader, '/add/5', 'POST') == {'value': 5}
        self.cluster.converged(5)
        self.baseline = self.cluster.request(self.leader, '/metrics')
        self.record('baseline', leader=self.leader, value=5, metrics=self.baseline,
                    processes={node: child.pid for node, child in self.cluster.children.items()})

    def refused(self, path, method='POST'):
        started = time.monotonic()
        try:
            result = self.cluster.request(self.leader, path, method, timeout=8)
        except urllib.error.HTTPError as error:
            assert error.code == 503, error
            body = json.load(error)
            elapsed = time.monotonic() - started
            self.record('refused', path=path, response=body, elapsed_seconds=elapsed)
            return body, elapsed
        raise AssertionError(f'operation unexpectedly acknowledged during stall: {path}: {result}')

    def while_stalled(self):
        refused_before_append = 0
        for attempt in range(1, 10):
            error, elapsed = self.refused('/add/1')
            if error['write_may_have_committed']:
                assert 'timed out' in error['error'], error
                self.unknown += 1
            else:
                assert 'write admission is full' in error['error'], error
                assert elapsed < 2, 'full write admission did not refuse promptly'
                refused_before_append += 1
            metrics = self.cluster.request(self.leader, '/metrics')
            assert metrics['applied'] == self.baseline['applied'], metrics
            # Queued cancellations can release credits before append. Metrics
            # are last-published observations, not current admission counters.
            self.record('pending', attempts=attempt, uncertain=self.unknown,
                        not_appended=refused_before_append, last_published_metrics=metrics)
        assert self.unknown > 0, 'no write reached the uncertain timeout path'
        error, _ = self.refused('/value', 'GET')
        assert 'timed out' in error['error'], error
        self.record('held', timed_out_writes=self.unknown, not_appended=refused_before_append)

    def finish(self):
        leader = self.cluster.leader()
        value = self.cluster.request(leader, '/value')['value']
        assert 5 <= value <= 5 + self.unknown, value
        # Never retry uncertain increments. Bound their aggregate result instead.
        acknowledged = self.cluster.request(leader, '/add/100', 'POST')['value']
        assert 105 <= acknowledged <= 105 + self.unknown, acknowledged
        self.cluster.converged(acknowledged)
        self.cluster.wait('owner queues drain after resumed I/O', lambda:
            all(self.cluster.request(node, '/metrics')['pending_writes'] == 0
                for node in self.cluster.children))
        self.record('resumed', value_before_new_write=value, acknowledged=acknowledged)
        for node in list(self.cluster.children):
            self.cluster.stop(node, graceful=True)
        for node in (1, 2, 3):
            self.cluster.start(node)
        leader = self.cluster.leader()
        assert self.cluster.request(leader, '/value') == {'value': acknowledged}
        assert self.cluster.request(leader, '/add/1', 'POST') == {'value': acknowledged + 1}
        self.cluster.converged(acknowledged + 1)
        for node in list(self.cluster.children):
            self.cluster.stop(node, graceful=True)
        self.record('cold_recovery', preserved=acknowledged, next_write=acknowledged + 1)

    def close(self):
        errors = []
        children = list(self.cluster.children.items())
        # Attempt every child even if one cannot be reaped. Caller resumes first.
        for node, child in children:
            try:
                if child.poll() is None:
                    child.kill()
            except BaseException as error:
                errors.append(f'kill node {node}: {error!r}')
        for node, child in children:
            try:
                child.wait(timeout=15)
                del self.cluster.children[node]
            except BaseException as error:
                errors.append(f'reap node {node}: {error!r}')
        for log in self.cluster.logs:
            log.close()
        for path in self.root.glob('*.log'):
            (self.artifacts / ('counter-' + path.name)).write_bytes(path.read_bytes())
        self.record('cleanup', errors=errors,
                    status='failed' if errors else 'passed',
                    processes=[dict(node=n, pid=p.pid, returncode=p.returncode) for n, p in children])
        if errors:
            raise RuntimeError('; '.join(errors))
