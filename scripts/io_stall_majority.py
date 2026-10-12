"""Require a healthy majority to progress while only the old leader's I/O is held."""
from io_stall_counter import CounterWorkload


class MajorityCounterWorkload(CounterWorkload):
    def __init__(self, binary, root, mounts, artifacts):
        if set(mounts) != {1, 2, 3} or len(set(mounts.values())) != 3:
            raise ValueError('three distinct replica mounts are required')
        super().__init__(binary, root, mounts[1], artifacts)
        self.mounts = mounts
        self.held_acknowledged = None

    def prepare_data(self):
        super().prepare_data()
        # Every init writes only a fresh private filesystem. The other bootstrap
        # directories remain unopened; each live replica uses its own mount.
        for node in (2, 3):
            self.initialize(self.mounts[node], f'counter-init-{node}')
        data = self.root / 'data'
        data.unlink()  # The symlink just created by the base preparation.
        data.mkdir()
        layout = {}
        for node, mount in self.mounts.items():
            target = mount / 'data' / f'node-{node}'
            (data / f'node-{node}').symlink_to(target, target_is_directory=True)
            device = target.stat().st_dev
            assert device == mount.stat().st_dev, 'replica data escaped its mount'
            layout[node] = dict(path=str(target), device=device)
        assert len({entry['device'] for entry in layout.values()}) == 3, 'replicas share a filesystem'
        self.record('replica_filesystems', nodes=layout)

    def healthy_leader(self):
        # The blocked owner may still publish its old Leader role. Query only
        # the two healthy replicas; a successful quorum read proves readiness.
        peers = [node for node in self.cluster.children if node != self.leader]
        assert len(peers) == 2, 'healthy majority requires both remaining replicas'
        views = {node: self.cluster.request(node, '/metrics') for node in peers}
        leaders = [node for node, view in views.items() if view['role'] == 'Leader'
                   and view['term'] > self.baseline['term'] and view['ready']]
        if len(leaders) != 1:
            return None
        leader = leaders[0]
        value = self.cluster.request(leader, '/value')['value']
        assert 5 <= value <= 5 + self.unknown, value
        return dict(node=leader, term=views[leader]['term'], value=value)

    def while_stalled(self):
        # This write forces the old leader to persist on the suspended mapping.
        # Its timeout is uncertain; never retry the increment.
        error, _ = self.refused('/add/1')
        assert error['write_may_have_committed'] and 'timed out' in error['error'], error
        self.unknown = 1
        elected = self.cluster.wait('healthy majority elects a higher-term leader', self.healthy_leader)
        leader = elected['node']
        acknowledged = self.cluster.request(leader, '/add/100', 'POST')['value']
        assert elected['value'] + 100 <= acknowledged <= 105 + self.unknown, acknowledged
        assert self.cluster.request(leader, '/value') == {'value': acknowledged}
        self.held_acknowledged = acknowledged
        blocked = self.cluster.request(self.leader, '/metrics')
        assert blocked['applied'] == self.baseline['applied'], blocked
        assert 0 <= blocked['retained_writes'] <= 8 and 0 <= blocked['retained_write_bytes'] <= 64, blocked
        self.record('majority_acknowledged_while_held', old_leader=self.leader,
                    elected=elected, acknowledged=acknowledged, blocked_metrics=blocked,
                    uncertain_writes=self.unknown)

    def finish(self):
        leader = self.cluster.leader()
        value = self.cluster.request(leader, '/value')['value']
        assert self.held_acknowledged <= value <= 105 + self.unknown, value
        self.cluster.converged(value)
        drained = self.cluster.wait('all replica credits drain after resume', self.drained_metrics)
        acknowledged = self.cluster.request(leader, '/add/1', 'POST')['value']
        assert acknowledged == value + 1, acknowledged
        self.cluster.converged(acknowledged)
        self.record('majority_resumed', value_after_resume=value, acknowledged=acknowledged,
                    drained_metrics=drained)
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
        self.record('majority_cold_recovery', preserved=acknowledged, next_write=acknowledged + 1)
