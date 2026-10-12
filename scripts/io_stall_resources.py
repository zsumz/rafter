"""Sample only owned, live Linux children; values are non-atomic observations."""
import os
from pathlib import Path
import time


def natural(value):
    if not value or any(c not in '0123456789' for c in value):
        raise ValueError('invalid non-negative proc counter')
    return int(value)


def parse_stat(raw, pid):
    prefix, separator, tail = raw.rpartition(') ')
    if not separator or not prefix.startswith(str(pid) + ' ('):
        raise ValueError('proc stat identity mismatch')
    fields = tail.split()  # Starts at field 3; comm may contain spaces and ')'.
    if len(fields) < 20:
        raise ValueError('truncated proc stat')
    return dict(start_ticks=natural(fields[19]), user_ticks=natural(fields[11]),
                system_ticks=natural(fields[12]))


def parse_status(raw, pid):
    required = {'Pid', 'VmRSS', 'VmHWM', 'VmSwap', 'Threads'}
    fields = {}
    for line in raw.splitlines():
        key, separator, value = line.partition(':')
        if separator and key in required:
            if key in fields:
                raise ValueError('duplicate proc status counter: ' + key)
            fields[key] = value.split()
    if set(fields) != required:
        raise ValueError('missing proc status counters')
    for key in ('Pid', 'Threads'):
        if len(fields[key]) != 1:
            raise ValueError('invalid proc status counter: ' + key)
    if natural(fields['Pid'][0]) != pid:
        raise ValueError('proc status identity mismatch')
    result = dict(threads=natural(fields['Threads'][0]))
    if result['threads'] == 0:
        raise ValueError('live child has no threads')
    for key, name in [('VmRSS', 'rss_bytes'), ('VmHWM', 'rss_high_water_bytes'),
                      ('VmSwap', 'swap_bytes')]:
        if len(fields[key]) != 2 or fields[key][1] != 'kB':
            raise ValueError('invalid proc status memory unit: ' + key)
        result[name] = natural(fields[key][0]) * 1024
    return result


def sample_process(child, proc_root=Path('/proc')):
    started = time.monotonic_ns()
    if child.poll() is not None:
        raise RuntimeError('resource sample requires a live owned child')
    root = proc_root / str(child.pid)
    before = (root / 'stat').read_text()
    identity = parse_stat(before, child.pid)
    status = (root / 'status').read_text()
    values = parse_status(status, child.pid)
    descriptors = sorted(entry.name for entry in (root / 'fd').iterdir())
    if any(not name.isascii() or not name.isdecimal() for name in descriptors):
        raise ValueError('invalid proc descriptor name')
    after = (root / 'stat').read_text()
    current = parse_stat(after, child.pid)
    if child.poll() is not None or identity['start_ticks'] != current['start_ticks']:
        raise RuntimeError('child exited or changed identity during resource sample')
    ticks_per_second = os.sysconf('SC_CLK_TCK')
    if ticks_per_second <= 0:
        raise RuntimeError('invalid process clock tick rate')
    return dict(pid=child.pid, started_ns=started, finished_ns=time.monotonic_ns(),
                ticks_per_second=ticks_per_second, descriptors=len(descriptors),
                **current, **values,
                raw=dict(stat_before=before, stat_after=after, status=status,
                         descriptor_names=descriptors))


def sample_children(children):
    if set(children) != {1, 2, 3}:
        raise RuntimeError('resource sample requires all three owned nodes')
    if len({child.pid for child in children.values()}) != 3:
        raise RuntimeError('resource sample requires three distinct owned processes')
    return {node: sample_process(child) for node, child in sorted(children.items())}
