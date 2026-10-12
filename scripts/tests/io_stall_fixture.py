"""Stateful host fixture: no privileged command or real block device is used."""
import json
from pathlib import Path
import subprocess



class FakeHost:
    def __init__(self):
        self.commands = []
        self.loop = None
        self.mapping = None
        self.mounted = False
        self.suspended = False
        self.failure = None
        self.after_effect = False
        self.foreign_mount = False

    def __call__(self, args, check=True):
        args = list(args)
        if args[:2] == ['sudo', '-n']:
            args = args[2:]
        self.commands.append(args)
        key = tuple(args[:2])
        failing = self.failure == key
        if failing and not self.after_effect:
            self.failure = None
            raise subprocess.CalledProcessError(23, args)
        code, output = 0, ''
        if args[0] == 'mkfs.ext4':
            assert Path(args[-1]).is_file()
        elif key == ('losetup', '--find'):
            assert self.loop is None
            self.loop = args[-1]
            output = '/dev/loop123\n'
        elif key == ('losetup', '--json'):
            devices = [] if self.loop is None else [dict(name='/dev/loop123',
                         **{'back-file': self.loop, 'ro': False})]
            output = json.dumps({'loopdevices': devices})
        elif args[0] == 'lsblk':
            output = '7:123\n' if args[-1] == '/dev/loop123' else '253:123\n'
        elif key == ('dmsetup', 'create'):
            assert self.loop is not None and self.mapping is None
            self.mapping = dict(name=args[2], uuid=args[4], table=args[6])
        elif key == ('dmsetup', 'info'):
            if self.mapping:
                output = self.mapping['name'] + '|' + self.mapping['uuid'] + '\n'
        elif key == ('dmsetup', 'table'):
            output = self.mapping['table'] + '\n'
        elif args[0] == 'mount':
            assert self.mapping and not self.mounted
            self.mounted = args[-1]
        elif args[0] == 'findmnt':
            if self.mounted:
                output = json.dumps({'filesystems': [dict(target=self.mounted,
                    fstype='ext4', **{'maj:min': '9:9' if self.foreign_mount else '253:123'})]})
            else:
                code = 1
        elif args[0] == 'chown':
            assert args[-1] == self.mounted
        elif key == ('dmsetup', 'suspend'):
            assert self.mapping and self.mounted
            self.suspended = True
        elif key == ('dmsetup', 'resume'):
            assert self.mapping
            self.suspended = False
        elif args[0] == 'umount':
            assert self.mounted == args[-1] and not self.suspended
            self.mounted = False
        elif key == ('dmsetup', 'remove'):
            assert self.mapping and not self.mounted and not self.suspended
            self.mapping = None
        elif key == ('losetup', '--detach'):
            assert self.loop and not self.mapping
            self.loop = None
        else:
            raise AssertionError(('unexpected host command', args))
        if failing:
            self.failure = None
            raise subprocess.TimeoutExpired(args, 30)
        assert code == 0 or not check
        return subprocess.CompletedProcess(args, code, stdout=output, stderr='')

    def empty(self):
        return self.loop is None and self.mapping is None and not self.mounted
