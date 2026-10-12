"""Own only a newly created private Linux filesystem for stall qualification.

No block device is accepted from the caller. Formatting targets only the newly
created regular image; all subsequent resources derive from that image.
"""
import json
import os
from pathlib import Path
import re
import sys
import uuid


class DisposableVolume:
    def __init__(self, directory, run):
        self.root = Path(directory).resolve()
        self.image = self.root / 'backing.img'
        self.mount = self.root / 'mounted'
        token = uuid.uuid4().hex
        self.name = 'rafter-stall-' + token
        self.uuid = 'rafter-stall-' + token
        self.device = '/dev/mapper/' + self.name
        self.run = run
        self.loop = None
        self.loop_attempted = False
        self.loop_number = None
        self.mapping_attempted = False
        self.mount_attempted = False
        self.suspend_attempted = False
        self.sectors = 128 * 1024 * 1024 // 512

    def admin(self, *args, check=True):
        return self.run(['sudo', '-n', *args], check=check)

    def create(self):
        if sys.platform != 'linux':
            raise RuntimeError('isolated block-device qualification requires Linux')
        self.root.mkdir(mode=0o700)
        self.mount.mkdir(mode=0o700)
        with self.image.open('xb') as image:
            image.truncate(self.sectors * 512)
        try:
            self.run(['mkfs.ext4', '-q', '-F', '-E',
                      'nodiscard,lazy_itable_init=0,lazy_journal_init=0', str(self.image)])
            self.loop_attempted = True
            loop = self.admin('losetup', '--find', '--show', '--nooverlap',
                              str(self.image)).stdout.strip()
            if not re.fullmatch(r'/dev/loop[0-9]+', loop):
                raise RuntimeError('losetup did not return one loop device')
            self.loop = loop
            self.assert_loop()
            self.loop_number = self.admin('lsblk', '-dn', '-o', 'MAJ:MIN', loop).stdout.strip()
            if not re.fullmatch(r'[0-9]+:[0-9]+', self.loop_number):
                raise RuntimeError('loop device number is unavailable')
            self.mapping_attempted = True
            self.admin('dmsetup', 'create', self.name, '--uuid', self.uuid,
                       '--table', self.expected_table())
            self.assert_mapping()
            self.mount_attempted = True
            self.admin('mount', '-t', 'ext4', '-o', 'nodev,nosuid,noexec,noatime',
                       self.device, str(self.mount))
            self.assert_mount()
            self.admin('chown', f'{os.getuid()}:{os.getgid()}', str(self.mount))
        except BaseException:
            # A failed or timed-out create may still have acquired a resource.
            # Cleanup queries the exact owned identity before releasing it.
            self.close()
            raise

    def expected_table(self):
        return f'0 {self.sectors} linear {self.loop_number} 0'

    def recover_loop(self):
        """A timed-out allocation may have succeeded without returning its name."""
        result = self.admin('losetup', '--json', '--list', '--output',
                            'NAME,BACK-FILE,RO')
        matches = [device for device in json.loads(result.stdout)['loopdevices']
                   if device['back-file'] == str(self.image)]
        if not matches:
            return
        if len(matches) != 1 or not re.fullmatch(r'/dev/loop[0-9]+', matches[0]['name']):
            raise RuntimeError('cannot establish a unique owned loop association')
        self.loop = matches[0]['name']
        self.assert_loop()

    def assert_loop(self):
        result = self.admin('losetup', '--json', '--list', '--output',
                            'NAME,BACK-FILE,RO', self.loop)
        devices = json.loads(result.stdout)['loopdevices']
        if len(devices) != 1:
            raise RuntimeError('loop association is not unique')
        device = devices[0]
        if (device['name'] != self.loop or device['back-file'] != str(self.image)
                or device['ro'] not in (False, 0)):
            raise RuntimeError('loop association no longer belongs to this image')

    def mapping_exists(self):
        # A successful inventory distinguishes absence from a query failure.
        result = self.admin('dmsetup', 'info', '-c', '--noheadings', '--separator',
                            '|', '-o', 'name,uuid')
        matches = [line.strip().split('|') for line in result.stdout.splitlines()
                   if line.strip().split('|')[0].strip() == self.name]
        if not matches:
            return False
        if len(matches) != 1 or [x.strip() for x in matches[0]] != [self.name, self.uuid]:
            raise RuntimeError('mapping identity changed; refusing to operate on it')
        return True

    def assert_mapping(self):
        self.assert_loop()
        if not self.mapping_exists():
            raise RuntimeError('owned mapping is missing')
        table = self.admin('dmsetup', 'table', self.name).stdout.strip()
        if ' '.join(table.split()) != self.expected_table():
            raise RuntimeError('mapping table no longer names the owned image')

    def mount_record(self):
        result = self.run(['findmnt', '--json', '--mountpoint', str(self.mount),
                           '-o', 'TARGET,FSTYPE,MAJ:MIN'], check=False)
        if result.returncode == 1 and not result.stdout and not result.stderr:
            return None
        if result.returncode:
            raise RuntimeError('cannot establish mount ownership')
        records = json.loads(result.stdout)['filesystems']
        if len(records) != 1:
            raise RuntimeError('mount record is not unique')
        return records[0]

    def assert_mount(self):
        record = self.mount_record()
        number = self.admin('lsblk', '-dn', '-o', 'MAJ:MIN', self.device).stdout.strip()
        if not re.fullmatch(r'[0-9]+:[0-9]+', number):
            raise RuntimeError('mapping device number is unavailable')
        if (record is None or record['target'] != str(self.mount)
                or record['fstype'] != 'ext4' or record['maj:min'] != number):
            raise RuntimeError('mount no longer belongs to the owned mapping')

    def suspend(self):
        self.assert_mapping()
        self.assert_mount()
        self.suspend_attempted = True
        self.admin('dmsetup', 'suspend', '--nolockfs', '--noflush', self.name)

    def resume(self):
        if self.suspend_attempted:
            self.assert_mapping()
            self.admin('dmsetup', 'resume', self.name)
            self.suspend_attempted = False

    def close(self):
        # The controller must call resume BEFORE reaping any blocked child.
        # Do not tear down a lower resource after an upper cleanup failed.
        self.resume()
        if self.mount_attempted and self.mount_record() is not None:
            self.assert_mapping()
            self.assert_mount()
            self.admin('umount', str(self.mount))
            if self.mount_record() is not None:
                raise RuntimeError('owned mount still exists after unmount')
        self.mount_attempted = False
        if self.mapping_attempted and self.mapping_exists():
            self.assert_mapping()
            self.admin('dmsetup', 'remove', self.name)
            if self.mapping_exists():
                raise RuntimeError('owned mapping still exists after removal')
        self.mapping_attempted = False
        if self.loop_attempted and self.loop is None:
            self.recover_loop()
        if self.loop is not None:
            self.assert_loop()
            self.admin('losetup', '--detach', self.loop)
            self.loop = None
            self.recover_loop()
            if self.loop is not None:
                raise RuntimeError('owned loop remains associated after detach')
        self.loop_attempted = False
