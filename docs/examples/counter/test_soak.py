"""Failure paths retain owned-process cleanup and original recovery inputs."""
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

import soak


class RetainedSoak(unittest.TestCase):
    def test_cleanup_attempts_other_children_after_a_wait_failure(self):
        first, second = Mock(pid=11), Mock(pid=12)
        first.poll.return_value = second.poll.return_value = None
        first.wait.side_effect = subprocess.TimeoutExpired("owned-child", 15)
        errors = soak.reap([first, second])
        self.assertEqual(len(errors), 1)
        first.kill.assert_called_once_with()
        second.kill.assert_called_once_with()
        second.wait.assert_called_once_with(timeout=15)

    def test_finished_children_are_reaped_without_signalling_them(self):
        child = Mock(pid=13)
        child.poll.return_value = -9
        self.assertEqual(soak.reap([child]), [])
        child.kill.assert_not_called()
        child.wait.assert_called_once_with(timeout=15)

    def test_recovery_input_copy_is_independent_and_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, copied = root / "source", root / "before-recovery"
            source.mkdir()
            (source / "state").write_bytes(b"before")
            manifest = soak.preserve_state(source, copied)
            self.assertEqual(manifest, {"state": {"bytes": 6, "sha256": hashlib.sha256(b"before").hexdigest()}})
            (source / "state").write_bytes(b"after")
            self.assertEqual((copied / "state").read_bytes(), b"before")
            with self.assertRaises(FileExistsError):
                soak.preserve_state(source, copied)

    def test_state_copy_refuses_links_before_copying_foreign_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, copied = root / "source", root / "before-recovery"
            source.mkdir()
            (root / "outside").write_text("unrelated")
            (source / "link").symlink_to(root / "outside")
            with self.assertRaisesRegex(ValueError, "symlinks"):
                soak.preserve_state(source, copied)
            self.assertFalse(copied.exists())


if __name__ == "__main__":
    unittest.main()
