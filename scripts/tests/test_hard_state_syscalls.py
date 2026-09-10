"""Publication evidence fails on missing fences, extra metadata syncs, or gaps."""

from pathlib import Path
import runpy
import unittest

CHECK = runpy.run_path(str(Path(__file__).resolve().parents[1] / "check-hard-state-syscalls"))["check"]


def trace(body):
    return 'write(2, "BENCH_WRITES_BEGIN\\n", 19) = 19\n' + body + 'write(2, "BENCH_WRITES_END\\n", 17) = 17\n'


class PublicationSyscalls(unittest.TestCase):
    def test_both_contracts(self):
        self.assertEqual(CHECK(trace('fdatasync(3) = 0\n'), "journal", 1)["fdatasync"], 1)
        CHECK(trace('fdatasync(3) = 0\nrename("a", "b") = 0\nfsync(3) = 0\n'), "replace", 1)

    def test_missing_sync_or_extra_metadata_work_fails(self):
        for body in ['', 'fdatasync(3) = -1 EIO\n', 'fdatasync(3) = 0\nfsync(4) = 0\n',
                     'fdatasync(3) = 0\nrenameat2(3, "a", 4, "b", 0) = 0\n']:
            with self.assertRaises(ValueError):
                CHECK(trace(body), "journal", 1)

    def test_missing_duplicate_or_reversed_markers_fail(self):
        for text in ['', trace('') + trace(''), 'BENCH_WRITES_END\nBENCH_WRITES_BEGIN']:
            with self.assertRaises(ValueError):
                CHECK(text, "journal", 1)


if __name__ == "__main__":
    unittest.main()
