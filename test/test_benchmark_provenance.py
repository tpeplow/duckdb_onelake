import hashlib
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch
import uuid

from benchmark_provenance import checkout_provenance


class ProvenanceTest(unittest.TestCase):
    def test_diff_and_untracked_hashes_identify_dirty_source_without_saving_contents(self):
        directory = Path(__file__).parent / (".benchmark-provenance-test-" + uuid.uuid4().hex)
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory)
        source = directory / "new.py"
        source.write_bytes(b"source content not persisted")
        diff = b"tracked patch content not persisted"

        def git(command):
            if "rev-parse" in command:
                return b"known-head\n"
            if "status" in command:
                return b" M tracked.rs\0?? new.py\0"
            if "diff" in command:
                return diff
            if "ls-files" in command:
                return b"new.py\0"
            self.fail("Unexpected git call")

        with patch("benchmark_provenance.subprocess.check_output", side_effect=git):
            first = checkout_provenance(directory)
            source.write_bytes(b"different source content")
            second = checkout_provenance(directory)
        self.assertTrue(first["dirty"])
        self.assertEqual(first["tracked_diff_sha256"], hashlib.sha256(diff).hexdigest())
        self.assertNotEqual(first["worktree_state_sha256"], second["worktree_state_sha256"])
        self.assertNotEqual(first["untracked_files"][0]["sha256"], second["untracked_files"][0]["sha256"])
        self.assertNotIn("content not persisted", json.dumps(first))
        self.assertIn("Not proof of candidate build inputs", first["scope"])


if __name__ == "__main__":
    unittest.main()
