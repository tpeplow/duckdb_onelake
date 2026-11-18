"""Content hashes identify artifacts and dirty checkout state without storing diffs."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def checkout_provenance(root):
    def git(*args):
        return subprocess.check_output(["git", "--no-pager", "-C", str(root), *args])

    head = git("rev-parse", "HEAD").decode("ascii").strip()
    status = git("status", "--porcelain", "-z")
    tracked_diff_hash = hashlib.sha256(git("diff", "--binary", "HEAD", "--")).hexdigest()
    untracked = []
    for raw_path in sorted(filter(None, git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0"))):
        relative = os.fsdecode(raw_path)
        path = Path(root).joinpath(*relative.split("/"))
        is_link = path.is_symlink()
        digest = hashlib.sha256(os.fsencode(os.readlink(path))).hexdigest() if is_link else file_sha256(path)
        untracked.append({"path": relative, "sha256": digest, "symlink": is_link})
    state = {
        "head": head,
        "tracked_diff_sha256": tracked_diff_hash,
        "untracked_files": untracked,
    }
    return {
        **state,
        "dirty": bool(status),
        "worktree_state_sha256": hashlib.sha256(
            json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": (
            "Checkout observed at benchmark start: HEAD, git diff --binary HEAD, "
            "and git-visible untracked file hashes. Ignored files excluded. "
            "Not proof of candidate build inputs; source label is caller-supplied, "
            "executable SHA256 identifies the measured artifact."
        ),
    }
