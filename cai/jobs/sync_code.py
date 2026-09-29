#!/usr/bin/env python3
"""
CAI Job rsingh-mule-acct-sync-code: brings the CAI project to the pushed code.
The nightly DAG runs it before the scoring job; run it by hand after a push.

  1. git fetch + reset --hard to origin/<GIT_BRANCH> (default main) in the project.
     Data, models/, logs/ and .env are gitignored, so they are untouched; an
     uncommitted edit to a tracked file is discarded (develop in Git, not in the
     CAI project).
  2. pip installs requirements.txt when it differs from the last install (hash kept
     in models/.requirements.sha256). Packages land in the project's
     /home/cdsw/.local, which every job, the model build and the app see, so the
     first run of this job is also the project's one-time setup.
  3. EXPECTED_GIT_SHA (optional, in the run's environment) stops with a failure
     if the branch is at another commit.

Python 3.11 runtime, 2 vCPU / 8 GB: pip is killed at 2 GB while it installs the
torch and CUDA wheels. MULE_SKIP_GIT=1 skips step 1 (a project not cloned from Git).
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=_repo_root(), text=True).strip()


def install_requirements(root: Path, pip=None) -> bool:
    """pip install -r requirements.txt unless this exact file was installed last time."""
    reqs = root / "requirements.txt"
    digest = hashlib.sha256(reqs.read_bytes()).hexdigest()
    marker = root / "models" / ".requirements.sha256"
    if marker.exists() and marker.read_text().strip() == digest:
        print(f"requirements.txt unchanged ({digest[:12]}): nothing to install")
        return False
    print(f"installing requirements.txt ({digest[:12]})", flush=True)
    (pip or (lambda: subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", str(reqs)])))()
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(digest)
    return True


def main() -> int:
    root = _repo_root()
    branch = os.environ.get("GIT_BRANCH", "main")
    expected = os.environ.get("EXPECTED_GIT_SHA", "")
    if os.environ.get("MULE_SKIP_GIT", "") != "1":
        git("fetch", "origin", branch)
        git("reset", "--hard", f"origin/{branch}")
        sha = git("rev-parse", "HEAD")
        print(f"project now at {sha} (origin/{branch})")
        if expected and not sha.startswith(expected):
            print(f"expected commit {expected} but origin/{branch} is {sha[:12]}: stopping")
            return 1
    install_requirements(root)
    return 0


if __name__ == "__main__":
    # CAI Jobs run a script inside a Jupyter kernel wrapper: any SystemExit,
    # even sys.exit(0), is reported as a failure. Only exit on real failure.
    _rc = main()
    if _rc:
        sys.exit(_rc)
