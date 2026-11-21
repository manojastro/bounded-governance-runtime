"""GitOps — changes land on the cluster only through git.

APPLY never touches the cluster directly: it opens a branch + a pull
request record in the mock GitHub (the SQLite `prs` table). Merging a
PR moves `main`; a reconciler (services/reconciler.py) watches `main`
and applies it. Auto-revert is `git revert` of the merge commit.
"""
from __future__ import annotations

import subprocess
import threading
from pathlib import Path

from bounded.models import PRRecord


class GitRepo:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        if not (self.path / ".git").exists():
            self._run("git", "init", "-b", "main")
            (self.path / "README.md").write_text(
                "# Bounded GitOps environment\n\nmain == live cluster state.\n")
            self._run("git", "add", "-A")
            self._commit("initial: empty environment")

    def _run(self, *args: str, cwd: Path | None = None) -> str:
        out = subprocess.run(args, cwd=cwd or self.path, capture_output=True,
                             text=True, shell=False)
        if out.returncode != 0:
            raise RuntimeError(f"{' '.join(args)} failed: {out.stderr.strip()}")
        return out.stdout.strip()

    def _commit(self, msg: str) -> str:
        self._run("git", "-c", "user.name=bounded-bot",
                  "-c", "user.email=bounded@bot.local",
                  "commit", "-m", msg)
        return self.head()

    def head(self) -> str:
        return self._run("git", "rev-parse", "HEAD")

    def branch_exists(self, branch: str) -> bool:
        out = subprocess.run(["git", "rev-parse", "--verify", branch],
                             cwd=self.path, capture_output=True, text=True)
        return out.returncode == 0

    def open_change(self, intent_id: str, files: dict[str, str],
                    message: str) -> tuple[str, str]:
        """Create a branch with the proposed files; return (branch, commit)."""
        with self._lock:
            branch = f"bounded/{intent_id}"
            if self.branch_exists(branch):
                self._run("git", "branch", "-D", branch)
            self._run("git", "checkout", "-b", branch, "main")
            try:
                for rel, content in files.items():
                    target = self.path / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(content)
                self._run("git", "add", "-A")
                commit = self._commit(message)
            finally:
                self._run("git", "checkout", "main")
            return branch, commit

    def merge(self, branch: str, message: str) -> str:
        with self._lock:
            self._run("git", "checkout", "main")
            self._run("git", "merge", "--no-ff", branch, "-m", message)
            sha = self.head()
            self._run("git", "branch", "-D", branch)
            return sha

    def reject(self, branch: str):
        with self._lock:
            self._run("git", "checkout", "main")
            if self.branch_exists(branch):
                self._run("git", "branch", "-D", branch)

    def revert(self, merge_sha: str, message: str) -> str:
        with self._lock:
            self._run("git", "checkout", "main")
            self._run("git", "-c", "user.name=bounded-bot",
                      "-c", "user.email=bounded@bot.local",
                      "revert", "-m", "1", "--no-edit", merge_sha)
            # ensure the explanatory message lands in the log
            return self.head()

    def list_files(self, prefix: str = "") -> dict[str, str]:
        with self._lock:
            names = self._run("git", "ls-files").splitlines()
            out = {}
            for n in names:
                if n.startswith(prefix) and n.endswith((".json", ".yaml", ".yml", ".tf")):
                    p = self.path / n
                    if p.exists():
                        out[n] = p.read_text()
            return out

    def changed_files(self, since_sha: str) -> list[str]:
        out = self._run("git", "diff", "--name-only", since_sha, "HEAD")
        return [l for l in out.splitlines() if l.strip()]
