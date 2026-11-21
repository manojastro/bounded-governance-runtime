"""SQLite persistence for intents, pipeline traces, PRs, quotas and events."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_DEFAULT_DB = Path(os.environ.get("BOUNDED_DB", "runtime/bounded.db"))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    description TEXT NOT NULL,
    adapter TEXT NOT NULL,
    op TEXT NOT NULL,
    requested_by TEXT DEFAULT 'agent',
    ticket_ref TEXT DEFAULT '',
    apply_direct INTEGER DEFAULT 0,
    label TEXT DEFAULT '',
    expected TEXT DEFAULT '',
    degrade INTEGER DEFAULT 0,
    verdict TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    ok INTEGER DEFAULT 1,
    started_at TEXT NOT NULL,
    duration_ms INTEGER DEFAULT 0,
    input_summary TEXT DEFAULT '',
    output_summary TEXT DEFAULT '',
    detail TEXT DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS prs (
    number INTEGER PRIMARY KEY,
    intent_id TEXT NOT NULL,
    branch TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    created_at TEXT NOT NULL,
    merged_at TEXT,
    rollback_commit TEXT DEFAULT '',
    diff_summary TEXT DEFAULT '',
    title TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS audit (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL,
    sig TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    intent_id TEXT DEFAULT '',
    data TEXT DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS quota_usage (
    agent_id TEXT NOT NULL,
    day TEXT NOT NULL,
    used INTEGER DEFAULT 0,
    PRIMARY KEY (agent_id, day)
);
CREATE TABLE IF NOT EXISTS exec_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    arm TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    adapter TEXT NOT NULL,
    op TEXT NOT NULL,
    tier0 INTEGER DEFAULT 0,
    ok INTEGER DEFAULT 1,
    detail TEXT DEFAULT ''
);
"""


class Store:
    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path or _DEFAULT_DB)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._conn() as c:
            c.executescript(_SCHEMA)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # ---- intents ----
    def save_intent(self, intent_dict: dict):
        with self._lock, self._conn() as c:
            c.execute(
                """INSERT OR REPLACE INTO intents
                   (id, agent_id, submitted_at, description, adapter, op,
                    requested_by, ticket_ref, apply_direct, label, expected,
                    degrade, verdict)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    intent_dict["id"], intent_dict["agent_id"],
                    intent_dict["submitted_at"], intent_dict["description"],
                    intent_dict["adapter"], json.dumps(intent_dict["op"]),
                    intent_dict.get("requested_by", "agent"),
                    intent_dict.get("ticket_ref", ""),
                    int(intent_dict.get("apply_direct", False)),
                    intent_dict.get("label", ""), intent_dict.get("expected", ""),
                    int(intent_dict.get("degrade", False)),
                    intent_dict.get("verdict", ""),
                ),
            )

    def set_verdict(self, intent_id: str, verdict: str):
        with self._lock, self._conn() as c:
            c.execute("UPDATE intents SET verdict=? WHERE id=?", (verdict, intent_id))

    def get_intent(self, intent_id: str) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM intents WHERE id=?", (intent_id,)).fetchone()
            return dict(row) if row else None

    def list_intents(self, limit: int = 500) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM intents ORDER BY rowid DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ---- traces ----
    def add_trace(self, stage: dict):
        with self._lock, self._conn() as c:
            c.execute(
                """INSERT INTO traces (intent_id, stage, ok, started_at,
                   duration_ms, input_summary, output_summary, detail)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (stage["intent_id"], stage["stage"], int(stage.get("ok", True)),
                 stage.get("started_at", datetime.now(timezone.utc).isoformat()),
                 stage.get("duration_ms", 0), stage.get("input_summary", ""),
                 stage.get("output_summary", ""), json.dumps(stage.get("detail", {}))),
            )

    def get_trace(self, intent_id: str) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM traces WHERE intent_id=? ORDER BY id", (intent_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ---- PRs ----
    def save_pr(self, pr: dict):
        with self._lock, self._conn() as c:
            c.execute(
                """INSERT OR REPLACE INTO prs (number, intent_id, branch, commit_sha,
                   status, created_at, merged_at, rollback_commit, diff_summary, title)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (pr["number"], pr["intent_id"], pr["branch"],
                 pr.get("commit_sha") or pr.get("commit"),
                 pr["status"], pr["created_at"], pr.get("merged_at"),
                 pr.get("rollback_commit", ""), pr.get("diff_summary", ""),
                 pr.get("title", "")),
            )

    def get_pr(self, number: int) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM prs WHERE number=?", (number,)).fetchone()
            return dict(row) if row else None

    def get_pr_by_intent(self, intent_id: str) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM prs WHERE intent_id=? ORDER BY number DESC LIMIT 1",
                (intent_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_prs(self) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM prs ORDER BY number").fetchall()
            return [dict(r) for r in rows]

    def next_pr_number(self) -> int:
        with self._conn() as c:
            row = c.execute("SELECT COALESCE(MAX(number), 100) AS m FROM prs").fetchone()
            return (row["m"] if row else 100) + 1

    # ---- events ----
    def add_event(self, kind: str, intent_id: str = "", data: dict | None = None):
        with self._lock, self._conn() as c:
            c.execute(
                "INSERT INTO events (ts, kind, intent_id, data) VALUES (?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), kind, intent_id,
                 json.dumps(data or {})),
            )

    def list_events(self, limit: int = 200) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ---- quotas ----
    def quota_used(self, agent_id: str, day: str) -> int:
        with self._conn() as c:
            row = c.execute(
                "SELECT used FROM quota_usage WHERE agent_id=? AND day=?",
                (agent_id, day),
            ).fetchone()
            return row["used"] if row else 0

    def quota_increment(self, agent_id: str, day: str, by: int = 1) -> int:
        with self._lock, self._conn() as c:
            c.execute(
                """INSERT INTO quota_usage (agent_id, day, used) VALUES (?,?,?)
                   ON CONFLICT(agent_id, day) DO UPDATE SET used = used + ?""",
                (agent_id, day, by, by),
            )
            row = c.execute(
                "SELECT used FROM quota_usage WHERE agent_id=? AND day=?",
                (agent_id, day),
            ).fetchone()
            return row["used"]

    # ---- unguarded-arm execution log (for the eval comparison) ----
    def log_exec(self, arm: str, agent_id: str, intent_id: str, adapter: str,
                 op: dict, tier0: bool, ok: bool, detail: str = ""):
        with self._lock, self._conn() as c:
            c.execute(
                """INSERT INTO exec_log (ts, arm, agent_id, intent_id, adapter, op,
                   tier0, ok, detail) VALUES (?,?,?,?,?,?,?,?,?)""",
                (datetime.now(timezone.utc).isoformat(), arm, agent_id, intent_id,
                 adapter, json.dumps(op), int(tier0), int(ok), detail),
            )

    def exec_log(self, arm: str) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM exec_log WHERE arm=? ORDER BY id", (arm,)
            ).fetchall()
            return [dict(r) for r in rows]
