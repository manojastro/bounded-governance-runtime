"""Immutable, hash-linked, signed audit chain.

Every row carries sha256(prev_hash + seq + canonical payload) and an
HMAC-SHA256 signature. `verify_chain()` detects any tampering, deletion
or reordering. Appends go through a single lock so the chain never forks.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

GENESIS = "0" * 64


def _secret() -> bytes:
    return os.environ.get("BOUNDED_AUDIT_SECRET", "bounded-dev-secret").encode()


def canonical(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class AuditChain:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _last_row(self, conn) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT seq, hash FROM audit ORDER BY seq DESC LIMIT 1"
        ).fetchone()

    def append(self, actor: str, intent_id: str, stage: str, action: str,
               payload: dict) -> dict:
        """Append one immutable audit row. Returns the row with its hash."""
        with self._lock:
            conn = self._connect()
            try:
                last = self._last_row(conn)
                prev_hash = last["hash"] if last else GENESIS
                seq = (last["seq"] + 1) if last else 1
                ts = datetime.now(timezone.utc).isoformat()
                body = canonical({"seq": seq, "ts": ts, "actor": actor,
                                  "intent_id": intent_id, "stage": stage,
                                  "action": action, "payload": payload})
                digest = hashlib.sha256(
                    (prev_hash + body).encode("utf-8")
                ).hexdigest()
                sig = hmac.new(_secret(), digest.encode("ascii"),
                               hashlib.sha256).hexdigest()
                conn.execute(
                    """INSERT INTO audit (seq, ts, actor, intent_id, stage, action,
                       payload, prev_hash, hash, sig) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (seq, ts, actor, intent_id, stage, action,
                     canonical(payload), prev_hash, digest, sig),
                )
                conn.commit()
                return {"seq": seq, "hash": digest, "sig": sig, "prev_hash": prev_hash}
            finally:
                conn.close()

    def verify_chain(self) -> dict:
        """Walk the whole chain; report the first inconsistency if any."""
        conn = self._connect()
        try:
            rows = conn.execute("SELECT * FROM audit ORDER BY seq").fetchall()
        finally:
            conn.close()
        prev = GENESIS
        expect_seq = 1
        for r in rows:
            if r["seq"] != expect_seq:
                return {"ok": False, "bad_seq": r["seq"],
                        "reason": f"expected seq {expect_seq}, found {r['seq']}"}
            body = canonical({"seq": r["seq"], "ts": r["ts"], "actor": r["actor"],
                              "intent_id": r["intent_id"], "stage": r["stage"],
                              "action": r["action"], "payload": json.loads(r["payload"])})
            digest = hashlib.sha256((prev + body).encode("utf-8")).hexdigest()
            if digest != r["hash"]:
                return {"ok": False, "bad_seq": r["seq"], "reason": "hash mismatch"}
            sig = hmac.new(_secret(), digest.encode("ascii"),
                           hashlib.sha256).hexdigest()
            if sig != r["sig"]:
                return {"ok": False, "bad_seq": r["seq"], "reason": "signature mismatch"}
            if r["prev_hash"] != prev:
                return {"ok": False, "bad_seq": r["seq"], "reason": "broken link"}
            prev = digest
            expect_seq += 1
        return {"ok": True, "bad_seq": None, "reason": "", "length": len(rows)}

    def list(self, limit: int = 300) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM audit ORDER BY seq DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
