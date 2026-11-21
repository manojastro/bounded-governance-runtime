"""OBSERVE + AUTO-REVERT.

After a change is applied, Bounded watches health signals for a window.
If availability, error rate, latency or restarts degrade beyond
threshold, it reverts the merge commit and alerts — automatically.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable, Optional

from bounded.models import HealthSnapshot


def degraded(snap: HealthSnapshot, baseline: HealthSnapshot) -> Optional[str]:
    """Return a reason string when a snapshot is worse than baseline."""
    if not snap.available:
        return "availability lost"
    if snap.error_rate > max(0.05, baseline.error_rate + 0.05):
        return f"error rate {snap.error_rate:.0%} > threshold"
    if snap.p95_ms > max(150.0, 3 * max(baseline.p95_ms, 1.0)):
        return f"p95 {snap.p95_ms:.0f}ms > threshold"
    if snap.restarts - baseline.restarts > 2:
        return f"restart count jumped by {snap.restarts - baseline.restarts}"
    return None


class Observer:
    def __init__(self, probe: Callable[[], HealthSnapshot],
                 revert_fn: Callable[[], str],
                 on_event: Callable[[str, dict], None],
                 window_s: float = 20.0, interval_s: float = 2.0):
        self.probe = probe
        self.revert_fn = revert_fn
        self.on_event = on_event
        self.window_s = window_s
        self.interval_s = interval_s

    def watch(self, intent_id: str, baseline: HealthSnapshot) -> dict:
        """Poll health; on degradation revert and confirm recovery."""
        started = time.monotonic()
        applied_at = datetime.now(timezone.utc)
        reason = None
        detected_at: Optional[datetime] = None
        reverted_at: Optional[datetime] = None
        recovered_at: Optional[datetime] = None
        snapshots: list[dict] = []

        while time.monotonic() - started < self.window_s:
            snap = self.probe()
            snapshots.append(snap.model_dump())
            why = degraded(snap, baseline)
            if why:
                reason = why
                detected_at = datetime.now(timezone.utc)
                self.on_event("degrade_detected", {
                    "intent_id": intent_id, "reason": why,
                    "snapshot": snap.model_dump()})
                sha = self.revert_fn()
                reverted_at = datetime.now(timezone.utc)
                self.on_event("auto_revert", {
                    "intent_id": intent_id, "reason": why, "commit": sha})
                # wait briefly for the reconciler to roll back and health return
                recovery_deadline = time.monotonic() + self.window_s
                while time.monotonic() < recovery_deadline:
                    snap = self.probe()
                    if not degraded(snap, baseline):
                        recovered_at = datetime.now(timezone.utc)
                        self.on_event("recovered", {"intent_id": intent_id})
                        break
                    time.sleep(self.interval_s)
                break
            time.sleep(self.interval_s)

        mttr_s = ((reverted_at or datetime.now(timezone.utc)) - applied_at
                  ).total_seconds() if reason else None
        return {
            "intent_id": intent_id,
            "healthy": reason is None,
            "reason": reason or "",
            "detected_at": detected_at.isoformat() if detected_at else None,
            "reverted_at": reverted_at.isoformat() if reverted_at else None,
            "recovered_at": recovered_at.isoformat() if recovered_at else None,
            "mttr_s": round(mttr_s, 2) if mttr_s is not None else None,
            "samples": len(snapshots),
        }
