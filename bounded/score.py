"""SCORE — compute a 0-100 blast radius from the dry-run diff.

The score is deliberately transparent: every component is listed with a
rationale string, so a human approver (and the dashboard) can see exactly
why a change scored what it did.
"""
from __future__ import annotations

from bounded.models import ParsedChange, ScoreResult, SimulateResult

VERB_BASE = {"create": 8, "update": 12, "patch": 10, "scale": 14,
             "delete": 34, "grant": 22, "read": 0}

TOUCH_WEIGHTS = {
    "network_policy": 12,
    "rbac": 18,
    "secrets": 14,
    "storage": 16,
    "statefulset": 18,
    "tier0": 35,
}

PROD_NAMESPACES = {"shop", "payments"}  # env=prod in this demo estate


def score_change(parsed: ParsedChange, sim: SimulateResult) -> ScoreResult:
    comps: dict[str, int] = {}
    why: list[str] = []

    comps["verb"] = VERB_BASE.get(parsed.verb, 15)
    why.append(f"base for {parsed.verb} operation: {comps['verb']}")

    # namespace-level changes (creating/deleting namespaces) — not merely
    # "the op targets some namespace"
    if sim.namespaces_affected:
        comps["namespaces"] = min(sim.namespaces_affected * 18, 54)
        why.append(f"namespaces affected: {sim.namespaces_affected} "
                   f"(+{comps['namespaces']})")

    if sim.workloads_affected:
        comps["workloads"] = min(sim.workloads_affected * 3, 24)
        why.append(f"workloads affected: {sim.workloads_affected} "
                   f"(+{comps['workloads']})")

    if sim.pods_affected:
        comps["pods"] = min(sim.pods_affected, 15)
        why.append(f"pods affected: ~{sim.pods_affected} (+{comps['pods']})")

    if sim.services_affected:
        comps["services"] = min(sim.services_affected * 2, 8)

    if sim.dependents:
        comps["dependents"] = min(len(sim.dependents) * 3, 15)
        why.append(f"downstream dependents: {', '.join(sim.dependents)} "
                   f"(+{comps['dependents']})")

    for flag, weight in TOUCH_WEIGHTS.items():
        if getattr(sim.touches, flag):
            comps[f"touch_{flag}"] = weight
            why.append(f"touches {flag} (+{weight})")

    if any(ns in PROD_NAMESPACES for ns in parsed.namespaces):
        comps["prod"] = 8
        why.append("production namespace (+8)")

    total = min(sum(comps.values()), 100)
    return ScoreResult(total=total, components=comps,
                       tier0=sim.touches.tier0, rationale=why)
