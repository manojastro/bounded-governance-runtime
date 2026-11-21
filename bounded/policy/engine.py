"""POLICY — evaluate a change against OPA/Rego (or the embedded mirror).

Two engines, one rule set:
  * OpaEngine      — POSTs the input to an OPA server running bounded.rego
  * EmbeddedEngine — a line-for-line Python mirror of bounded.rego

BOUNDED_POLICY_ENGINE=auto (default) prefers OPA and falls back to the
embedded engine if OPA is unreachable, so tests and offline runs never
hard-require a server. Equivalence between the two is asserted in
tests/test_policy_equivalence.py whenever OPA is up.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

from bounded.identity import Identity
from bounded.models import (ChangeIntent, ParsedChange, PolicyDecision,
                            SimulateResult)

RULE_TEXT = {
    "R_UNKNOWN_AGENT":      "submitting agent is not registered (deny-by-default)",
    "R_IDENTITY_VERB":      "agent identity does not allow this verb",
    "R_IDENTITY_NAMESPACE": "agent identity does not allow this namespace",
    "R_QUOTA":              "agent exceeded its daily action quota",
    "R_NO_BYPASS":          "direct application requested — the pipeline cannot be bypassed",
    "R_FREEZE":             "a change freeze is in effect",
    "R_PROTECTED_NAMESPACE": "the payments namespace is protected",
    "R_TIER0":              "the change touches a tier-0 (criticality) resource",
    "R_TIME_WINDOW":        "production changes are blocked during business hours (Mon-Fri 09:00-17:00)",
    "R_NAMESPACE_DELETE":   "deleting namespaces is never allowed",
    "R_DELETE_WILDCARD":    "wildcard deletes are never allowed",
    "R_STATEFULSET_DELETE": "deleting StatefulSets (stateful workloads) is never allowed",
    "R_STATEFULSET_ZERO":   "scaling a StatefulSet to zero is never allowed",
    "R_PVC_DELETE":         "deleting PersistentVolumeClaims (data loss) is never allowed",
    "R_NETPOL_DELETE":      "deleting NetworkPolicies can silently remove segmentation",
    "R_CLUSTERADMIN_GRANT": "granting cluster-admin is never allowed",
    "R_SECRET_EXFIL":       "reading secrets is never allowed",
    "R_SECRET_ROLE_GRANT":  "granting roles that can read secrets is never allowed",
    "A_TIER0":              "tier-0 change by an authorized operator — needs human approval",
    "A_RBAC":               "RBAC change — needs human approval",
    "A_SECRET_WRITE":       "secret write — needs human approval",
    "A_NETPOL_WRITE":       "network policy change — needs human approval",
    "A_STORAGE":            "storage change — needs human approval",
    "A_STATEFULSET":        "stateful workload change — needs human approval",
}

PROD_NS = {"shop", "payments"}


def _freeze_active() -> bool:
    if os.environ.get("BOUNDED_FREEZE", "") in ("1", "true", "yes"):
        return True
    return Path("runtime/CHANGE_FREEZE").exists()


def is_business_hours(ts: datetime) -> bool:
    """Mon-Fri 09:00-17:00 in the timestamp's own timezone."""
    return ts.weekday() < 5 and 9 <= ts.hour < 17


def _rbac_flags(intent: ChangeIntent) -> dict:
    """Scan manifests for cluster-admin grants and secret-reading roles."""
    flags = {"grants_cluster_admin": False, "grants_secret_read": False}
    manifests = []
    op = intent.op
    if intent.adapter == "kubernetes":
        if op.get("manifest"):
            manifests.append(op["manifest"])
    for m in manifests:
        kind = (m.get("kind") or "").lower()
        if kind in ("role", "clusterrole"):
            for rule in m.get("rules", []) or []:
                res = rule.get("resources", []) or []
                verbs = rule.get("verbs", []) or []
                if "secrets" in res and any(v in verbs for v in ("get", "list", "watch", "*")):
                    flags["grants_secret_read"] = True
        if kind in ("rolebinding", "clusterrolebinding"):
            ref = ((m.get("roleRef") or {}).get("name") or "")
            if ref == "cluster-admin":
                flags["grants_cluster_admin"] = True
    return flags


def build_policy_input(intent: ChangeIntent, parsed: ParsedChange,
                       sim: SimulateResult, identity: Identity) -> dict:
    agent = identity.get(intent.agent_id)
    op = intent.op
    kinds = parsed.k8s_kinds[:]
    if intent.adapter == "terraform":
        kinds = ["Terraform"]  # resource kinds resolved at simulate time
    wildcard = (op.get("name") in ("*", "all", "everything")
                or op.get("label_selector") in ("*", "everything"))
    replicas = op.get("replicas")
    return {
        "change": {
            "verb": parsed.verb,
            "kinds": kinds or ["Unknown"],
            "namespaces": parsed.namespaces,
            "wildcard": bool(wildcard),
            "target_replicas": replicas if replicas is not None else 99,
            "tier0": bool(sim.touches.tier0),
            "touches": sim.touches.model_dump(),
            "apply_direct": bool(intent.apply_direct),
            **_rbac_flags(intent),
        },
        "agent": {
            "exists": agent is not None,
            "may_touch_tier0": bool(agent and agent.get("may_touch_tier0")),
            "allowed_verbs": agent["allowed_verbs"] if agent else [],
            "allowed_namespaces": agent["allowed_namespaces"] if agent else [],
        },
        "quota": {
            "used": 0,   # filled by caller with store data
            "limit": int(agent["max_actions_per_day"]) if agent else 0,
        },
        "context": {
            "now": intent.submitted_at.isoformat(),
            "business_hours": is_business_hours(intent.submitted_at),
            "freeze": _freeze_active(),
        },
    }


class EmbeddedEngine:
    """Line-for-line Python mirror of bounded.rego."""
    name = "embedded"

    def evaluate(self, inp: dict) -> PolicyDecision:
        ch, ag, qu, ctx = inp["change"], inp["agent"], inp["quota"], inp["context"]
        deny: set[str] = set()
        approve: set[str] = set()

        if not ag["exists"]:
            deny.add("R_UNKNOWN_AGENT")
        else:
            if ch["verb"] != "read" and ch["verb"] not in ag["allowed_verbs"]:
                deny.add("R_IDENTITY_VERB")
            ns_allowed = ("*" in ag["allowed_namespaces"]
                          or (ch["namespaces"]
                              and all(n in ag["allowed_namespaces"]
                                      for n in ch["namespaces"])))
            if ch["namespaces"] and not ns_allowed:
                deny.add("R_IDENTITY_NAMESPACE")
            if qu["used"] >= qu["limit"]:
                deny.add("R_QUOTA")

        if ch["apply_direct"]:
            deny.add("R_NO_BYPASS")
        if ctx["freeze"] and ch["verb"] != "read":
            deny.add("R_FREEZE")
        if ("payments" in ch["namespaces"] and ch["verb"] != "read"
                and not ag["may_touch_tier0"]):
            deny.add("R_PROTECTED_NAMESPACE")
        if ch["tier0"] and ch["verb"] != "read" and not ag["may_touch_tier0"]:
            deny.add("R_TIER0")
        if (ch["verb"] != "read" and not ag["may_touch_tier0"]
                and ctx["business_hours"]
                and any(n in PROD_NS for n in ch["namespaces"])):
            deny.add("R_TIME_WINDOW")
        if ch["verb"] == "delete" and "Namespace" in ch["kinds"]:
            deny.add("R_NAMESPACE_DELETE")
        if ch["verb"] == "delete" and ch["wildcard"]:
            deny.add("R_DELETE_WILDCARD")
        if ch["verb"] == "delete" and "StatefulSet" in ch["kinds"]:
            deny.add("R_STATEFULSET_DELETE")
        if (ch["verb"] == "scale" and "StatefulSet" in ch["kinds"]
                and ch["target_replicas"] <= 0):
            deny.add("R_STATEFULSET_ZERO")
        if ch["verb"] == "delete" and "PersistentVolumeClaim" in ch["kinds"]:
            deny.add("R_PVC_DELETE")
        if ch["verb"] == "delete" and "NetworkPolicy" in ch["kinds"]:
            deny.add("R_NETPOL_DELETE")
        if ch["grants_cluster_admin"]:
            deny.add("R_CLUSTERADMIN_GRANT")
        if ch["verb"] == "read" and ch["touches"]["secrets"]:
            deny.add("R_SECRET_EXFIL")
        if ch["grants_secret_read"]:
            deny.add("R_SECRET_ROLE_GRANT")

        if deny:
            rules = sorted(deny)
        else:
            if ch["tier0"] and ch["verb"] != "read" and ag["may_touch_tier0"]:
                approve.add("A_TIER0")
            t = ch["touches"]
            if ch["verb"] != "read":
                if t["rbac"]:
                    approve.add("A_RBAC")
                if t["secrets"]:
                    approve.add("A_SECRET_WRITE")
                if t["network_policy"]:
                    approve.add("A_NETPOL_WRITE")
                if t["storage"]:
                    approve.add("A_STORAGE")
                if t["statefulset"]:
                    approve.add("A_STATEFULSET")
            rules = sorted(approve)

        if deny:
            decision = "deny"
        elif approve:
            decision = "approval"
        else:
            decision = "allow"
        return PolicyDecision(decision=decision, rules=rules,
                              reasons=[RULE_TEXT[r] for r in rules],
                              engine=f"embedded")


class OpaEngine:
    name = "opa"

    def __init__(self, url: str | None = None):
        self.url = (url or os.environ.get("OPA_URL", "http://localhost:8181")).rstrip("/")

    def evaluate(self, inp: dict) -> PolicyDecision:
        resp = httpx.post(f"{self.url}/v1/data/bounded/decision",
                          json={"input": inp}, timeout=5.0)
        resp.raise_for_status()
        result = resp.json()["result"]
        rules = sorted(result.get("rules", []))
        decision = result["decision"]
        return PolicyDecision(decision=decision, rules=rules,
                              reasons=[RULE_TEXT[r] for r in rules],
                              engine="opa")


class PolicyEngine:
    """Facade: OPA-first with embedded fallback (auto mode)."""

    def __init__(self, opa_url: str | None = None):
        self.mode = os.environ.get("BOUNDED_POLICY_ENGINE", "auto").lower()
        self._opa = OpaEngine(opa_url) if self.mode in ("auto", "opa") else None
        self._embedded = EmbeddedEngine()
        self._opa_down = False

    def evaluate(self, inp: dict) -> PolicyDecision:
        if self._opa and not self._opa_down:
            try:
                return self._opa.evaluate(inp)
            except Exception:
                if self.mode == "opa":
                    raise
                self._opa_down = True  # fall back for the rest of the process
        return self._embedded.evaluate(inp)
