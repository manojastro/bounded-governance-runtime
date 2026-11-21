"""Adapter interface — one contract for every execution target.

Bounded works on Kubernetes and Terraform through the same five calls.
Adapters never decide anything; they only report what WOULD happen
(dry_run) and execute/revert what the pipeline has approved.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from bounded.models import ChangeIntent, HealthSnapshot, ParsedChange, SimulateResult

# kinds that map to blast-radius touch flags
KIND_TOUCHES = {
    "NetworkPolicy": "network_policy",
    "Role": "rbac", "RoleBinding": "rbac",
    "ClusterRole": "rbac", "ClusterRoleBinding": "rbac",
    "Secret": "secrets",
    "PersistentVolumeClaim": "storage",
    "StorageClass": "storage",
    "StatefulSet": "statefulset",
}

WORKLOAD_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "ReplicaSet"}

# static service dependency graph for the demo estate: edges point from
# consumer to dependency; dependents are computed by reversal.
# (a live topology could be discovered from the cluster; the demo uses this)
TOPOLOGY = {
    "shop/web": ["shop/api"],
    "shop/api": ["shop/postgres"],
    "payments/payments-api": [],
}


def dependents_of(target: str) -> list[str]:
    """Everything that (transitively) depends on `target`."""
    rev: dict[str, list[str]] = {}
    for consumer, deps in TOPOLOGY.items():
        for d in deps:
            rev.setdefault(d, []).append(consumer)
    out, frontier = [], [target]
    seen = {target}
    while frontier:
        cur = frontier.pop()
        for dep in rev.get(cur, []):
            if dep not in seen:
                seen.add(dep)
                out.append(dep)
                frontier.append(dep)
    return out


class Adapter(ABC):
    name: str = "base"

    @abstractmethod
    def dry_run(self, intent: ChangeIntent, parsed: ParsedChange) -> SimulateResult:
        """Simulate the change and return the exact diff."""

    @abstractmethod
    def apply(self, intent: ChangeIntent, parsed: ParsedChange) -> dict:
        """Execute the change (only ever called after policy+route)."""

    @abstractmethod
    def snapshot_resource(self, kind: str, namespace: str, name: str) -> dict | None:
        """Current live spec of one resource (used to build rollbacks)."""

    def probe(self) -> HealthSnapshot:
        """Health of the estate this adapter manages (OBSERVE stage)."""
        return HealthSnapshot()

    def revert_files(self, pr_number: int, repo_dir: Path) -> dict:
        """Adapters that need file-level revert hooks implement this."""
        return {}
