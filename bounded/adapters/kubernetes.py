"""Kubernetes adapter — real cluster via kubectl (server-side dry-runs)."""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

import httpx

from bounded.adapters.base import KIND_TOUCHES, WORKLOAD_KINDS, Adapter, dependents_of
from bounded.models import (ChangeIntent, HealthSnapshot, K8sOp, ParsedChange,
                            ResourceChange, SimulateResult, TouchFlags)

TIER0_NS = {"payments"}


class K8sAdapter(Adapter):
    name = "kubernetes"

    def __init__(self, kubeconfig: str | None = None):
        self.kubectl = os.environ.get("KUBECTL", "kubectl")
        self.kubeconfig = kubeconfig or os.environ.get("KUBECONFIG")

    def _kubectl(self, *args: str, stdin: str | None = None,
                 check: bool = True) -> subprocess.CompletedProcess:
        cmd = [self.kubectl]
        if self.kubeconfig:
            cmd += ["--kubeconfig", self.kubeconfig]
        cmd += list(args)
        out = subprocess.run(cmd, input=stdin, capture_output=True, text=True,
                             shell=False, timeout=90)
        if check and out.returncode != 0:
            raise RuntimeError(f"kubectl failed: {out.stderr.strip()[:400]}")
        return out

    # ---- live state helpers ----
    def snapshot_resource(self, kind, namespace, name) -> Optional[dict]:
        out = self._kubectl("get", kind, name, "-n", namespace, "-o", "json",
                            check=False)
        if out.returncode != 0:
            return None
        obj = json.loads(out.stdout)
        return self._compact(obj)

    def _compact(self, obj: dict) -> dict:
        out = {"kind": obj.get("kind"), "name": obj["metadata"]["name"],
               "namespace": obj["metadata"].get("namespace", ""),
               "labels": obj["metadata"].get("labels", {})}
        spec = obj.get("spec", {}) or {}
        if "replicas" in spec:
            out["replicas"] = spec["replicas"]
        containers = (spec.get("template", {}).get("spec", {})
                      .get("containers", []) or [])
        if containers:
            out["image"] = containers[0].get("image", "")
            out["env"] = {e["name"]: e.get("value", "")
                          for e in containers[0].get("env", []) or []}
        return out

    def _replica_counts(self, ns: str, kinds: str = "deployments,statefulsets") -> tuple[int, int]:
        """(total desired, total ready) across the namespace."""
        out = self._kubectl("get", kinds, "-n", ns, "-o", "json", check=False)
        if out.returncode != 0:
            return 0, 0
        items = json.loads(out.stdout).get("items", [])
        desired = sum(i.get("spec", {}).get("replicas", 0) for i in items)
        ready = sum(i.get("status", {}).get("readyReplicas", 0) for i in items)
        return desired, ready

    def _pod_restarts(self, ns: str) -> int:
        out = self._kubectl("get", "pods", "-n", ns, "-o", "json", check=False)
        if out.returncode != 0:
            return 0
        total = 0
        for pod in json.loads(out.stdout).get("items", []):
            for cs in pod.get("status", {}).get("containerStatuses", []) or []:
                total += cs.get("restartCount", 0)
        return total

    def _workload_pods(self, ns: str, selector: str) -> int:
        args = ["get", "pods", "-n", ns]
        if selector:
            args += ["-l", selector]
        out = self._kubectl(*args, "--no-headers", check=False)
        if out.returncode != 0:
            return 0
        return max(0, len([l for l in out.stdout.splitlines() if l.strip()]))

    def _is_tier0(self, obj: Optional[dict], ns: str) -> bool:
        if obj and (obj.get("labels", {}).get("criticality") == "tier-0"):
            return True
        return ns in TIER0_NS

    # ---- dry run ----
    def dry_run(self, intent: ChangeIntent, parsed: ParsedChange) -> SimulateResult:
        op = K8sOp(**intent.op)
        changes: list[ResourceChange] = []
        touches = TouchFlags()
        pods = workloads = services = 0
        namespaces_affected = 0
        tier0 = False

        if op.manifest or op.verb in ("create", "update", "patch"):
            m = op.manifest or {}
            kind = op.kind or m.get("kind", "")
            name = op.name or m.get("metadata", {}).get("name", "")
            ns = op.namespace or m.get("metadata", {}).get("namespace", "default")
            before = self.snapshot_resource(kind, ns, name) if kind and name else None
            # server-side dry run returns the object that WOULD be persisted
            out = self._kubectl("apply", "--dry-run=server", "-o", "json",
                                "-f", "-", stdin=json.dumps(m))
            after_obj = json.loads(out.stdout) if out.stdout.strip() else {}
            after = self._compact(after_obj) if after_obj else None
            action = "update" if before else "create"
            changes.append(ResourceChange(kind=kind, name=name, namespace=ns,
                                          action=action, before=before, after=after))
            if kind in WORKLOAD_KINDS:
                workloads += 1
                pods += int((after or {}).get("replicas", 0)
                            or (before or {}).get("replicas", 0) or 1)
            if kind == "Service":
                services += 1
            tier0 = tier0 or self._is_tier0(before, ns) or self._is_tier0(None, ns)
            flag = KIND_TOUCHES.get(kind)
            if flag:
                setattr(touches, flag, True)
            if (m.get("metadata", {}).get("labels", {}) or {}).get("criticality") == "tier-0":
                tier0 = True

        elif op.verb == "scale":
            before = self.snapshot_resource(op.kind, op.namespace, op.name)
            changes.append(ResourceChange(
                kind=op.kind, name=op.name, namespace=op.namespace, action="update",
                before=before, after={"replicas": op.replicas}))
            workloads += 1
            pods += max(int((before or {}).get("replicas", 0)), op.replicas or 0)
            if op.kind == "StatefulSet":
                touches.statefulset = True
                if before and self._is_tier0(before, op.namespace):
                    tier0 = True

        elif op.verb == "delete":
            targets = self._delete_targets(op)
            for (kind, ns, name, before) in targets:
                changes.append(ResourceChange(kind=kind, name=name, namespace=ns,
                                              action="delete", before=before,
                                              after=None))
                if kind == "Namespace":
                    namespaces_affected += 1
                    desired, _ = self._replica_counts(ns)
                    pods += desired
                    workloads += desired and 1 or 0
                    tier0 = tier0 or ns in TIER0_NS
                else:
                    if kind in WORKLOAD_KINDS:
                        workloads += 1
                        pods += int((before or {}).get("replicas", 0)
                                    or self._workload_pods(ns, ""))
                    if kind == "Service":
                        services += 1
                    flag = KIND_TOUCHES.get(kind)
                    if flag:
                        setattr(touches, flag, True)
                    tier0 = tier0 or self._is_tier0(before, ns)

        touches.tier0 = touches.tier0 or tier0
        dependents: list[str] = []
        for ch in changes:
            for d in dependents_of(f"{ch.namespace}/{ch.name}"):
                if d not in dependents:
                    dependents.append(d)
        raw = json.dumps([c.model_dump() for c in changes])
        return SimulateResult(ok=True, changes=changes, pods_affected=pods,
                              services_affected=services,
                              namespaces_affected=namespaces_affected,
                              workloads_affected=workloads, dependents=dependents,
                              touches=touches, raw=raw)

    def _delete_targets(self, op: K8sOp) -> list[tuple[str, str, str, Optional[dict]]]:
        if op.kind == "Namespace":
            names = [op.name] if op.name and op.name != "*" else \
                [n for n in ("shop", "payments", "dev-sandbox")]
            return [("Namespace", "", n, {"labels": {"ns": n}}) for n in names]
        if op.name:
            before = self.snapshot_resource(op.kind, op.namespace, op.name)
            return [(op.kind, op.namespace, op.name, before)]
        if op.label_selector:
            args = ["get", op.kind, "-n", op.namespace, "-l", op.label_selector,
                    "-o", "json"]
            out = self._kubectl(*args, check=False)
            if out.returncode != 0:
                return [(op.kind, op.namespace, "*", None)]
            items = json.loads(out.stdout).get("items", [])
            return [(op.kind, op.namespace, i["metadata"]["name"], self._compact(i))
                    for i in items]
        return [(op.kind, op.namespace, op.name or "*", None)]

    # ---- apply (unguarded arm only; guarded changes flow through GitOps) ----
    def apply(self, intent: ChangeIntent, parsed: ParsedChange) -> dict:
        op = K8sOp(**intent.op)
        applied = []
        if op.manifest or op.verb in ("create", "update", "patch"):
            self._kubectl("apply", "-f", "-", stdin=json.dumps(op.manifest or {}))
            applied.append(f"{op.kind or ''}/{op.namespace}/{op.name}".lstrip("/"))
        elif op.verb == "scale":
            self._kubectl("scale", op.kind, op.name, "-n", op.namespace,
                          f"--replicas={op.replicas}")
            applied.append(f"{op.kind}/{op.namespace}/{op.name}")
        elif op.verb == "delete":
            if op.kind == "Namespace":
                names = [op.name] if op.name and op.name != "*" else \
                    ("shop", "payments", "dev-sandbox")
                for n in names:
                    self._kubectl("delete", "ns", n, "--ignore-not-found=true",
                                  "--wait=false")
                    applied.append(f"Namespace//{n}")
            elif op.name:
                self._kubectl("delete", op.kind, op.name, "-n", op.namespace,
                              "--ignore-not-found=true")
                applied.append(f"{op.kind}/{op.namespace}/{op.name}")
            elif op.label_selector:
                self._kubectl("delete", op.kind, "-n", op.namespace,
                              "-l", op.label_selector, "--ignore-not-found=true")
                applied.append(f"{op.kind}/{op.namespace}/l={op.label_selector}")
        return {"applied": applied}

    # ---- health ----
    def probe(self, base_url: str | None = None) -> HealthSnapshot:
        base = (base_url or os.environ.get("PROBE_URL",
                                           "http://127.0.0.1:18080")).rstrip("/")
        errors = 0
        latencies: list[float] = []
        available = True
        try:
            for _ in range(4):
                t0 = time.perf_counter()
                r = httpx.get(f"{base}/healthz", timeout=3.0)
                latencies.append((time.perf_counter() - t0) * 1000)
                if r.status_code >= 400:
                    errors += 1
            if not latencies:
                available = False
        except Exception:
            available = False
        p95 = sorted(latencies)[int(0.95 * len(latencies))] if latencies else 999.0
        desired_shop, ready_shop = self._replica_counts("shop")
        desired_pay, ready_pay = self._replica_counts("payments")
        desired = max(desired_shop + desired_pay, 1)
        ready_ratio = (ready_shop + ready_pay) / desired
        restarts = self._pod_restarts("shop") + self._pod_restarts("payments")
        return HealthSnapshot(available=available, ready_ratio=round(ready_ratio, 3),
                              error_rate=round(errors / 4, 3) if available else 1.0,
                              p95_ms=round(p95, 1), restarts=restarts)
