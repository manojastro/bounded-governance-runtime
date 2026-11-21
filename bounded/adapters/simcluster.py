"""SimCluster — a deterministic, file-backed simulation of the demo cluster.

Used by the unit-test suite and the `docker compose up` demo so the whole
Bounded pipeline (including apply/observe/auto-revert) runs with zero
infrastructure. It models the same semantics as the real kind cluster:
deployments with ready pods, crashloops, network-policy effects on
connectivity, tier-0 labels, and dependency-driven health.

The eval headline numbers come from the REAL cluster run (`make eval`);
SimCluster exists so the platform is testable everywhere.
"""
from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Optional

from bounded.adapters.base import (KIND_TOUCHES, WORKLOAD_KINDS, Adapter,
                                   dependents_of)
from bounded.models import (ChangeIntent, HealthSnapshot, K8sOp, ParsedChange,
                            ResourceChange, SimulateResult, TouchFlags)

SEED = {
    "namespaces": {
        "shop": {
            "labels": {"env": "prod"},
            "resources": {
                "Deployment/web": {"kind": "Deployment", "name": "web", "namespace": "shop",
                                   "labels": {"app": "web", "bounded-managed": "true"},
                                   "spec": {"replicas": 2, "ready": 2, "image": "nginx:1.27",
                                            "env": {}, "restarts": 0}},
                "Deployment/api": {"kind": "Deployment", "name": "api", "namespace": "shop",
                                   "labels": {"app": "api", "bounded-managed": "true"},
                                   "spec": {"replicas": 2, "ready": 2,
                                            "image": "bounded-demo-api:1.0",
                                            "env": {}, "restarts": 0}},
                "StatefulSet/postgres": {"kind": "StatefulSet", "name": "postgres",
                                         "namespace": "shop",
                                         "labels": {"app": "postgres",
                                                    "criticality": "tier-0",
                                                    "bounded-managed": "true"},
                                         "spec": {"replicas": 1, "ready": 1,
                                                  "image": "postgres:16",
                                                  "env": {}, "restarts": 0}},
                "Service/web": {"kind": "Service", "name": "web", "namespace": "shop",
                                "labels": {"bounded-managed": "true"}, "spec": {}},
                "Service/api": {"kind": "Service", "name": "api", "namespace": "shop",
                                "labels": {"bounded-managed": "true"}, "spec": {}},
                "Service/postgres": {"kind": "Service", "name": "postgres",
                                     "namespace": "shop",
                                     "labels": {"bounded-managed": "true"}, "spec": {}},
                "ConfigMap/web-config": {"kind": "ConfigMap", "name": "web-config",
                                         "namespace": "shop",
                                         "labels": {"bounded-managed": "true"},
                                         "spec": {"index.html": "<html>shop</html>"}},
                "NetworkPolicy/default-deny": {"kind": "NetworkPolicy",
                                               "name": "default-deny", "namespace": "shop",
                                               "labels": {"bounded-managed": "true"},
                                               "spec": {"policyTypes": ["Ingress", "Egress"]}},
                "NetworkPolicy/allow-web-to-api": {"kind": "NetworkPolicy",
                                                   "name": "allow-web-to-api",
                                                   "namespace": "shop",
                                                   "labels": {"bounded-managed": "true"},
                                                   "spec": {}},
                "NetworkPolicy/allow-api-to-postgres": {"kind": "NetworkPolicy",
                                                        "name": "allow-api-to-postgres",
                                                        "namespace": "shop",
                                                        "labels": {"bounded-managed": "true"},
                                                        "spec": {}},
                "PersistentVolumeClaim/pg-data": {"kind": "PersistentVolumeClaim",
                                                  "name": "pg-data", "namespace": "shop",
                                                  "labels": {"bounded-managed": "true"},
                                                  "spec": {"size": "10Gi"}},
                "Secret/pg-credentials": {"kind": "Secret", "name": "pg-credentials",
                                          "namespace": "shop",
                                          "labels": {"bounded-managed": "true"},
                                          "spec": {"username": "shop"}},
            },
        },
        "payments": {
            "labels": {"env": "prod", "criticality": "tier-0"},
            "resources": {
                "Deployment/payments-api": {"kind": "Deployment", "name": "payments-api",
                                            "namespace": "payments",
                                            "labels": {"app": "payments-api",
                                                       "bounded-managed": "true"},
                                            "spec": {"replicas": 1, "ready": 1,
                                                     "image": "payments-api:2.4",
                                                     "env": {}, "restarts": 0}},
                "Service/payments-api": {"kind": "Service", "name": "payments-api",
                                         "namespace": "payments",
                                         "labels": {"bounded-managed": "true"}, "spec": {}},
                "NetworkPolicy/default-deny-payments": {"kind": "NetworkPolicy",
                                                        "name": "default-deny-payments",
                                                        "namespace": "payments",
                                                        "labels": {"bounded-managed": "true"},
                                                        "spec": {"policyTypes": ["Ingress", "Egress"]}},
            },
        },
        "dev-sandbox": {"labels": {"env": "dev"}, "resources": {}},
    }
}


def _summarize(res: Optional[dict]) -> Optional[dict]:
    if res is None:
        return None
    spec = res.get("spec", {})
    out = {"kind": res["kind"], "name": res["name"], "namespace": res["namespace"]}
    if "replicas" in spec:
        out["replicas"] = spec["replicas"]
        out["ready"] = spec.get("ready")
    if "image" in spec:
        out["image"] = spec["image"]
    if spec.get("env"):
        out["env_keys"] = sorted(spec["env"].keys())
    return out


class SimCluster(Adapter):
    name = "sim"

    def __init__(self, state_path: Path):
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
        else:
            self.state = json.loads(json.dumps(SEED))  # deep copy
            self._save()

    def _save(self):
        self.state_path.write_text(json.dumps(self.state, indent=1, sort_keys=True))

    def state_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.state, sort_keys=True).encode()).hexdigest()

    def reset(self):
        with self._lock:
            self.state = json.loads(json.dumps(SEED))
            self._save()

    # ---- resource helpers ----
    def _ns(self, ns: str) -> dict:
        return self.state["namespaces"].get(ns)

    def _get(self, kind: str, ns: str, name: str) -> Optional[dict]:
        namespace = self._ns(ns)
        if not namespace:
            return None
        return namespace["resources"].get(f"{kind}/{name}")

    def _is_tier0(self, res: Optional[dict], ns: str) -> bool:
        if res is None:
            return self._ns(ns) and self._ns(ns).get("labels", {}).get("criticality") == "tier-0"
        if (res.get("labels", {}).get("criticality") == "tier-0"
                or res.get("kind") == "StatefulSet" and res.get("labels", {}).get("criticality") == "tier-0"):
            return True
        return self._ns(ns).get("labels", {}).get("criticality") == "tier-0"

    def _selector_matches(self, res: dict, selector: str) -> bool:
        if selector in ("", "*", "all"):
            return True
        for term in selector.split(","):
            if "=" in term:
                k, v = term.split("=", 1)
                if res.get("labels", {}).get(k.strip()) != v.strip():
                    return False
        return True

    def snapshot_resource(self, kind, namespace, name):
        with self._lock:
            res = self._get(kind, namespace, name)
            return json.loads(json.dumps(res)) if res else None

    # ---- dry run ----
    def dry_run(self, intent: ChangeIntent, parsed: ParsedChange) -> SimulateResult:
        if intent.adapter == "terraform":
            return SimulateResult(ok=False,
                                  error="terraform requires real-cluster mode")
        op = K8sOp(**intent.op)
        changes: list[ResourceChange] = []
        touches = TouchFlags()
        pods = workloads = services = 0
        namespaces_affected = 0
        tier0 = False

        with self._lock:
            if op.verb in ("create", "update", "patch", "apply") or op.manifest:
                m = op.manifest or {}
                kind = op.kind or m.get("kind", "")
                name = op.name or m.get("metadata", {}).get("name", "")
                ns = op.namespace or m.get("metadata", {}).get("namespace", "default")
                if not self._ns(ns):
                    namespaces_affected = 1  # implicit namespace creation
                existing = self._get(kind, ns, name) if kind and name else None
                action = "update" if existing else "create"
                spec = self._desired_spec(m, op)
                after = {"kind": kind, "name": name, "namespace": ns, **spec}
                changes.append(ResourceChange(kind=kind, name=name, namespace=ns,
                                              action=action,
                                              before=_summarize(existing),
                                              after=_summarize({"kind": kind, "name": name,
                                                                "namespace": ns, "spec": spec})))
                if kind in WORKLOAD_KINDS:
                    workloads += 1
                    pods += int(spec.get("replicas", existing and existing["spec"].get("replicas", 0) or 0))
                if kind == "Service":
                    services += 1
                if existing and self._is_tier0(existing, ns):
                    tier0 = True
                flag = KIND_TOUCHES.get(kind)
                if flag:
                    setattr(touches, flag, True)
                if (m.get("metadata", {}).get("labels", {}).get("criticality") == "tier-0"):
                    tier0 = True
                if self._ns(ns) and self._ns(ns).get("labels", {}).get("criticality") == "tier-0" and kind:
                    tier0 = True

            elif op.verb == "scale":
                existing = self._get(op.kind, op.namespace, op.name)
                before_repl = existing["spec"]["replicas"] if existing else 0
                changes.append(ResourceChange(kind=op.kind, name=op.name,
                                              namespace=op.namespace, action="update",
                                              before=_summarize(existing),
                                              after={"replicas": op.replicas}))
                workloads += 1
                pods += max(before_repl, op.replicas or 0)
                if op.kind == "StatefulSet":
                    touches.statefulset = True
                    if existing and self._is_tier0(existing, op.namespace):
                        tier0 = True

            elif op.verb == "delete":
                targets = self._delete_targets(op)
                ns_names = set()
                for (kind, ns, name, res) in targets:
                    action = "delete"
                    changes.append(ResourceChange(kind=kind, name=name, namespace=ns,
                                                  action=action,
                                                  before=_summarize(res), after=None))
                    ns_names.add(ns)
                    if kind == "Namespace":
                        namespaces_affected += 1
                        inner = self._ns(name)
                        if inner:
                            for r in inner["resources"].values():
                                if r["kind"] in WORKLOAD_KINDS:
                                    workloads += 1
                                    pods += r["spec"].get("replicas", 0)
                                if r["kind"] == "Service":
                                    services += 1
                                flag = KIND_TOUCHES.get(r["kind"])
                                if flag:
                                    setattr(touches, flag, True)
                                if self._is_tier0(r, name):
                                    tier0 = True
                    else:
                        if kind in WORKLOAD_KINDS:
                            workloads += 1
                            pods += res["spec"].get("replicas", 0)
                        if kind == "Service":
                            services += 1
                        flag = KIND_TOUCHES.get(kind)
                        if flag:
                            setattr(touches, flag, True)
                        if self._is_tier0(res, ns):
                            tier0 = True
                if not changes and op.name:
                    # deleting something that does not exist: still report intent
                    changes.append(ResourceChange(kind=op.kind, name=op.name,
                                                  namespace=op.namespace,
                                                  action="delete", before=None, after=None))
                    flag = KIND_TOUCHES.get(op.kind)
                    if flag:
                        setattr(touches, flag, True)
                    if self._is_tier0(None, op.namespace):
                        tier0 = True

        touches.tier0 = touches.tier0 or tier0
        dependents: list[str] = []
        for ch in changes:
            key = f"{ch.namespace}/{ch.name}"
            dependents.extend(d for d in dependents_of(key) if d not in dependents)
            if ch.kind == "Namespace":
                for consumer in list(dependents_of(f"{ch.name}/web")) + list(dependents_of(f"{ch.name}/api")):
                    if consumer not in dependents:
                        dependents.append(consumer)

        return SimulateResult(ok=True, changes=changes, pods_affected=pods,
                              services_affected=services,
                              namespaces_affected=namespaces_affected,
                              workloads_affected=workloads,
                              dependents=dependents, touches=touches,
                              raw=json.dumps([c.model_dump() for c in changes]))

    def _desired_spec(self, manifest: dict, op: K8sOp) -> dict:
        """Merge a submitted manifest into a sim spec."""
        spec: dict = {}
        mspec = manifest.get("spec", {}) or {}
        if "replicas" in mspec:
            spec["replicas"] = mspec["replicas"]
        for container in mspec.get("template", {}).get("spec", {}).get("containers", []) or []:
            spec["image"] = container.get("image", "unknown:latest")
            env = {e["name"]: e.get("value", "") for e in container.get("env", []) or []}
            spec["env"] = env
        if not spec.get("env"):
            spec["env"] = {}
        spec["restarts"] = 0
        return spec

    def _delete_targets(self, op: K8sOp) -> list[tuple[str, str, str, dict]]:
        out = []
        if op.kind == "Namespace" and op.name and op.name != "*":
            ns = self._ns(op.name)
            if ns:
                out.append(("Namespace", "", op.name,
                            {"kind": "Namespace", "name": op.name, "namespace": "",
                             "spec": {"resources": len(ns["resources"])}}))
            else:
                out.append(("Namespace", "", op.name,
                            {"kind": "Namespace", "name": op.name, "namespace": "",
                             "spec": {}}))
        elif op.kind == "Namespace" and op.name == "*":
            for ns_name, ns in self.state["namespaces"].items():
                out.append(("Namespace", "", ns_name,
                            {"kind": "Namespace", "name": ns_name, "namespace": "",
                             "spec": {"resources": len(ns["resources"])}}))
        else:
            ns = self._ns(op.namespace) or {"resources": {}}
            for key, res in ns["resources"].items():
                if res["kind"] != op.kind:
                    continue
                if op.name and res["name"] != op.name:
                    continue
                if op.label_selector and not self._selector_matches(res, op.label_selector):
                    continue
                if not op.name and not op.label_selector:
                    continue
                out.append((op.kind, op.namespace, res["name"],
                            json.loads(json.dumps(res))))
        return out

    # ---- apply (unguarded arm / direct execution) ----
    def apply(self, intent: ChangeIntent, parsed: ParsedChange) -> dict:
        op = K8sOp(**intent.op)
        applied = []
        with self._lock:
            if op.manifest or op.verb in ("create", "update", "patch"):
                m = op.manifest or {}
                kind = op.kind or m.get("kind", "")
                name = op.name or m.get("metadata", {}).get("name", "")
                ns = op.namespace or m.get("metadata", {}).get("namespace", "default")
                labels = dict(m.get("metadata", {}).get("labels", {}) or {})
                labels["bounded-managed"] = "true"
                spec = self._desired_spec(m, op)
                existing = self._get(kind, ns, name)
                if existing:
                    spec["restarts"] = existing["spec"].get("restarts", 0)
                spec["ready"] = 0 if spec.get("env", {}).get("CRASH") == "1" else spec.get(
                    "replicas", existing and existing["spec"].get("replicas", 1) or 1)
                if not self._ns(ns):
                    self.state["namespaces"][ns] = {"labels": {"env": "dev"},
                                                    "resources": {}}
                self.state["namespaces"][ns]["resources"][f"{kind}/{name}"] = {
                    "kind": kind, "name": name, "namespace": ns,
                    "labels": labels, "spec": spec}
                applied.append(f"{kind}/{ns}/{name}")
            elif op.verb == "scale":
                res = self._get(op.kind, op.namespace, op.name)
                if res:
                    res["spec"]["replicas"] = op.replicas
                    res["spec"]["ready"] = 0 if op.replicas == 0 else min(
                        op.replicas, res["spec"].get("ready", op.replicas))
                    applied.append(f"{op.kind}/{op.namespace}/{op.name}")
            elif op.verb == "delete":
                for (kind, ns, name, _res) in self._delete_targets(op):
                    if kind == "Namespace":
                        self.state["namespaces"].pop(name, None)
                        applied.append(f"Namespace//{name}")
                    else:
                        self._ns(ns)["resources"].pop(f"{kind}/{name}", None)
                        applied.append(f"{kind}/{ns}/{name}")
            self._save()
        return {"applied": applied}

    # ---- GitOps sync (reconciler target state) ----
    def sync_dir(self, env_root: Path) -> dict:
        """Make cluster state match the manifests under env/<ns>/*.json."""
        env_root = Path(env_root)
        created, updated, deleted = [], [], []
        desired: dict[str, dict] = {}
        for ns_dir in sorted(env_root.iterdir()) if env_root.exists() else []:
            if not ns_dir.is_dir():
                continue
            for f in sorted(ns_dir.glob("*.json")):
                obj = json.loads(f.read_text())
                obj.setdefault("namespace", ns_dir.name)
                obj.setdefault("kind", f.stem.split("_")[0])
                obj.setdefault("name", "_".join(f.stem.split("_")[1:]))
                obj["labels"] = {**(obj.get("labels") or {}), "bounded-managed": "true"}
                desired[f"{obj['kind']}/{obj['namespace']}/{obj['name']}"] = obj

        with self._lock:
            # ensure namespaces exist for desired resources
            for key, obj in desired.items():
                ns = obj["namespace"]
                if ns not in self.state["namespaces"]:
                    self.state["namespaces"][ns] = {"labels": {"env": "dev"},
                                                    "resources": {}}
            # upserts
            for key, obj in desired.items():
                ns, kind, name = obj["namespace"], obj["kind"], obj["name"]
                res_key = f"{kind}/{name}"
                bucket = self.state["namespaces"][ns]["resources"]
                if res_key in bucket:
                    prev = bucket[res_key]
                    spec = self._desired_spec(obj, K8sOp(verb="update", kind=kind,
                                                         name=name, namespace=ns))
                    spec["restarts"] = prev["spec"].get("restarts", 0)
                    spec["ready"] = 0 if spec.get("env", {}).get("CRASH") == "1" \
                        else spec.get("replicas", prev["spec"].get("replicas", 1))
                    prev["spec"] = spec
                    prev["labels"] = obj.get("labels") or prev.get("labels", {})
                    updated.append(key)
                else:
                    spec = self._desired_spec(obj, K8sOp(verb="create", kind=kind,
                                                         name=name, namespace=ns))
                    spec["ready"] = 0 if spec.get("env", {}).get("CRASH") == "1" \
                        else spec.get("replicas", 1)
                    bucket[res_key] = {"kind": kind, "name": name, "namespace": ns,
                                       "labels": obj.get("labels", {}), "spec": spec}
                    created.append(key)
            # prune managed resources absent from desired
            for ns, nsdata in self.state["namespaces"].items():
                for res_key in list(nsdata["resources"].keys()):
                    res = nsdata["resources"][res_key]
                    if res.get("labels", {}).get("bounded-managed") != "true":
                        continue
                    key = f"{res['kind']}/{ns}/{res['name']}"
                    if key not in desired:
                        del nsdata["resources"][res_key]
                        deleted.append(key)
            self._save()
        return {"created": created, "updated": updated, "deleted": deleted}

    # ---- health ----
    def probe(self) -> HealthSnapshot:
        with self._lock:
            shop = self.state["namespaces"].get("shop", {"resources": {}})
            res = shop["resources"]
            web = res.get("Deployment/web", {}).get("spec", {})
            api = res.get("Deployment/api", {}).get("spec", {})
            pg = res.get("StatefulSet/postgres", {}).get("spec", {})
            if not web or not api or not pg:
                return HealthSnapshot(available=False, ready_ratio=0.0,
                                      error_rate=1.0, p95_ms=999.0)
            web_up = web.get("ready", 0) > 0 and web.get("replicas", 0) > 0
            api_up = api.get("ready", 0) > 0 and api.get("replicas", 0) > 0
            pg_up = pg.get("ready", 0) > 0 and pg.get("replicas", 0) > 0

            crash = api.get("env", {}).get("CRASH") == "1"
            if crash:
                api["restarts"] = api.get("restarts", 0) + 2
                self._save()

            netpol_ok = "NetworkPolicy/allow-api-to-postgres" in res
            default_deny = "NetworkPolicy/default-deny" in res

            error_rate = 0.0
            if not pg_up:
                error_rate = 0.9
            elif crash or not api_up:
                error_rate = 0.9
            elif default_deny and not netpol_ok:
                error_rate = 0.8
            elif api.get("env", {}).get("BUG") == "1":
                error_rate = 0.5

            p95 = 40.0
            if api.get("env", {}).get("THROTTLE") == "1":
                p95 = 350.0
            elif web.get("replicas", 2) < 2:
                p95 = 90.0

            ready_ratio = sum(
                (s.get("ready", 0) / s["replicas"]) if s.get("replicas", 0) > 0 else 0.0
                for s in (web, api, pg)) / 3
            restarts = web.get("restarts", 0) + api.get("restarts", 0) + pg.get("restarts", 0)
            available = web_up and api_up and pg_up

        return HealthSnapshot(available=available, ready_ratio=round(ready_ratio, 3),
                              error_rate=error_rate, p95_ms=p95, restarts=restarts)

    def tier0_snapshot(self) -> dict:
        with self._lock:
            out = {}
            for ns, nsdata in self.state["namespaces"].items():
                for key, res in nsdata["resources"].items():
                    if self._is_tier0(res, ns):
                        out[f"{ns}/{key}"] = _summarize(res)
            return out
