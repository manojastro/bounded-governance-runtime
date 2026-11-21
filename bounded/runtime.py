"""Runtime wiring — one place that assembles adapters, repo, reconciler
and the Bounded pipeline for either mode:

  sim  — SimCluster (deterministic file-backed cluster); no infra needed
  real — kind cluster via kubectl, plus terraform when the binary exists
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from bounded.adapters.base import Adapter
from bounded.adapters.simcluster import SEED, SimCluster
from bounded.audit import AuditChain
from bounded.gitops import GitRepo
from bounded.identity import Identity
from bounded.llm import get_llm
from bounded.policy.engine import PolicyEngine
from bounded.store import Store

RUNTIME_DIR = Path(os.environ.get("BOUNDED_RUNTIME", "runtime"))
ENV_DIR_NAME = "env"


def rm_rf(path: Path):
    """Windows-safe recursive delete (git objects are read-only)."""
    import stat

    def _onerror(fn, p, exc):
        os.chmod(p, stat.S_IWRITE)
        fn(p)

    if path.exists():
        shutil.rmtree(path, onerror=_onerror)


@dataclass
class Runtime:
    mode: str
    store: Store
    audit: AuditChain
    identity: Identity
    policy: PolicyEngine
    repo: GitRepo
    adapters: dict
    reconciler: "Reconciler"
    llm: object = field(default_factory=get_llm)

    def adapter(self, name: str) -> Optional[Adapter]:
        return self.adapters.get(name)

    def probe(self):
        probe_adapter = self.adapters.get("kubernetes") or next(iter(self.adapters.values()))
        return probe_adapter.probe()


def sim_resource_to_manifest(res: dict) -> dict:
    """Convert a SimCluster resource into a k8s-shaped manifest."""
    kind, name, ns = res["kind"], res["name"], res["namespace"]
    labels = dict(res.get("labels", {}))
    spec = res.get("spec", {})
    if kind in ("Deployment", "StatefulSet"):
        out_spec = {
            "replicas": spec.get("replicas", 1),
            "template": {
                "metadata": {"labels": {"app": labels.get("app", name)}},
                "spec": {"containers": [{
                    "name": name,
                    "image": spec.get("image", "unknown:latest"),
                    "env": [{"name": k, "value": v}
                            for k, v in (spec.get("env") or {}).items()],
                }]},
            },
        }
    else:
        out_spec = {}
    return {
        "apiVersion": "apps/v1" if kind in ("Deployment", "StatefulSet") else "v1",
        "kind": kind,
        "metadata": {"name": name, "namespace": ns, "labels": labels},
        "spec": out_spec,
    }


def seed_env(repo: GitRepo, mode: str):
    """Write the initial desired-state files into the GitOps repo."""
    env_root = repo.path / ENV_DIR_NAME
    if env_root.exists() and any(env_root.iterdir()):
        return
    if mode == "sim":
        for ns, nsdata in SEED["namespaces"].items():
            ns_dir = env_root / ns
            ns_dir.mkdir(parents=True, exist_ok=True)
            for res in nsdata["resources"].values():
                m = sim_resource_to_manifest(res)
                (ns_dir / f"{res['kind']}_{res['name']}.json").write_text(
                    json.dumps(m, indent=1))
    else:
        import yaml
        manifests_root = Path("services/demo-app/manifests")
        for f in sorted(manifests_root.glob("*.yaml")):
            for doc in yaml.safe_load_all(f.read_text()):
                if not doc:
                    continue
                md = doc.get("metadata", {})
                ns = md.get("namespace", "default")
                ns_dir = env_root / ns
                ns_dir.mkdir(parents=True, exist_ok=True)
                (ns_dir / f"{doc['kind']}_{md['name']}.json").write_text(
                    json.dumps(doc, indent=1))
    repo._run("git", "add", "-A")
    repo._commit("seed desired-state environment")


class Reconciler:
    """Mock Argo-CD: watches the GitOps repo main branch and syncs it
    to the cluster (SimCluster.sync_dir in sim mode, kubectl/terraform
    in real mode)."""

    def __init__(self, repo: GitRepo, adapters: dict, mode: str,
                 state_file: Path):
        self.repo = repo
        self.adapters = adapters
        self.mode = mode
        self.state_file = Path(state_file)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _state(self) -> dict:
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text())
            except Exception:
                pass
        return {"last_synced": "", "history": []}

    def _write_state(self, head: str, result: dict):
        st = self._state()
        st["history"].append({"sha": head, "ts": time.time(), "result": result})
        st["history"] = st["history"][-50:]
        st["last_synced"] = head
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(st, indent=1))

    def sync(self, force: bool = False) -> dict:
        with self._lock:
            head = self.repo.head()
            st = self._state()
            if head == st.get("last_synced") and not force:
                return {"noop": True, "sha": head}
            result = self._apply_all()
            self._write_state(head, result)
            return result

    def _apply_all(self) -> dict:
        detail = {}
        if self.mode == "sim":
            sim = self.adapters.get("kubernetes")
            detail = sim.sync_dir(self.repo.path / ENV_DIR_NAME) if sim else {}
        else:
            k8s = self.adapters.get("kubernetes")
            if k8s:
                env_root = self.repo.path / ENV_DIR_NAME
                for ns_dir in sorted(p for p in env_root.iterdir() if p.is_dir()) \
                        if env_root.exists() else []:
                    out = k8s._kubectl("apply", "-f", str(ns_dir),
                                       "--prune", "-l", "bounded-managed=true",
                                       check=False)
                    detail[ns_dir.name] = (out.stdout + out.stderr).strip()[-300:]
            tf = self.adapters.get("terraform")
            if tf:
                try:
                    detail["terraform"] = tf.apply_live_from_repo()
                except Exception as e:
                    detail["terraform_error"] = str(e)[:300]
        return detail

    def wait_synced(self, sha: str, timeout_s: float = 40.0) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self._state().get("last_synced") == sha:
                return True
            time.sleep(0.5)
        return False

    def start(self, interval_s: float = 2.0):
        if self._thread and self._thread.is_alive():
            return

        def loop():
            while not self._stop.is_set():
                try:
                    self.sync()
                except Exception as e:
                    st = self._state()
                    st["last_error"] = str(e)[:300]
                    self.state_file.parent.mkdir(parents=True, exist_ok=True)
                    self.state_file.write_text(json.dumps(st, indent=1))
                time.sleep(interval_s)

        self._thread = threading.Thread(target=loop, daemon=True,
                                        name="bounded-reconciler")
        self._thread.start()

    def stop(self):
        self._stop.set()


def build_runtime(mode: str | None = None, fresh: bool = False) -> Runtime:
    mode = (mode or os.environ.get("BOUNDED_MODE", "sim")).lower()
    root = RUNTIME_DIR
    if fresh and root.exists():
        rm_rf(root)
    root.mkdir(parents=True, exist_ok=True)

    store = Store(root / "bounded.db")
    audit = AuditChain(root / "bounded.db")
    identity = Identity()
    policy = PolicyEngine()
    repo = GitRepo(root / "gitops")
    seed_env(repo, mode)

    adapters: dict = {}
    if mode == "sim":
        adapters["kubernetes"] = SimCluster(root / "simcluster.json")
    else:
        from bounded.adapters.kubernetes import K8sAdapter
        adapters["kubernetes"] = K8sAdapter()
        from bounded.adapters.terraform import TerraformAdapter
        if TerraformAdapter.available():
            adapters["terraform"] = TerraformAdapter(
                repo_tf_dir=repo.path / "tf",
                workroot=root / "tfwork")
    reconciler = Reconciler(repo, adapters, mode, root / "reconciler_state.json")
    rt = Runtime(mode=mode, store=store, audit=audit, identity=identity,
                 policy=policy, repo=repo, adapters=adapters,
                 reconciler=reconciler)
    from bounded.pipeline import Bounded
    rt.bounded = Bounded(rt)
    rt.reconciler.start()
    return rt
