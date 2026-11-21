"""Terraform adapter — same contract as Kubernetes, backed by
`terraform plan` diffs against the local kind cluster."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from bounded.adapters.base import Adapter
from bounded.models import (ChangeIntent, ParsedChange, ResourceChange,
                            SimulateResult, TouchFlags)

TF_TYPE_KIND = {
    "kubernetes_deployment": "Deployment",
    "kubernetes_stateful_set": "StatefulSet",
    "kubernetes_service": "Service",
    "kubernetes_config_map": "ConfigMap",
    "kubernetes_network_policy": "NetworkPolicy",
    "kubernetes_secret": "Secret",
    "kubernetes_persistent_volume_claim": "PersistentVolumeClaim",
    "kubernetes_role": "Role",
    "kubernetes_role_binding": "RoleBinding",
}

TF_TYPE_TOUCH = {
    "kubernetes_network_policy": "network_policy",
    "kubernetes_role": "rbac",
    "kubernetes_role_binding": "rbac",
    "kubernetes_secret": "secrets",
    "kubernetes_persistent_volume_claim": "storage",
    "kubernetes_stateful_set": "statefulset",
}


class TerraformAdapter(Adapter):
    name = "terraform"

    def __init__(self, repo_tf_dir: Path, workroot: Path,
                 kubeconfig: str | None = None):
        self.tf = self._find_binary()
        self.repo_tf_dir = Path(repo_tf_dir)
        self.workroot = Path(workroot)
        self.workroot.mkdir(parents=True, exist_ok=True)
        self.kubeconfig = kubeconfig or os.environ.get("KUBECONFIG", "")
        self._initialized = False

    @staticmethod
    def available() -> bool:
        return bool(TerraformAdapter._find_binary())

    @staticmethod
    def _find_binary() -> Optional[str]:
        env = os.environ.get("TERRAFORM")
        if env and Path(env).exists():
            return env
        local = Path(".tools/terraform/terraform.exe")
        if not local.exists():
            local = Path(".tools/terraform/terraform")
        if local.exists():
            return str(local)
        return shutil.which("terraform")

    def _workdir(self, intent_id: str, live: bool = False) -> Path:
        d = self.workroot / (intent_id if not live else "live")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _stage(self, intent: ChangeIntent, live: bool = False) -> Path:
        """Copy the repo's tf tree + the intent's files into a workdir."""
        wd = self._workdir(intent.id, live=live)
        if self.repo_tf_dir.exists():
            for f in self.repo_tf_dir.glob("*.tf"):
                shutil.copy(f, wd / f.name)
        else:
            (wd / "providers.tf").write_text(
                'provider "kubernetes" {\n'
                f'  config_path = "{self.kubeconfig.replace(os.sep, "/")}"\n'
                '}\n')
        for rel, content in intent.op.get("files", {}).items():
            target = wd / Path(rel).name
            target.write_text(content)
        return wd

    def _run(self, *args: str, cwd: Path) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env |= {"TF_IN_AUTOMATION": "1", "TF_INPUT": "0",
                "TF_PLUGIN_CACHE_DIR": str(self.workroot / "_plugin_cache")}
        out = subprocess.run([self.tf, *args], cwd=cwd, capture_output=True,
                             text=True, shell=False, timeout=300, env=env)
        if out.returncode != 0:
            raise RuntimeError(f"terraform {' '.join(args)} failed: "
                               f"{(out.stderr or out.stdout)[-600:]}")
        return out

    def _ensure_init(self, wd: Path):
        if not (wd / ".terraform").exists():
            self._run("init", "-backend=false", cwd=wd)

    def dry_run(self, intent: ChangeIntent, parsed: ParsedChange) -> SimulateResult:
        wd = self._stage(intent)
        self._ensure_init(wd)
        self._run("plan", "-out=tfplan", "-detailed-exitcode", cwd=wd)
        out = self._run("show", "-json", "tfplan", cwd=wd)
        plan = json.loads(out.stdout)
        changes: list[ResourceChange] = []
        touches = TouchFlags()
        pods = workloads = 0
        tier0 = False
        for rc in plan.get("resource_changes", []) or []:
            rtype = rc.get("type", "")
            rname = rc.get("name", "")
            kind = TF_TYPE_KIND.get(rtype, rtype)
            actions = rc.get("change", {}).get("actions", [])
            action = ("delete" if "delete" in actions
                      else "update" if "update" in actions
                      else "create" if "create" in actions else "none")
            after = rc.get("change", {}).get("after", {}) or {}
            before = rc.get("change", {}).get("before", {}) or {}
            ns = (after or before).get("metadata", [{}])[0].get("namespace", "") \
                if isinstance((after or before).get("metadata"), list) \
                and (after or before).get("metadata") else \
                ((after or before).get("metadata", {}) or {}).get("namespace", "")
            compact = {"kind": kind, "name": rname}
            rep = after.get("spec", [{}])[0].get("replicas") \
                if isinstance(after.get("spec"), list) and after.get("spec") \
                else (after.get("spec", {}) or {}).get("replicas")
            if rep is not None:
                compact["replicas"] = rep
            changes.append(ResourceChange(kind=kind, name=rname, namespace=ns,
                                          action=action,
                                          before=None if action == "create" else {"kind": kind},
                                          after=None if action == "delete" else compact))
            if kind in ("Deployment", "StatefulSet"):
                workloads += 1
                pods += int(rep or 0)
            flag = TF_TYPE_TOUCH.get(rtype)
            if flag:
                setattr(touches, flag, True)
            if ns == "payments":
                tier0 = True
        touches.tier0 = touches.tier0 or tier0
        namespaces = sorted({c.namespace for c in changes if c.namespace})
        return SimulateResult(
            ok=True, changes=changes, pods_affected=pods,
            workloads_affected=workloads,
            namespaces_affected=0, touches=touches,
            dependents=[],
            raw=json.dumps({"format_version": plan.get("format_version"),
                            "resource_changes": len(plan.get("resource_changes", []))}),
        )

    def apply(self, intent: ChangeIntent, parsed: ParsedChange) -> dict:
        wd = self._stage(intent)
        self._ensure_init(wd)
        self._run("plan", "-out=tfplan", cwd=wd)
        self._run("apply", "tfplan", cwd=wd)
        return {"applied": [f"terraform:{k}" for k in intent.op.get("files", {})]}

    def apply_live_from_repo(self) -> dict:
        """Used by the reconciler after tf files change on main."""
        wd = self._stage(ChangeIntent(id="live", agent_id="reconciler",
                                      submitted_at=__import__("datetime").datetime.now(
                                          __import__("datetime").timezone.utc),
                                      description="sync", adapter="terraform", op={}),
                         live=True)
        self._ensure_init(wd)
        self._run("apply", "-auto-approve", cwd=wd)
        return {"applied": ["terraform:live"]}

    def snapshot_resource(self, kind, namespace, name) -> Optional[dict]:
        return None  # tf state diffs cover this; not needed by the pipeline
