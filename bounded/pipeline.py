"""The Bounded pipeline as a LangGraph state machine.

  PARSE → SIMULATE → SCORE → POLICY → ROUTE → {APPLY | OPEN_PR | REJECT}
  APPLY → OBSERVE (auto-revert on degradation)

Agents submit intents; they never touch the cluster. Every stage writes
a trace row (for the dashboard) and an append-only audit row.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Optional, TypedDict

from langgraph.graph import END, StateGraph

from bounded.gitops import GitRepo
from bounded.llm import get_llm
from bounded.models import (ChangeIntent, K8sOp, PRRecord, ParsedChange,
                            PolicyDecision, RouteDecision, ScoreResult,
                            SimulateResult, Verdict, now_utc)
from bounded.observe import Observer
from bounded.parse import parse_intent
from bounded.policy.engine import build_policy_input
from bounded.route import route_decision
from bounded.score import score_change


class BState(TypedDict, total=False):
    intent: dict
    parsed: Optional[dict]
    sim: Optional[dict]
    score: Optional[dict]
    policy: Optional[dict]
    route: str
    status: str
    pr: Optional[dict]
    explanation: str
    error: str


class Bounded:
    def __init__(self, rt, background_observe: bool | None = None,
                 observe_window_s: float | None = None,
                 observe_interval_s: float | None = None):
        self.rt = rt
        self.llm = get_llm()
        sim = rt.mode == "sim"
        self.observe_window_s = observe_window_s if observe_window_s is not None \
            else float(os.environ.get("BOUNDED_OBSERVE_WINDOW", 6 if sim else 25))
        self.observe_interval_s = observe_interval_s if observe_interval_s is not None \
            else float(os.environ.get("BOUNDED_OBSERVE_INTERVAL", 0.4 if sim else 2.0))
        self.background_observe = (background_observe if background_observe is not None
                                   else os.environ.get("BOUNDED_BACKGROUND_OBSERVE",
                                                       "1") == "1")
        self._observer_threads: list[threading.Thread] = []
        self._intent_meta: dict[str, dict] = {}   # label/expected/degrade for eval
        self.graph = self._build_graph()

    # ------------------------------------------------------------------ graph
    def _build_graph(self):
        g = StateGraph(BState)
        g.add_node("parse", self._n_parse)
        g.add_node("simulate", self._n_simulate)
        g.add_node("score", self._n_score)
        g.add_node("policy", self._n_policy)
        g.add_node("route", self._n_route)
        g.add_node("apply_auto", self._n_apply_auto)
        g.add_node("open_pr", self._n_open_pr)
        g.add_node("reject", self._n_reject)
        g.add_node("observe", self._n_observe)

        g.set_entry_point("parse")
        g.add_edge("parse", "simulate")
        g.add_edge("simulate", "score")
        g.add_edge("score", "policy")
        g.add_edge("policy", "route")
        g.add_conditional_edges(
            "route",
            lambda s: ("reject" if s.get("status") in ("invalid", "error")
                       else s.get("route", "reject")),
            {"apply_auto": "apply_auto", "approval": "open_pr",
             "reject": "reject"})
        g.add_edge("apply_auto", "observe")
        g.add_edge("open_pr", END)
        g.add_edge("reject", END)
        g.add_edge("observe", END)
        return g.compile()

    # ------------------------------------------------------------------ helpers
    def _trace(self, intent_id, stage, ok=True, inp="", outp="", detail=None,
               t0=None):
        self.rt.store.add_trace({
            "intent_id": intent_id, "stage": stage, "ok": ok,
            "duration_ms": int((time.monotonic() - t0) * 1000) if t0 else 0,
            "input_summary": inp, "output_summary": outp,
            "detail": detail or {},
        })

    def _audit(self, actor, intent_id, stage, action, payload):
        return self.rt.audit.append(actor, intent_id, stage, action, payload)

    # ------------------------------------------------------------------ nodes
    def _n_parse(self, s: BState) -> dict:
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        parsed = parse_intent(intent)
        ok = parsed.valid
        self._trace(intent.id, "parse", ok, intent.description,
                    parsed.targets[0] if parsed.targets else "n/a",
                    {"error": parsed.error, "targets": parsed.targets}, t0)
        self._audit(intent.agent_id, intent.id, "parse",
                    "normalized" if ok else "invalid",
                    {"targets": parsed.targets, "error": parsed.error})
        return {"parsed": parsed.model_dump(),
                "status": "" if ok else "invalid",
                "error": parsed.error}

    def _n_simulate(self, s: BState) -> dict:
        if s.get("status") == "invalid":
            return {}
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        parsed = ParsedChange(**s["parsed"])
        adapter = self.rt.adapter(intent.adapter)
        if adapter is None:
            sim = SimulateResult(ok=False,
                                 error=f"adapter '{intent.adapter}' unavailable "
                                       f"in {self.rt.mode} mode")
        else:
            try:
                sim = adapter.dry_run(intent, parsed)
            except Exception as e:
                sim = SimulateResult(ok=False, error=str(e)[:300])
        self._trace(intent.id, "simulate", sim.ok, parsed.targets[0] if parsed.targets else "",
                    f"{len(sim.changes)} changes, pods~{sim.pods_affected}",
                    {"changes": [c.model_dump() for c in sim.changes][:20],
                     "touches": sim.touches.model_dump(), "error": sim.error}, t0)
        self._audit(intent.agent_id, intent.id, "simulate", "dry-run",
                    {"changes": len(sim.changes), "pods": sim.pods_affected,
                     "error": sim.error})
        out = {"sim": sim.model_dump()}
        if not sim.ok:
            out["status"] = "error"
            out["error"] = sim.error
        return out

    def _n_score(self, s: BState) -> dict:
        if s.get("status") in ("invalid", "error"):
            return {}
        t0 = time.monotonic()
        parsed = ParsedChange(**s["parsed"])
        sim = SimulateResult(**s["sim"])
        sc = score_change(parsed, sim)
        self._trace(s["intent"]["id"], "score", True,
                    f"verb={parsed.verb}", f"total={sc.total}",
                    {"components": sc.components, "rationale": sc.rationale}, t0)
        self._audit(s["intent"]["agent_id"], s["intent"]["id"], "score",
                    "blast-radius", {"total": sc.total,
                                     "components": sc.components})
        return {"score": sc.model_dump()}

    def _n_policy(self, s: BState) -> dict:
        if s.get("status") in ("invalid", "error"):
            return {}
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        parsed = ParsedChange(**s["parsed"])
        sim = SimulateResult(**s["sim"])
        pinp = build_policy_input(intent, parsed, sim, self.rt.identity)
        pinp["quota"]["used"] = self.rt.store.quota_used(
            intent.agent_id, intent.submitted_at.date().isoformat())
        decision = self.rt.policy.evaluate(pinp)
        self._trace(intent.id, "policy", decision.decision != "deny",
                    f"engine={decision.engine}", decision.decision,
                    {"rules": decision.rules, "reasons": decision.reasons,
                     "input_change": pinp["change"]}, t0)
        self._audit(intent.agent_id, intent.id, "policy", decision.decision,
                    {"rules": decision.rules, "engine": decision.engine,
                     "quota_used": pinp["quota"]["used"]})
        return {"policy": decision.model_dump()}

    def _n_route(self, s: BState) -> dict:
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        if s.get("status") == "invalid":
            rd = RouteDecision(route="reject",
                               explanation=f"invalid intent: {s.get('error')}")
        elif s.get("status") == "error":
            rd = RouteDecision(route="reject",
                               explanation=f"simulation failed: {s.get('error')}")
        else:
            sc = ScoreResult(**s["score"])
            pol = PolicyDecision(**s["policy"])
            rd = route_decision(sc, pol)
        self._trace(intent.id, "route", rd.route != "reject", "",
                    rd.route, {"explanation": rd.explanation}, t0)
        self._audit(intent.agent_id, intent.id, "route", rd.route,
                    {"explanation": rd.explanation})
        return {"route": rd.route, "explanation": rd.explanation}

    # ---- file materialization for GitOps ----
    def _repo_manifest(self, op: K8sOp) -> Optional[dict]:
        rel = f"env/{op.namespace}/{op.kind}_{op.name}.json"
        path = self.rt.repo.path / rel
        if path.exists():
            return json.loads(path.read_text())
        snap = self.rt.adapter("kubernetes").snapshot_resource(
            op.kind, op.namespace, op.name)
        if snap:
            from bounded.runtime import sim_resource_to_manifest
            return sim_resource_to_manifest(snap)
        return None

    def _files_for(self, intent: ChangeIntent, parsed: ParsedChange) -> dict:
        if intent.adapter == "terraform":
            return {f"tf/{p}": c for p, c in parsed.tf_files.items()}
        op = K8sOp(**intent.op)
        files: dict[str, str | None] = {}
        if op.manifest or op.verb in ("create", "update"):
            m = json.loads(json.dumps(op.manifest or {}))
            md = m.setdefault("metadata", {})
            md.setdefault("name", op.name)
            md.setdefault("namespace", op.namespace)
            labels = md.setdefault("labels", {})
            labels["bounded-managed"] = "true"
            rel = f"env/{md['namespace']}/{m['kind']}_{md['name']}.json"
            files[rel] = json.dumps(m, indent=1)
        elif op.verb == "scale":
            m = self._repo_manifest(op)
            if m is None:
                raise RuntimeError(f"cannot resolve target "
                                   f"{op.kind}/{op.namespace}/{op.name}")
            m.setdefault("spec", {})["replicas"] = op.replicas
            rel = f"env/{op.namespace}/{op.kind}_{op.name}.json"
            files[rel] = json.dumps(m, indent=1)
        elif op.verb == "patch":
            m = self._repo_manifest(op)
            if m is None:
                raise RuntimeError(f"cannot resolve target "
                                   f"{op.kind}/{op.namespace}/{op.name}")
            containers = m.setdefault("spec", {}).setdefault(
                "template", {}).setdefault("spec", {}).setdefault(
                "containers", [{"name": op.name, "image": "unknown"}])
            env = containers[0].setdefault("env", [])
            current = {e["name"]: e for e in env}
            for k, v in (op.env_patch or {}).items():
                current[k] = {"name": k, "value": str(v)}
            containers[0]["env"] = list(current.values())
            rel = f"env/{op.namespace}/{op.kind}_{op.name}.json"
            files[rel] = json.dumps(m, indent=1)
        else:
            raise RuntimeError(f"unsupported verb for GitOps: {op.verb}")
        return files

    def _open_pr(self, intent: ChangeIntent, files: dict) -> PRRecord:
        number = self.rt.store.next_pr_number()
        branch, commit = self.rt.repo.open_change(
            intent.id, files,
            f"bounded: {intent.description[:70]} (agent {intent.agent_id})")
        title = self.llm.complete(
            "You draft concise pull-request titles for infrastructure changes.",
            f"Intent: {intent.description}\nFiles: {sorted(files)}")
        pr = PRRecord(number=number, intent_id=intent.id, branch=branch,
                      commit_sha=commit, title=f"PR #{number}: {intent.description[:70]}",
                      diff_summary="; ".join(sorted(files)))
        self.rt.store.save_pr(pr.model_dump(mode="json"))
        self._audit(intent.agent_id, intent.id, "apply", "pr_opened",
                    {"number": number, "branch": branch, "commit": commit,
                     "files": sorted(files), "llm_title": title})
        self.rt.store.add_event("pr_opened", intent.id,
                                {"number": number, "title": pr.title,
                                 "files": sorted(files)})
        return pr

    def _merge_and_sync(self, pr: PRRecord, intent: ChangeIntent, actor: str) -> str:
        sha = self.rt.repo.merge(pr.branch, f"merge PR #{pr.number} ({intent.id})")
        pr.status = "merged"
        pr.merged_at = now_utc()
        pr.rollback_commit = sha
        self.rt.store.save_pr(pr.model_dump(mode="json"))
        self._audit(actor, intent.id, "apply", "merged",
                    {"number": pr.number, "merge_commit": sha})
        self.rt.reconciler.sync()
        ok = self.rt.reconciler.wait_synced(sha, timeout_s=60 if self.rt.mode == "real" else 20)
        self._audit(actor, intent.id, "apply", "reconciled",
                    {"number": pr.number, "synced": ok})
        if not ok:
            raise RuntimeError("reconciler did not sync after merge")
        self.rt.store.quota_increment(intent.agent_id,
                                      intent.submitted_at.date().isoformat())
        return sha

    def _n_apply_auto(self, s: BState) -> dict:
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        parsed = ParsedChange(**s["parsed"])
        try:
            files = self._files_for(intent, parsed)
            pr = self._open_pr(intent, files)
            baseline = self.rt.probe()
            merge_sha = self._merge_and_sync(pr, intent, intent.agent_id)
            self._trace(intent.id, "apply", True, "auto_apply",
                        f"PR #{pr.number} merged @ {merge_sha[:8]}",
                        {"merge_commit": merge_sha, "files": sorted(files)}, t0)
            return {"status": "applied", "pr": pr.model_dump(mode="json"),
                    "_merge_sha": merge_sha, "_baseline": baseline.model_dump()}
        except Exception as e:
            self._trace(intent.id, "apply", False, "auto_apply", str(e)[:200],
                        {}, t0)
            self._audit(intent.agent_id, intent.id, "apply", "failed",
                        {"error": str(e)[:300]})
            return {"status": "error", "error": str(e)[:300]}

    def _n_open_pr(self, s: BState) -> dict:
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        parsed = ParsedChange(**s["parsed"])
        try:
            files = self._files_for(intent, parsed)
            pr = self._open_pr(intent, files)
            self._trace(intent.id, "apply", True, "needs_approval",
                        f"PR #{pr.number} open",
                        {"files": sorted(files)}, t0)
            return {"status": "needs_approval", "pr": pr.model_dump(mode="json")}
        except Exception as e:
            self._trace(intent.id, "apply", False, "needs_approval", str(e)[:200],
                        {}, t0)
            return {"status": "error", "error": str(e)[:300]}

    def _n_reject(self, s: BState) -> dict:
        t0 = time.monotonic()
        intent = ChangeIntent(**s["intent"])
        self._trace(intent.id, "reject", True, s.get("route", "reject"),
                    s.get("explanation", ""), {}, t0)
        self._audit(intent.agent_id, intent.id, "route", "rejected",
                    {"reason": s.get("explanation", ""),
                     "route": s.get("route", ""),
                     "score": (s.get("score") or {}).get("total")})
        self.rt.store.add_event("intent_rejected", intent.id,
                                {"reason": s.get("explanation", "")})
        return {"status": "rejected"}

    def _n_observe(self, s: BState) -> dict:
        if s.get("status") != "applied":
            return {}
        intent = ChangeIntent(**s["intent"])
        from bounded.models import HealthSnapshot
        baseline = HealthSnapshot(**s["_baseline"])
        merge_sha = s["_merge_sha"]
        pr = PRRecord(**s["pr"])
        self._start_observation(intent, pr, merge_sha, baseline)
        return {}

    # ------------------------------------------------------------------ observation
    def _revert_fn(self, intent: ChangeIntent, pr: PRRecord, merge_sha: str):
        def revert() -> str:
            sha = self.rt.repo.revert(merge_sha,
                                      f"bounded auto-revert PR #{pr.number}")
            pr.status = "reverted"
            self.rt.store.save_pr(pr.model_dump(mode="json"))
            self.rt.reconciler.sync()
            self.rt.reconciler.wait_synced(sha, timeout_s=60)
            return sha
        return revert

    def _on_event(self, intent_id):
        def emit(kind, data):
            self.rt.store.add_event(kind, intent_id, data)
            self._audit("bounded", intent_id, "observe", kind,
                        {k: v for k, v in data.items() if k != "snapshot"})
        return emit

    def _start_observation(self, intent: ChangeIntent, pr: PRRecord,
                           merge_sha: str, baseline):
        obs = Observer(self.rt.probe, self._revert_fn(intent, pr, merge_sha),
                       self._on_event(intent.id),
                       window_s=self.observe_window_s,
                       interval_s=self.observe_interval_s)

        def run():
            result = obs.watch(intent.id, baseline)
            if not result["healthy"]:
                self.rt.store.set_verdict(intent.id, "reverted")
                meta = self._intent_meta.get(intent.id, {})
                meta["observation"] = result
                self._intent_meta[intent.id] = meta
            else:
                meta = self._intent_meta.get(intent.id, {})
                meta["observation"] = result
                self._intent_meta[intent.id] = meta

        if self.background_observe:
            t = threading.Thread(target=run, daemon=True,
                                 name=f"observe-{intent.id}")
            self._observer_threads.append(t)
            t.start()
        else:
            run()

    def wait_for_observations(self, timeout_s: float = 120) -> None:
        deadline = time.monotonic() + timeout_s
        for t in list(self._observer_threads):
            t.join(max(0.1, deadline - time.monotonic()))
        self._observer_threads = [t for t in self._observer_threads if t.is_alive()]

    # ------------------------------------------------------------------ public API
    def submit(self, intent: ChangeIntent, label: str = "", expected: str = "",
               degrade: bool = False) -> Verdict:
        d = intent.model_dump(mode="json")
        d |= {"label": label, "expected": expected, "degrade": int(degrade),
              "verdict": ""}
        self._intent_meta[intent.id] = {"label": label, "expected": expected,
                                        "degrade": degrade}
        self.rt.store.save_intent(d)
        self._audit(intent.agent_id, intent.id, "intake", "received",
                    {"description": intent.description,
                     "adapter": intent.adapter, "apply_direct": intent.apply_direct})
        state = self.graph.invoke({"intent": d})
        verdict = self._verdict_from(state)
        self.rt.store.set_verdict(intent.id, verdict.status)
        return verdict

    def _verdict_from(self, s: BState) -> Verdict:
        status = s.get("status") or "error"
        pr = PRRecord(**s["pr"]) if s.get("pr") else None
        return Verdict(
            intent_id=s["intent"]["id"],
            status=status,  # type: ignore[arg-type]
            score=(s.get("score") or {}).get("total", 0),
            route=s.get("route", ""),
            policy_rules=(s.get("policy") or {}).get("rules", []),
            pr=pr,
            explanation=s.get("explanation") or s.get("error", ""))

    def approve(self, pr_number: int, approver: str = "human") -> Verdict:
        prd = self.rt.store.get_pr(pr_number)
        if not prd or prd["status"] != "open":
            return Verdict(intent_id=prd["intent_id"] if prd else "?",
                           status="error", explanation="PR not open")
        pr = PRRecord(**{k: v for k, v in prd.items()
                         if k in PRRecord.model_fields})
        d = self.rt.store.get_intent(pr.intent_id)
        intent = ChangeIntent(**{k: v for k, v in d.items()
                                 if k in ChangeIntent.model_fields})
        baseline = self.rt.probe()
        try:
            merge_sha = self._merge_and_sync(pr, intent, f"human:{approver}")
            self._audit(f"human:{approver}", intent.id, "apply", "approved",
                        {"number": pr_number})
            self.rt.store.add_event("pr_approved", intent.id,
                                    {"number": pr_number, "approver": approver})
            self._start_observation(intent, pr, merge_sha, baseline)
            verdict = Verdict(intent_id=intent.id, status="applied",
                              score=0, route="approval",
                              policy_rules=[], pr=pr,
                              explanation=f"PR #{pr_number} approved and merged")
        except Exception as e:
            verdict = Verdict(intent_id=intent.id, status="error",
                              explanation=str(e)[:300])
        self.rt.store.set_verdict(intent.id, verdict.status)
        return verdict

    def reject_pr(self, pr_number: int, approver: str = "human") -> Verdict:
        prd = self.rt.store.get_pr(pr_number)
        if not prd or prd["status"] != "open":
            return Verdict(intent_id=prd["intent_id"] if prd else "?",
                           status="error", explanation="PR not open")
        pr = PRRecord(**{k: v for k, v in prd.items()
                         if k in PRRecord.model_fields})
        self.rt.repo.reject(pr.branch)
        pr.status = "rejected"
        self.rt.store.save_pr(pr.model_dump(mode="json"))
        self._audit(f"human:{approver}", pr.intent_id, "apply", "pr_rejected",
                    {"number": pr_number})
        self.rt.store.set_verdict(pr.intent_id, "rejected")
        self.rt.store.add_event("pr_rejected", pr.intent_id,
                                {"number": pr_number, "approver": approver})
        return Verdict(intent_id=pr.intent_id, status="rejected",
                       route="approval",
                       explanation=f"PR #{pr_number} rejected by {approver}")
