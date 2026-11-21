"""Typed data models shared across the Bounded pipeline."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

Verb = Literal["create", "update", "patch", "scale", "delete", "grant", "read"]
Route = Literal["auto_apply", "approval", "reject"]
PolicyDecisionKind = Literal["allow", "approval", "deny"]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


class K8sOp(BaseModel):
    """A Kubernetes change an agent wants to make."""
    verb: Verb
    kind: str = ""                 # Deployment, Namespace, NetworkPolicy, ...
    name: str = ""                 # may be empty with label_selector
    namespace: str = "default"
    manifest: Optional[dict] = None
    replicas: Optional[int] = None  # for verb == "scale"
    label_selector: str = ""
    env_patch: Optional[dict] = None  # for verb == "patch": container env overrides

    def describe(self) -> str:
        target = self.name or self.label_selector or "*"
        return f"{self.verb} {self.kind or 'manifest'} {self.namespace}/{target}"


class TfOp(BaseModel):
    """A Terraform change an agent wants to make (a set of .tf file contents)."""
    files: dict[str, str] = Field(default_factory=dict)  # path -> full file content
    workspace: str = "default"

    def describe(self) -> str:
        return f"terraform change to {sorted(self.files)}"


class ChangeIntent(BaseModel):
    """What an agent submits to Bounded. The agent never touches the cluster."""
    id: str = Field(default_factory=lambda: new_id("int"))
    agent_id: str
    submitted_at: datetime = Field(default_factory=now_utc)
    description: str
    adapter: Literal["kubernetes", "terraform"]
    op: dict[str, Any]
    requested_by: str = "agent"
    ticket_ref: str = ""
    apply_direct: bool = False   # asking to skip the pipeline entirely

    def op_model(self):
        if self.adapter == "terraform":
            return TfOp(**self.op)
        return K8sOp(**self.op)


class ParsedChange(BaseModel):
    """Normalized, typed form of an intent (output of PARSE)."""
    intent_id: str
    adapter: Literal["kubernetes", "terraform"]
    verb: Verb
    targets: list[str] = Field(default_factory=list)   # "Deployment/shop/api" style
    namespaces: list[str] = Field(default_factory=list)
    k8s_kinds: list[str] = Field(default_factory=list)
    tf_files: dict[str, str] = Field(default_factory=dict)
    valid: bool = True
    error: str = ""


class ResourceChange(BaseModel):
    kind: str
    name: str
    namespace: str
    action: Literal["create", "update", "delete", "none"]
    before: Optional[dict] = None
    after: Optional[dict] = None


class TouchFlags(BaseModel):
    network_policy: bool = False
    rbac: bool = False
    secrets: bool = False
    storage: bool = False
    statefulset: bool = False
    tier0: bool = False


class SimulateResult(BaseModel):
    """Output of SIMULATE: the exact diff a dry-run produced."""
    ok: bool
    error: str = ""
    changes: list[ResourceChange] = Field(default_factory=list)
    pods_affected: int = 0
    services_affected: int = 0
    namespaces_affected: int = 0
    workloads_affected: int = 0
    dependents: list[str] = Field(default_factory=list)
    touches: TouchFlags = Field(default_factory=TouchFlags)
    raw: str = ""


class ScoreResult(BaseModel):
    """Blast radius 0-100 with a transparent component breakdown."""
    total: int = 0
    components: dict[str, int] = Field(default_factory=dict)
    tier0: bool = False
    rationale: list[str] = Field(default_factory=list)


class PolicyDecision(BaseModel):
    decision: PolicyDecisionKind = "allow"
    rules: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    engine: str = "embedded"


class RouteDecision(BaseModel):
    route: Route
    explanation: str = ""


class PRRecord(BaseModel):
    number: int
    intent_id: str
    branch: str
    commit_sha: str
    status: Literal["open", "merged", "rejected", "reverted"] = "open"
    created_at: datetime = Field(default_factory=now_utc)
    merged_at: Optional[datetime] = None
    rollback_commit: str = ""
    diff_summary: str = ""
    title: str = ""


class Verdict(BaseModel):
    """Final answer handed back to the submitting agent."""
    intent_id: str
    status: Literal[
        "invalid", "rejected", "needs_approval", "applied", "reverted", "error"
    ]
    score: int = 0
    route: str = ""
    policy_rules: list[str] = Field(default_factory=list)
    pr: Optional[PRRecord] = None
    explanation: str = ""
    reverted_reason: str = ""


class HealthSnapshot(BaseModel):
    ts: datetime = Field(default_factory=now_utc)
    available: bool = True
    ready_ratio: float = 1.0
    error_rate: float = 0.0
    p95_ms: float = 0.0
    restarts: int = 0


class TraceStage(BaseModel):
    stage: str
    ok: bool = True
    started_at: datetime = Field(default_factory=now_utc)
    duration_ms: int = 0
    input_summary: str = ""
    output_summary: str = ""
    detail: dict[str, Any] = Field(default_factory=dict)
