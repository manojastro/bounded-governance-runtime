# Bounded

A **deny-by-default governance runtime for AI-agent actions**. Agents submit *intents*; they never touch the
cluster directly. Every intent goes through a deterministic pipeline and lands on infrastructure only via GitOps.

> Portfolio/POC build. The default mode is a deterministic simulated cluster (`SimCluster`); no real infrastructure is needed.

## Pipeline

LangGraph state machine (`bounded/pipeline.py`):

```
PARSE -> SIMULATE -> SCORE -> POLICY -> ROUTE -> { APPLY | OPEN_PR | REJECT }
                                                    APPLY -> OBSERVE (auto-revert on degradation)
```

| Step | Module | Notes |
|---|---|---|
| Identity | `identity.py`, `config/agents.yaml` | Agents not listed cannot submit. Per-agent adapters, verbs, namespaces, tier-0 access and daily quota |
| Parse | `parse.py` | Normalises an intent into a typed change |
| Simulate | `adapters/` | `simcluster` (file-backed), `kubernetes` (kubectl dry-run), `terraform` (plan diff) |
| Score | `score.py` | 0-100 blast radius; every component is listed with a rationale |
| Policy | `policy/bounded.rego`, `policy/engine.py` | Deny rules first, then approval rules, then allow. OPA server or an embedded Python mirror with identical decisions |
| Route | `route.py` | Auto-apply, open a PR for approval, or reject |
| Apply | `gitops.py` | Changes land via a branch and PR record; a reconciler applies merged state |
| Observe | `observe.py` | Watches health after apply and **reverts automatically** on degradation |
| Audit | `audit.py` | Hash-linked, HMAC-signed audit chain; `verify_chain()` detects tampering |

Safety decisions are 100% deterministic (policy plus scoring). The LLM only narrates (`llm.py`,
`LLM_PROVIDER=fake` by default).

One-line integration for an existing agent:

```python
@bounded.guard("reckless-rs")
def execute_change(op: dict, description: str) -> dict:
    return raw_kubectl_apply(op)
```

## Try it

```bash
python scripts/smoke.py      # runs the core pipeline in sim mode against several scripted intents
```

Python 3, with `langgraph`, `pydantic`, `httpx` and `pyyaml`. This copy has no dependency manifest or unit-test
suite; the smoke script is the executable check. `runtime/` holds sample simulated cluster and reconciler state.

## Demo agents (`config/agents.yaml`)

`reckless-rs` (limited namespaces, never delete or grant), `deploy-bot` (broad, still no tier-0),
`spammy-ci` (quota demo), `payments-sre` (tier-0 operator).
