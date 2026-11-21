"""`bounded.guard()` — wrap any agent's change-execution function in one line.

    @bounded.guard("reckless-rs")
    def execute_change(op: dict, description: str) -> dict:
        return raw_kubectl_apply(op)          # the agent's own (dangerous) way

With the guard active, the wrapped function never runs directly: the op is
turned into a Change Intent and pushed through the full Bounded pipeline
(dry-run → score → policy → route → GitOps apply → observe → auto-revert),
and the agent receives the Verdict. With BOUNDED_MODE=off the original
function runs untouched — that is the unguarded arm of the evaluation.
"""
from __future__ import annotations

import functools
import os
from typing import Callable

from bounded.models import ChangeIntent, Verdict


def guard(agent_id: str, runtime=None, description_default: str = "agent change"):
    def decorator(fn: Callable[[dict, str], dict]):
        @functools.wraps(fn)
        def wrapper(op: dict, description: str = "") -> dict:
            if os.environ.get("BOUNDED_MODE", "sim") == "off":
                return fn(op, description)
            rt = runtime
            if rt is None:
                from bounded.runtime import build_runtime
                rt = build_runtime()
            adapter = "terraform" if "files" in (op or {}) else "kubernetes"
            intent = ChangeIntent(
                agent_id=agent_id,
                description=description or description_default,
                adapter=adapter,
                op=op)
            verdict: Verdict = rt.bounded.submit(intent)
            return verdict.model_dump()
        wrapper.__guarded__ = True
        return wrapper
    return decorator
