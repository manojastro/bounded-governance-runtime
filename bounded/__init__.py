"""Bounded — a blast-radius guardrail layer for operations AI agents.

Agents never hold credentials. They submit a Change Intent; Bounded
dry-runs it, scores the blast radius, evaluates OPA/Rego policy, routes
it (auto-apply / human approval / reject), applies via GitOps only, and
auto-reverts if health degrades.
"""
from bounded.guard import guard  # noqa: F401

__version__ = "1.0.0"
