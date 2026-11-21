"""ROUTE — turn score + policy into one of three outcomes."""
from __future__ import annotations

import os

from bounded.models import PolicyDecision, RouteDecision, ScoreResult


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def route_decision(score: ScoreResult, policy: PolicyDecision) -> RouteDecision:
    auto_max = _env_int("BOUNDED_AUTO_MAX", 24)
    approval_max = _env_int("BOUNDED_APPROVAL_MAX", 60)

    if policy.decision == "deny":
        return RouteDecision(
            route="reject",
            explanation=f"policy denied: {', '.join(policy.rules)}")

    if score.total > approval_max:
        return RouteDecision(
            route="reject",
            explanation=(f"blast radius {score.total} exceeds the high "
                         f"threshold ({approval_max})"))

    if policy.decision == "approval" or score.tier0:
        return RouteDecision(
            route="approval",
            explanation=(f"blast radius {score.total}; requires one human "
                         f"approval ({', '.join(policy.rules) or 'score band'})"))

    if score.total <= auto_max:
        return RouteDecision(
            route="auto_apply",
            explanation=f"blast radius {score.total} is within the auto-apply "
                        f"threshold ({auto_max})")

    return RouteDecision(
        route="approval",
        explanation=f"blast radius {score.total} in the approval band "
                    f"({auto_max}-{approval_max})")
