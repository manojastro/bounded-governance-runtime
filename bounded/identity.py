"""Agent identity registry — deny-by-default, with per-agent verb,
namespace and daily-quota restrictions."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml

from bounded.models import ParsedChange, PolicyDecision

DEFAULT_CONFIG = Path(os.environ.get("BOUNDED_AGENTS_CONFIG", "config/agents.yaml"))


class Identity:
    def __init__(self, config_path: Path | None = None):
        self.config_path = Path(config_path or DEFAULT_CONFIG)
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        self.agents = {a["id"]: a for a in data.get("agents", [])}

    def get(self, agent_id: str) -> Optional[dict]:
        return self.agents.get(agent_id)

    def check(self, agent_id: str, parsed: ParsedChange, quota_used: int) -> PolicyDecision:
        """Identity-level checks only; resource rules live in policy/."""
        agent = self.get(agent_id)
        if agent is None:
            return PolicyDecision(
                decision="deny", rules=["R_UNKNOWN_AGENT"],
                reasons=[f"agent '{agent_id}' is not registered (deny by default)"],
                engine="identity",
            )
        rules, reasons = [], []

        if parsed.verb not in agent["allowed_verbs"]:
            rules.append("R_IDENTITY_VERB")
            reasons.append(
                f"agent '{agent_id}' may only use verbs {agent['allowed_verbs']}, "
                f"not '{parsed.verb}'")

        allowed_ns = agent["allowed_namespaces"]
        wildcard = "*" in allowed_ns
        for ns in parsed.namespaces:
            if ns not in allowed_ns and not wildcard:
                rules.append("R_IDENTITY_NAMESPACE")
                reasons.append(
                    f"agent '{agent_id}' may not touch namespace '{ns}' "
                    f"(allowed: {allowed_ns})")
                break

        if quota_used >= int(agent["max_actions_per_day"]):
            rules.append("R_QUOTA")
            reasons.append(
                f"agent '{agent_id}' hit its daily quota "
                f"({agent['max_actions_per_day']} actions/day)")

        if rules:
            return PolicyDecision(decision="deny", rules=rules, reasons=reasons,
                                  engine="identity")
        return PolicyDecision(decision="allow", rules=[], reasons=[], engine="identity")
