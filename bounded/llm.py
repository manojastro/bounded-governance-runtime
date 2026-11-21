"""Swappable LLM interface.

Safety decisions in Bounded are 100% deterministic (policy + scoring).
The LLM only *narrates*: PR descriptions, verdict explanations and the
reckless demo agent's self-justifications. LLM_PROVIDER=fake (default)
is fully deterministic and needs no API key — the whole test suite,
demo and eval run on it. Point LLM_PROVIDER=openai (+ OPENAI_BASE_URL,
OPENAI_API_KEY, LLM_MODEL) at any OpenAI-compatible endpoint to swap.
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Protocol


class LLM(Protocol):
    name: str

    def complete(self, system: str, user: str) -> str: ...


class FakeLLM:
    """Deterministic stand-in: hashes the prompt into a stable, sensible reply."""
    name = "fake"

    def complete(self, system: str, user: str) -> str:
        h = hashlib.sha256((system + "|" + user).encode()).hexdigest()
        # A few rotating deterministic phrasings keyed on the prompt hash.
        templates = [
            "Proceeding as planned — this change looks routine from where I stand.",
            "High confidence this is safe; the target matches the intended workload.",
            "Standard maintenance change. No unusual dependencies detected.",
            "This should be a no-op for users. Applying now.",
        ]
        return templates[int(h[:6], 16) % len(templates)]


class OpenAICompatLLM:
    """Minimal OpenAI-compatible chat client (no SDK dependency)."""
    name = "openai"

    def __init__(self):
        self.base_url = os.environ["OPENAI_BASE_URL"].rstrip("/")
        self.api_key = os.environ.get("OPENAI_API_KEY", "")
        self.model = os.environ.get("LLM_MODEL", "gpt-4o-mini")

    def complete(self, system: str, user: str) -> str:
        import urllib.request

        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps({
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0,
            }).encode(),
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {self.api_key}"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
        return data["choices"][0]["message"]["content"]


def get_llm() -> LLM:
    provider = os.environ.get("LLM_PROVIDER", "fake").lower()
    if provider == "openai":
        try:
            return OpenAICompatLLM()
        except KeyError:
            return FakeLLM()
    return FakeLLM()
