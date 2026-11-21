"""Quick smoke test of the core pipeline in sim mode (no cluster needed)."""
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if Path("runtime").exists():
    shutil.rmtree("runtime")

from bounded.models import ChangeIntent, now_utc
from bounded.runtime import build_runtime

rt = build_runtime("sim", fresh=False)
b = rt.bounded
b.background_observe = False
b.observe_window_s = 1.5
b.observe_interval_s = 0.2

cases = [
    # (description, adapter, op, expect_status)
    ("scale web 2->4", "kubernetes",
     {"verb": "scale", "kind": "Deployment", "name": "web", "namespace": "shop",
      "replicas": 4}, "approval_flow"),
    ("delete all namespaces", "kubernetes",
     {"verb": "delete", "kind": "Namespace", "name": "*", "namespace": ""},
     "rejected"),
    ("scale postgres to 0", "kubernetes",
     {"verb": "scale", "kind": "StatefulSet", "name": "postgres",
      "namespace": "shop", "replicas": 0}, "rejected"),
    ("delete payments netpol", "kubernetes",
     {"verb": "delete", "kind": "NetworkPolicy", "name": "default-deny-payments",
      "namespace": "payments"}, "rejected"),
    ("create cache deployment in dev", "kubernetes",
     {"verb": "create", "namespace": "dev-sandbox",
      "manifest": {"apiVersion": "apps/v1", "kind": "Deployment",
                   "metadata": {"name": "cache", "namespace": "dev-sandbox",
                                "labels": {"app": "cache"}},
                   "spec": {"replicas": 1, "template": {
                       "metadata": {"labels": {"app": "cache"}},
                       "spec": {"containers": [{"name": "cache",
                                                "image": "redis:7"}]}}}}},
     "applied"),
    ("patch api with BUG env (degrade->revert)", "kubernetes",
     {"verb": "patch", "kind": "Deployment", "name": "api", "namespace": "shop",
      "env_patch": {"BUG": "1"}}, "applied_or_reverted"),
    ("unknown agent change", "kubernetes",
     {"verb": "scale", "kind": "Deployment", "name": "web", "namespace": "shop",
      "replicas": 3}, "rejected"),
]

ok_all = True
for desc, adapter, op, expect in cases:
    agent = "mystery-agent" if desc.startswith("unknown") else "reckless-rs"
    from datetime import datetime, timezone
    intent = ChangeIntent(agent_id=agent, description=desc, adapter=adapter, op=op,
                          submitted_at=datetime(2026, 8, 22, 3, 0, tzinfo=timezone.utc))
    v = b.submit(intent)
    b.wait_for_observations(20)
    # re-read final verdict (may have flipped to reverted)
    from bounded.store import Store
    final = Store(rt.store.path).get_intent(intent.id)["verdict"]
    if final == "needs_approval":
        pr = rt.store.get_pr_by_intent(intent.id)
        v2 = b.approve(pr["number"], "smoke-human")
        b.wait_for_observations(20)
        final = Store(rt.store.path).get_intent(intent.id)["verdict"]
    ok = (final in ("applied", "reverted") if expect in ("applied_or_reverted", "approval_flow")
          else final == expect)
    ok_all &= ok
    print(f"{'OK ' if ok else 'FAIL'} {desc:45s} -> status={final} "
          f"score={v.score} route={v.route} rules={v.policy_rules}")

# verify audit chain + reconciler state
print("audit:", rt.audit.verify_chain())
print("health after revert:", rt.adapters["kubernetes"].probe())
sys.exit(0 if ok_all else 1)
