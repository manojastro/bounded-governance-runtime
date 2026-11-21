"""PARSE — normalize a submitted intent into a typed ParsedChange."""
from __future__ import annotations

from bounded.models import ChangeIntent, K8sOp, ParsedChange, TfOp


class ParseError(Exception):
    pass


def parse_intent(intent: ChangeIntent) -> ParsedChange:
    try:
        op = intent.op_model()
    except Exception as e:  # malformed op payload
        return ParsedChange(intent_id=intent.id, adapter=intent.adapter,
                            verb="update", valid=False, error=f"bad op payload: {e}")

    if intent.adapter == "terraform":
        if not isinstance(op, TfOp) or not op.files:
            return ParsedChange(intent_id=intent.id, adapter="terraform",
                                verb="update", valid=False,
                                error="terraform intent must include tf files")
        targets = sorted(op.files.keys())
        return ParsedChange(
            intent_id=intent.id, adapter="terraform", verb="update",
            targets=[f"terraform:{t}" for t in targets],
            namespaces=[], tf_files=dict(op.files),
        )

    # kubernetes
    assert isinstance(op, K8sOp)
    if not op.kind and not op.manifest:
        return ParsedChange(intent_id=intent.id, adapter="kubernetes",
                            verb=op.verb, valid=False,
                            error="kubernetes intent needs kind or manifest")
    if op.verb == "scale" and op.replicas is None:
        return ParsedChange(intent_id=intent.id, adapter="kubernetes",
                            verb=op.verb, valid=False,
                            error="scale intent needs target replicas")
    if op.verb in ("delete", "scale") and not (op.name or op.label_selector):
        return ParsedChange(intent_id=intent.id, adapter="kubernetes",
                            verb=op.verb, valid=False,
                            error=f"{op.verb} intent needs name or label_selector")

    kind = op.kind or (op.manifest or {}).get("kind", "")
    name = op.name or (op.manifest or {}).get("metadata", {}).get("name", "")
    ns = op.namespace or (op.manifest or {}).get("metadata", {}).get(
        "namespace", "default")
    targets = [f"{kind}/{ns}/{name or op.label_selector or '*'}"]
    return ParsedChange(
        intent_id=intent.id, adapter="kubernetes", verb=op.verb,
        targets=targets, namespaces=[ns], k8s_kinds=[kind] if kind else [],
    )
