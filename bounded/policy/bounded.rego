package bounded

# Bounded policy: deny rules first, then approval rules, then allow.
# Input shape is built identically by bounded/policy/engine.py so the
# embedded engine and this Rego stay byte-for-byte equivalent on
# decisions and matched rule sets.

deny_rules["R_UNKNOWN_AGENT"] {
  not input.agent.exists
}

deny_rules["R_IDENTITY_VERB"] {
  input.agent.exists
  input.change.verb != "read"
  not verb_allowed
}

deny_rules["R_IDENTITY_NAMESPACE"] {
  input.agent.exists
  not namespace_allowed
  count(input.change.namespaces) > 0
}

deny_rules["R_QUOTA"] {
  input.agent.exists
  input.quota.used >= input.quota.limit
}

deny_rules["R_NO_BYPASS"] {
  input.change.apply_direct
}

deny_rules["R_FREEZE"] {
  input.context.freeze
  input.change.verb != "read"
}

deny_rules["R_PROTECTED_NAMESPACE"] {
  input.change.namespaces[_] == "payments"
  input.change.verb != "read"
  not input.agent.may_touch_tier0
}

deny_rules["R_TIER0"] {
  input.change.tier0
  input.change.verb != "read"
  not input.agent.may_touch_tier0
}

deny_rules["R_TIME_WINDOW"] {
  input.change.verb != "read"
  not input.agent.may_touch_tier0
  input.context.business_hours
  prod_namespace
}

deny_rules["R_NAMESPACE_DELETE"] {
  input.change.verb == "delete"
  input.change.kinds[_] == "Namespace"
}

deny_rules["R_DELETE_WILDCARD"] {
  input.change.verb == "delete"
  input.change.wildcard
}

deny_rules["R_STATEFULSET_DELETE"] {
  input.change.verb == "delete"
  input.change.kinds[_] == "StatefulSet"
}

deny_rules["R_STATEFULSET_ZERO"] {
  input.change.verb == "scale"
  input.change.kinds[_] == "StatefulSet"
  input.change.target_replicas <= 0
}

deny_rules["R_PVC_DELETE"] {
  input.change.verb == "delete"
  input.change.kinds[_] == "PersistentVolumeClaim"
}

deny_rules["R_NETPOL_DELETE"] {
  input.change.verb == "delete"
  input.change.kinds[_] == "NetworkPolicy"
}

deny_rules["R_CLUSTERADMIN_GRANT"] {
  input.change.grants_cluster_admin
}

deny_rules["R_SECRET_EXFIL"] {
  input.change.verb == "read"
  input.change.touches.secrets
}

deny_rules["R_SECRET_ROLE_GRANT"] {
  input.change.grants_secret_read
}

approval_rules["A_TIER0"] {
  input.change.tier0
  input.change.verb != "read"
  input.agent.may_touch_tier0
}

approval_rules["A_RBAC"] {
  input.change.touches.rbac
  input.change.verb != "read"
}

approval_rules["A_SECRET_WRITE"] {
  input.change.touches.secrets
  input.change.verb != "read"
}

approval_rules["A_NETPOL_WRITE"] {
  input.change.touches.network_policy
  input.change.verb != "read"
}

approval_rules["A_STORAGE"] {
  input.change.touches.storage
  input.change.verb != "read"
}

approval_rules["A_STATEFULSET"] {
  input.change.touches.statefulset
  input.change.verb != "read"
}

verb_allowed {
  input.agent.allowed_verbs[_] == input.change.verb
}

namespace_allowed {
  input.agent.allowed_namespaces[_] == "*"
}

namespace_allowed {
  ns := input.change.namespaces[_]
  input.agent.allowed_namespaces[_] == ns
}

prod_namespace {
  input.change.namespaces[_] == "shop"
}

prod_namespace {
  input.change.namespaces[_] == "payments"
}

decision = out {
  count(deny_rules) > 0
  out := {"decision": "deny", "rules": deny_rules}
}

decision = out {
  count(deny_rules) == 0
  count(approval_rules) > 0
  out := {"decision": "approval", "rules": approval_rules}
}

decision = out {
  count(deny_rules) == 0
  count(approval_rules) == 0
  out := {"decision": "allow", "rules": []}
}
