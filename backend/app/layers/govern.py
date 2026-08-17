"""
Layer 6 — Govern & Harden (zero-trust).

Production stack: Keycloak (identity/RBAC) + Open Policy Agent (action policy) +
Postgres hash-chained WORM audit + OT/IT network segmentation.
Azure equivalent: Microsoft Entra ID + Azure Policy + immutable storage.

Here we implement a runnable equivalent: role checks, a policy gate, and a
genuine hash-chained (tamper-evident) audit log kept in memory.
"""
from __future__ import annotations
import hashlib
import json
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, List

# ---- Keycloak-style roles (who may do what) ----
ROLES = {
    "operator": {"view", "acknowledge"},
    "shift_lead": {"view", "acknowledge", "approve_low"},
    "qa": {"view", "acknowledge", "approve_low", "esign"},
    "agent": {"view", "act_autonomous"},
}


class AuditLog:
    """Hash-chained, append-only (WORM) audit trail."""
    def __init__(self):
        self._lock = Lock()
        self._chain: List[Dict] = []
        self._genesis = "0" * 64

    def _last_hash(self) -> str:
        return self._chain[-1]["hash"] if self._chain else self._genesis

    def append(self, actor: str, action: str, payload: Dict) -> Dict:
        with self._lock:
            prev = self._last_hash()
            entry = {
                "seq": len(self._chain) + 1,
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "actor": actor,
                "action": action,
                "payload": payload,
                "prev_hash": prev,
            }
            body = json.dumps(entry, sort_keys=True).encode()
            entry["hash"] = hashlib.sha256(body).hexdigest()
            self._chain.append(entry)
            return entry

    def recent(self, n: int = 30) -> List[Dict]:
        with self._lock:
            return self._chain[-n:][::-1]

    def verify(self) -> Dict:
        """Re-hash the chain to prove it hasn't been tampered with."""
        with self._lock:
            prev = self._genesis
            for e in self._chain:
                body = {k: e[k] for k in ("seq", "ts", "actor", "action", "payload", "prev_hash")}
                h = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
                if h != e["hash"] or e["prev_hash"] != prev:
                    return {"valid": False, "broken_at": e["seq"]}
                prev = e["hash"]
            return {"valid": True, "entries": len(self._chain)}

    def stats(self) -> Dict:
        with self._lock:
            return {"entries": len(self._chain)}


AUDIT = AuditLog()
AUDIT.append("system", "start", {"param": ""})


def has_permission(role: str, capability: str) -> bool:
    return capability in ROLES.get(role, set())


def _p_breach(zone: str) -> float:
    return {"trip": 0.95, "alarm": 0.90, "control": 0.12}.get(zone or "alarm", 0.90)


def policy_view(event: Dict, agent_result: Dict) -> Dict:
    """Fields for the Policy decision panel."""
    gov = agent_result.get("governance") or {}
    zone = event.get("zone") or "alarm"
    pid = event.get("param") or ""
    esign = bool(gov.get("required"))
    rule = "always e-sign" if pid == "ph" else (gov.get("control") or "policy gate")
    return {
        "event_id": f"evt-{pid}",
        "param": pid,
        "short": event.get("short") or pid,
        "asset": event.get("asset") or "",
        "tag": event.get("tag") or "",
        "zone": zone,
        "ph_rule": rule,
        "threshold_rule": "P(breach) ≥ 0.80",
        "p_breach": _p_breach(zone),
        "decision": "E-SIGN REQUIRED" if esign else "PERMITTED",
        "esign_required": esign,
        "control": gov.get("control") or "21 CFR Part 11",
        "message": gov.get("message") or "",
        "approver": gov.get("approver"),
    }


def decide_and_commit(event: Dict, agent_result: Dict, actor_role: str = "qa",
                      signer: str | None = None, meaning: str | None = None,
                      action: str = "commit") -> Dict:
    """Apply the governance decision and write the audit record."""
    gov = agent_result.get("governance", {})
    required = gov.get("required", False)
    action = (action or "commit").lower()
    actor = (signer or "").strip() or actor_role
    payload = {
        "asset": event.get("asset"),
        "param": event.get("param"),
        "tag": event.get("tag"),
        "zone": event.get("zone"),
        "remedy": (agent_result.get("remedy") or {}).get("summary"),
        "approver": gov.get("approver"),
        "control": gov.get("control"),
        "signer": signer,
        "meaning": meaning,
        "role": actor_role,
    }

    if action == "reject":
        entry = AUDIT.append(actor, "rejected", payload)
        return {
            "committed": False, "mode": "REJECTED",
            "reason": meaning or "rejected by signer",
            "audit_seq": entry["seq"], "audit_hash": entry["hash"][:12],
        }

    if not required:
        entry = AUDIT.append(
            actor="sentra-agent", action="autonomous_action", payload=payload,
        )
        return {"committed": True, "mode": "AUTONOMOUS",
                "audit_seq": entry["seq"], "audit_hash": entry["hash"][:12]}

    cap = "esign" if "cfr" in str(gov.get("control") or "").lower() else "approve_low"
    if not has_permission(actor_role, cap) and not has_permission(actor_role, "esign"):
        return {"committed": False, "mode": "BLOCKED",
                "reason": f"role '{actor_role}' lacks '{cap}'"}
    if required and not (signer or "").strip():
        return {"committed": False, "mode": "E-SIGN REQUIRED",
                "reason": "QA e-signature is required before commit"}

    entry = AUDIT.append(actor, "approved_with_esignature", payload)
    return {"committed": True, "mode": gov.get("action", "ESCALATE"),
            "approver": gov.get("approver"), "control": gov.get("control"),
            "audit_seq": entry["seq"], "audit_hash": entry["hash"][:12]}
