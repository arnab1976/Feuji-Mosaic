"""
SENTRA — the agentic intelligence layer (rides on top of MOSAIC).

Production stack: LangGraph orchestration + Ollama (local LLM reasoning) +
Qdrant + BGE-small (RAG).
Azure equivalent: Azure AI Foundry + Azure OpenAI + Azure AI Search.

The agent consumes a *contextualized* event (not a raw reading — that is the
whole point), diagnoses the deviation using the drivers the context carried,
retrieves the approved remedy via RAG, and produces a cited remedy card. High
-risk actions are routed to governance rather than executed autonomously.
"""
from __future__ import annotations
import json
import os
import urllib.error
import urllib.request
from typing import Dict, List, Optional

from .knowledge import search
from ..domain import PARAMETERS

AGENTS = {
    "temp": {
        "id": "temp", "name": "Temperature Agent", "short": "Temperature",
        "focus": "coolant loop, jacket temperature, heat-exchanger dP",
    },
    "ph": {
        "id": "ph", "name": "pH Agent", "short": "pH",
        "focus": "acid/base dosing, CO2 accumulation, probe drift",
    },
    "press": {
        "id": "press", "name": "Pressure Agent", "short": "Pressure",
        "focus": "filter dP, exhaust flow, seal integrity",
    },
    "cond": {
        "id": "cond", "name": "Conductivity Agent", "short": "Conductivity",
        "focus": "feed composition, TOC, resin-bed regeneration",
    },
    "hum": {
        "id": "hum", "name": "Humidity Agent", "short": "Humidity",
        "focus": "HVAC fan speed, cooling coil, HEPA dP",
    },
}


def event_to_queries(event: Dict) -> List[Dict]:
    """Turn a decomposed Flink event into retrieval queries (NL + SQL)."""
    pid = event.get("param") or "temp"
    p = PARAMETERS.get(pid)
    name = p.short if p else pid
    asset = event.get("asset") or (p.asset if p else "")
    drivers = event.get("drivers") or ((p.drivers[:3] if p else []))
    rid = event.get("reading_id") or ""
    tag = event.get("tag") or (p.tag if p else "")
    status = event.get("status") or ""
    zone = event.get("zone") or ""
    return [
        {
            "kind": "nl",
            "label": "Diagnosis query",
            "text": (
                f"{name} {status} {zone} excursion on {asset} "
                f"drivers {' '.join(drivers)} corrective action remedy"
            ),
        },
        {
            "kind": "nl",
            "label": "SOP retrieve",
            "text": f"SOP excursion response {name} {asset} setpoint control loop",
        },
        {
            "kind": "nl",
            "label": "CAPA retrieve",
            "text": f"CAPA historical root cause {asset} {name} {tag}",
        },
        {
            "kind": "sql",
            "label": "Flink SQL",
            "text": (
                "SELECT r.reading_id, r.value, a.asset, m.batch, s.material_no\n"
                "FROM readings r\n"
                "JOIN asset_model a ON r.tag = a.tag\n"
                "JOIN mes_batches m ON a.asset = m.asset "
                "AND r.ts BETWEEN m.start AND m.end\n"
                "JOIN sap_master s ON m.product = s.product\n"
                f"WHERE r.reading_id = '{rid}'"
            ),
        },
    ]


def run_agent(event: Dict) -> Dict:
    """Run one SENTRA cycle on a contextualized event."""
    pid = event.get("param")
    p = PARAMETERS[pid]
    meta = AGENTS.get(pid) or {"name": f"{p.short} Agent", "short": p.short, "focus": ""}
    status = event.get("status")
    steps: List[Dict] = []
    queries = event_to_queries(event)

    steps.append({
        "stage": "perceive",
        "detail": f"received contextualized event: {event['asset']} "
                  f"{p.short} {event['val']}{event['unit']} status={status}",
    })

    if status == "OK":
        steps.append({"stage": "assess", "detail": "value in control band — no action required"})
        return {
            "param": pid,
            "agent": meta["name"],
            "status": status,
            "action_required": False,
            "steps": steps,
            "queries": queries,
            "remedy": None,
            "citations": [],
            "hits": [],
            "governance": {"required": False},
            "llm": {"provider": "none", "used": False},
        }

    drivers = event.get("drivers", []) or p.drivers[:3]
    query = queries[0]["text"]
    steps.append({
        "stage": "diagnose",
        "detail": f"deviation {status} by {event.get('delta')}{event['unit']}; "
                  f"likely drivers: {', '.join(drivers)}",
        "query": query,
    })

    hits = search(query, top_k=3, param=pid)
    extra = []
    for q in queries[1:3]:
        extra.extend(search(q["text"], top_k=2, param=pid))
    seen = {h["id"] for h in hits if h.get("id")}
    for h in extra:
        hid = h.get("id") or h.get("ref")
        if hid in seen:
            continue
        seen.add(hid)
        hits.append(h)
    hits = hits[:3]
    steps.append({
        "stage": "retrieve",
        "detail": f"RAG retrieved {len(hits)} governed chunk(s): "
                  + ", ".join(h["ref"] for h in hits),
        "hits": hits,
    })

    remedy = _synthesize_remedy(pid, status, hits[0] if hits else None)
    action_steps, llm_info = _actionable_steps(pid, event, hits)
    remedy["steps"] = action_steps
    steps.append({
        "stage": "reason",
        "detail": (
            f"{meta['name']} synthesised a grounded remedy from retrieved SOP/CAPA/OEM"
            + (f" via {llm_info['provider']}" if llm_info.get("used") else " (local synthesis)")
        ),
    })

    gov = _governance_gate(pid, event, remedy)
    steps.append({"stage": "govern", "detail": gov["message"]})

    return {
        "param": pid,
        "agent": meta["name"],
        "status": status,
        "action_required": True,
        "queries": queries,
        "remedy": remedy,
        "citations": [{"ref": h["ref"], "type": h["type"], "score": h["score"]} for h in hits],
        "hits": hits,
        "governance": gov,
        "steps": steps,
        "llm": llm_info,
    }


def _synthesize_remedy(pid: str, status: str, top: Dict | None) -> Dict:
    p = PARAMETERS[pid]
    base = {
        "temp": "Reduce coolant-loop setpoint in increments <= 4%; wait one residence time between steps.",
        "ph": "Stage base dosing at <= 3% increments; QA e-signature required before any pH change.",
        "press": "Initiate filter integrity check; open exhaust vent V-12 in increments <= 10%.",
        "cond": "Cross-check feed composition vs recipe; if TOC-correlated, trigger hot-recirculation.",
        "hum": "Adjust HVAC fan-speed setpoint <= 6%; inspect cooling-coil temperature and HEPA dP.",
    }[pid]
    if status == "UNDER" and pid == "temp":
        base = "Reduce cooling / raise jacket setpoint in increments <= 4%; verify no chiller overshoot."
    return {
        "summary": base,
        "grounded_on": top["ref"] if top else None,
        "confidence": round(min(0.95, 0.6 + (top["score"] if top else 0)), 2),
    }


def _cite(hits: List[Dict], prefer: str, fallback: int = 0) -> str:
    for h in hits:
        if (h.get("type") or "").upper() == prefer:
            return h.get("ref") or h.get("type") or prefer
    if hits:
        i = min(fallback, len(hits) - 1)
        return hits[i].get("ref") or hits[i].get("type") or "KB"
    return "SOP-BSC-001"


def _local_steps(pid: str, event: Dict, hits: List[Dict]) -> List[Dict]:
    p = PARAMETERS[pid]
    asset = event.get("asset") or p.asset
    lo, hi = p.control
    mid = round((lo + hi) / 2.0, 2)
    driver = (event.get("drivers") or p.drivers or ["independent_variable"])[0]
    sop = _cite(hits, "SOP", 0)
    oem = _cite(hits, "OEM", 1 if len(hits) > 1 else 0)
    capa = _cite(hits, "CAPA", -1 if hits else 0)
    templates = {
        "temp": [
            f"Immediately adjust {p.short} primary control loop setpoint toward the nominal center ({mid} {p.unit}; band {lo}–{hi}).",
            f"Inspect secondary independent variable feedback ({driver}) for thermal or flow restriction.",
            f"Verify heat-exchanger differential pressure and valve seating integrity on asset {asset}.",
            "Initiate GxP deviation record under SOP-BSC-001 and alert QA Supervisor for Part 11 e-signature.",
        ],
        "ph": [
            f"Hold the batch. Do not change the pH setpoint until QA e-signature is captured (21 CFR Part 11).",
            f"Inspect {driver} and base-dosing pump P-2 actuation lag; stage any approved dose at ≤ 3% increments.",
            f"Verify probe calibration age and drift on asset {asset} before resuming control.",
            "Open a GxP deviation under SOP-BSC-001 §6 and route to QA for dual review.",
        ],
        "press": [
            f"Immediately adjust Differential Pressure primary control loop setpoint to nominal center value ({mid} {p.unit}).",
            f"Inspect secondary independent variable feedback ({driver}) for thermal or pressure restriction.",
            f"Verify heat exchanger differential pressure and valve seating integrity on asset {asset}.",
            "Initiate GxP deviation record under SOP-BSC-001 and alert QA Supervisor for Part 11 e-signature.",
        ],
        "cond": [
            f"Isolate the WFI loop if conductivity is outside {p.trip[0]}–{p.trip[1]} {p.unit}; otherwise hold charging.",
            "Cross-check feed composition against the approved recipe; if TOC-correlated, trigger hot-recirculation.",
            f"Review resin-bed regeneration cycle count and {driver} on asset {asset}.",
            "Raise a CAPA-linked deviation and require second-person verification before the next charge.",
        ],
        "hum": [
            f"Adjust HVAC fan-speed setpoint in increments ≤ 6% toward the {lo}–{hi} {p.unit} band.",
            f"Inspect cooling-coil temperature and {driver}; a warm coil cannot dehumidify.",
            f"Check HEPA differential pressure on asset {asset}; confirm door-open transients are not the driver.",
            "Log the excursion against SOP-BSC-001 §9; escalate to facilities if coil approach exceeds 3°C.",
        ],
    }
    texts = templates.get(pid) or templates["temp"]
    cites = [sop, oem, capa, capa]
    return [{"n": i + 1, "text": t, "cite": cites[i]} for i, t in enumerate(texts)]


def _openai_steps(event: Dict, hits: List[Dict]) -> Optional[List[Dict]]:
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key or len(key) < 20:
        return None
    model = (
        os.environ.get("OPENAI_MODEL")
        or os.environ.get("LLM_MODEL")
        or "gpt-4o-mini"
    )
    refs = [h.get("ref") for h in hits if h.get("ref")]
    payload = {
        "model": model,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are SENTRA, a GxP manufacturing intelligence agent. "
                    "Return JSON {\"steps\":[{\"n\":1,\"text\":\"...\",\"cite\":\"REF\"}]} "
                    "with exactly 4 actionable remediation steps. "
                    "cite must be one of the provided document refs. No markdown."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({
                    "event": {
                        "asset": event.get("asset"),
                        "param": event.get("short") or event.get("param"),
                        "value": event.get("val"),
                        "unit": event.get("unit"),
                        "status": event.get("status"),
                        "zone": event.get("zone"),
                        "batch": event.get("batch"),
                        "drivers": event.get("drivers"),
                    },
                    "refs": refs,
                    "chunks": [
                        {"ref": h.get("ref"), "type": h.get("type"), "text": h.get("text")}
                        for h in hits
                    ],
                }),
            },
        ],
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=18) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "")
        parsed = json.loads(content)
        steps = parsed.get("steps") or []
        out = []
        for i, s in enumerate(steps[:4], start=1):
            text = (s.get("text") or "").strip()
            if not text:
                continue
            cite = s.get("cite") or (refs[min(i - 1, len(refs) - 1)] if refs else "SOP-BSC-001")
            out.append({"n": i, "text": text, "cite": cite})
        return out if len(out) >= 3 else None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def _actionable_steps(pid: str, event: Dict, hits: List[Dict]):
    llm = {"provider": "local", "model": None, "used": False}
    if (os.environ.get("LLM_PROVIDER") or "openai").lower() == "openai":
        remote = _openai_steps(event, hits)
        if remote:
            model = os.environ.get("OPENAI_MODEL") or os.environ.get("LLM_MODEL") or "gpt-4o-mini"
            return remote, {"provider": "openai", "model": model, "used": True}
        llm["provider"] = "local-fallback"
    return _local_steps(pid, event, hits), llm


def _governance_gate(pid: str, event: Dict, remedy: Dict) -> Dict:
    """Decide whether SENTRA may act autonomously or must escalate."""
    zone = event.get("zone")
    if pid == "ph":
        return {"required": True, "action": "ESCALATE",
                "approver": "QA", "control": "21 CFR Part 11 e-signature",
                "message": "pH is GxP-critical -> QA e-signature required (no autonomous action)"}
    if zone == "trip":
        return {"required": True, "action": "ESCALATE",
                "approver": "QA + Production", "control": "dual e-signature",
                "message": "trip zone -> human approval required before execution"}
    if remedy["confidence"] < 0.7:
        return {"required": True, "action": "ESCALATE",
                "approver": "Shift Lead", "control": "confidence below threshold",
                "message": "confidence < 0.70 -> route to shift lead"}
    return {"required": False, "action": "AUTONOMOUS",
            "approver": None, "control": "policy-permitted, alarm zone",
            "message": "alarm zone, high confidence -> SENTRA may act, then log to audit"}
