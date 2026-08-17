"""SCADA Co-pilot Chatbot — parameter agents + Human Escalation Agent.

Answers come from the RAG KB (CAPA, Master Index, OEM, Regulatory, SOP).
The LLM is used only when RAG is not confident, and the UI is told so.
"""
from __future__ import annotations
import json
import os
import urllib.error
import urllib.request
from threading import Lock
from typing import Dict, List, Optional

from . import copilot_db
from .knowledge import search
from ..domain import PARAMETERS, PARAM_IDS
from ..layers import contextualize as ctx

AGENTS = {
    "temp": {"id": "temp", "name": "Temperature Agent", "short": "Temperature", "ic": "🌡️"},
    "ph": {"id": "ph", "name": "pH Agent", "short": "pH", "ic": "🧪"},
    "press": {"id": "press", "name": "Pressure Agent", "short": "Pressure", "ic": "⏲️"},
    "cond": {"id": "cond", "name": "Conductivity Agent", "short": "Conductivity", "ic": "💧"},
    "hum": {"id": "hum", "name": "Humidity Agent", "short": "Humidity", "ic": "☁️"},
    "escalation": {
        "id": "escalation",
        "name": "Human Escalation Agent",
        "short": "Human Escalation",
        "ic": "🛡️",
    },
}

RAG_MIN_SCORE = 0.08
CHART_HINTS = (
    "chart", "distribution", "histogram", "bar graph", "plot",
    "severity", "how many trip", "how many alarm",
)

_LAST_SI: Dict[str, Dict] = {}
_LOCK = Lock()


def _si_keys(agent_id: str, zone: Optional[str] = None) -> List[str]:
    agent = "escalation" if agent_id == "escalation" else agent_id
    keys = []
    if zone:
        keys.append(f"{agent}:{zone}")
    keys.append(agent)
    return keys


def remember_si(param: str, zone: str, payload: Dict) -> None:
    """Keep the latest SCADA Intelligence remedy so Co-pilot can refer to it."""
    ag = payload.get("agent") or {}
    ev = payload.get("event") or {}
    rec = {
        "param": param,
        "zone": zone,
        "reading_id": ev.get("reading_id") or payload.get("reading_id"),
        "tag": ev.get("tag"),
        "asset": ev.get("asset"),
        "status": ev.get("status"),
        "val": ev.get("val"),
        "unit": ev.get("unit"),
        "batch": ev.get("batch"),
        "remedy": (ag.get("remedy") or {}),
        "governance": (ag.get("governance") or {}),
        "citations": ag.get("citations") or [],
        "confidence": (ag.get("remedy") or {}).get("confidence"),
        "agent_name": ag.get("agent"),
    }
    keys = [f"{param}:{zone}", param, f"escalation:{zone}", "escalation"]
    with _LOCK:
        for k in keys:
            _LAST_SI[k] = rec
    for k in keys:
        copilot_db.save_si(k, rec)


def last_si(agent_id: str, zone: Optional[str] = None) -> Optional[Dict]:
    for key in _si_keys(agent_id, zone):
        rec = None
        with _LOCK:
            cached = _LAST_SI.get(key)
            if cached:
                rec = dict(cached)
        if rec is None:
            loaded = copilot_db.load_si(key)
            if loaded:
                with _LOCK:
                    _LAST_SI[key] = loaded
                rec = dict(loaded)
        if not rec:
            continue
        if zone and rec.get("zone") and rec.get("zone") != zone:
            if not key.endswith(f":{zone}"):
                continue
        return rec
    return None


def _rag_confident(hits: List[Dict]) -> bool:
    if not hits:
        return False
    return max(float(h.get("score") or 0) for h in hits) >= RAG_MIN_SCORE


def _wants_chart(query: str) -> bool:
    q = (query or "").lower()
    return any(h in q for h in CHART_HINTS)


def _chart(agent_id: str, rows: List[Dict]) -> List[Dict]:
    board = ctx.run_excursion_board(rows or [], ["trip", "alarm"])
    by_param = board.get("by_param") or {}
    if agent_id in PARAM_IDS:
        p = by_param.get(agent_id) or {}
        return [
            {"label": "Trip", "value": int(p.get("trip") or 0)},
            {"label": "Alarm", "value": int(p.get("alarm") or 0)},
            {"label": "In-spec", "value": int((p.get("counts") or {}).get("control") or 0)},
        ]
    out = []
    for pid in PARAM_IDS:
        p = by_param.get(pid) or {}
        short = (PARAMETERS.get(pid).short if PARAMETERS.get(pid) else pid)
        out.append({"label": short, "value": int(p.get("trip") or 0) + int(p.get("alarm") or 0)})
    return out


def _compose_rag(query: str, hits: List[Dict], agent: Dict, si: Optional[Dict]) -> str:
    parts = []
    if si and si.get("remedy"):
        rem = si["remedy"]
        tag = f"{si.get('tag') or ''} {si.get('val')}{si.get('unit') or ''}".strip()
        parts.append(
            f"Referring to the last SCADA Intelligence {si.get('zone') or 'trip'} "
            f"on {si.get('reading_id') or 'the selected reading'}"
            + (f" ({tag} · {si.get('status')})" if tag else "")
            + "."
        )
        if rem.get("summary"):
            parts.append(f"Automated remedy: {rem['summary']}")
    top = next((h for h in hits if (h.get("text") or "").strip()), None)
    if top:
        parts.append((top.get("text") or "").strip())
    extra = [h for h in hits[1:4] if (h.get("text") or "").strip() and h is not top]
    if extra:
        refs = ", ".join(
            f"{(h.get('type') or 'doc').upper()} {(h.get('ref') or h.get('id'))}"
            for h in extra
        )
        parts.append(f"Also see {refs}.")
    if agent["id"] == "escalation":
        parts.append(
            "Human Escalation owns this when a parameter agent cannot ground a "
            "near-perfect remedy. Require QA e-signature (21 CFR Part 11) and a WORM audit record before execution."
        )
    return "\n\n".join(p for p in parts if p).strip()


def _openai_answer(query: str, hits: List[Dict], agent: Dict, si: Optional[Dict]) -> Optional[str]:
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key or len(key) < 20:
        return None
    if (os.environ.get("LLM_PROVIDER") or "openai").lower() != "openai":
        return None
    model = (
        os.environ.get("OPENAI_MODEL")
        or os.environ.get("LLM_MODEL")
        or "gpt-4o-mini"
    )
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {
                "role": "system",
                "content": (
                    f"You are the MOSAIC {agent['name']}. RAG did not return a confident "
                    "match from CAPA / Master Index / OEM / Regulatory / SOP. "
                    "Answer helpfully, say that RAG was insufficient, and cite any chunks "
                    "that were retrieved. Be concise. No markdown headings."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({
                    "query": query,
                    "agent": agent["name"],
                    "last_scada_intel": si,
                    "chunks": [
                        {"ref": h.get("ref"), "type": h.get("type"),
                         "score": h.get("score"), "text": h.get("text")}
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
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        return content or None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def _default_questions(agent_id: str, zone: str) -> List[str]:
    zone = zone or "trip"
    if agent_id == "escalation":
        return [
            "What are the 21 CFR Part 11 e-signature requirements for GxP breaches?",
            "Show historical CAPA records and root causes across all parameter excursions",
            "When must the Human Escalation Agent override an autonomous parameter agent action?",
            "Generate breach severity distribution chart across all bioprocess parameters",
            "What WORM audit fields are required after a trip-zone remedy is signed?",
        ]
    p = PARAMETERS.get(agent_id)
    name = p.short if p else agent_id
    tag = p.tag if p else ""
    return [
        f"What did SCADA Intelligence recommend for the latest {name} {zone}?",
        f"Which SOP / CAPA / OEM chunks ground the {name} {zone} remedy?",
        f"Why is {tag or name} in {zone} rather than control?",
        f"Show a chart of recent {name} trip vs alarm observations",
        f"When should Human Escalation override the {name} Agent?",
    ]


def suggested_questions(agent_id: str, zone: str = "trip") -> List[str]:
    si = last_si(agent_id, zone)
    qs = _default_questions(agent_id, zone)
    if not si:
        return qs
    rid = si.get("reading_id") or "the last reading"
    p = PARAMETERS.get(si.get("param") or agent_id)
    name = p.short if p else (si.get("param") or "parameter")
    z = si.get("zone") or zone
    grounded = (si.get("remedy") or {}).get("grounded_on") or "the retrieved SOP / CAPA"
    if agent_id == "escalation":
        return [
            f"Explain the last SCADA Intel remedy for {name} {z} ({rid})",
            "Which RAG sources (CAPA, Master Index, OEM, Regulatory, SOP) ground this override?",
            f"When must Human Escalation override the {name} Agent on this {z}?",
            "Show a chart of trip vs alarm observations across all parameters",
            "What 21 CFR Part 11 e-signature and WORM fields are required before execution?",
        ]
    return [
        f"Explain the SCADA Intel remedy for reading_id {rid}",
        f"Which RAG sources (CAPA / Master Index / OEM / Regulatory / SOP) support {grounded}?",
        f"Why was this {name} {z} not handled autonomously?",
        f"Show a chart of recent {name} trip vs alarm observations",
        f"When should Human Escalation override the {name} Agent?",
    ]


def context(agent_id: str, zone: str = "trip", rows: Optional[List[Dict]] = None) -> Dict:
    agent = AGENTS.get(agent_id) or AGENTS["escalation"]
    si = last_si(agent_id, zone)
    history = copilot_db.recent(agent["id"], n=24)
    return {
        "agent": agent,
        "zone": zone,
        "last_si": si,
        "questions": suggested_questions(agent_id, zone),
        "history": history,
        "postgres": copilot_db.ping(),
        "rag_sources": ["CAPA", "Master Index", "OEM", "Regulatory", "SOP"],
    }


def chat(query: str, agent_id: str = "escalation", zone: str = "trip",
         rows: Optional[List[Dict]] = None, want_chart: bool = False) -> Dict:
    q = (query or "").strip()
    if not q:
        return {"error": "Type a query first"}
    agent = AGENTS.get(agent_id) or AGENTS["escalation"]
    param = None if agent["id"] == "escalation" else agent["id"]
    si = last_si(agent["id"], zone)
    hits = search(q, top_k=6, param=param)
    rag_ok = _rag_confident(hits)
    llm_fallback = False
    notice = None
    suggest_escalation = False
    source = "rag"

    if rag_ok:
        answer = _compose_rag(q, hits, agent, si)
        source = "rag"
    else:
        remote = _openai_answer(q, hits, agent, si)
        if remote:
            answer = remote
            source = "llm"
            llm_fallback = True
            notice = (
                "RAG (CAPA, Master Index, OEM, Regulatory, SOP) did not return a "
                "confident match. This answer was generated with the LLM."
            )
        else:
            answer = (
                f"{agent['name']} could not ground a near-perfect solution in the RAG KB "
                "(CAPA / Master Index / OEM / Regulatory / SOP), and the LLM was unavailable."
            )
            source = "none"
            suggest_escalation = agent["id"] != "escalation"
            notice = (
                "No confident RAG match. Switch to the Human Escalation Agent for GxP / "
                "Part 11 / cross-parameter guidance."
            )

    if suggest_escalation is False and not rag_ok and agent["id"] != "escalation":
        suggest_escalation = True

    chart = _chart(agent["id"], rows or []) if (want_chart or _wants_chart(q)) else None
    out = {
        "agent": agent["id"],
        "agent_name": agent["name"],
        "query": q,
        "answer": answer,
        "hits": hits,
        "source": source,
        "llm_fallback": llm_fallback,
        "notice": notice,
        "suggest_escalation": suggest_escalation,
        "last_si": si,
        "chart": chart,
        "zone": zone,
    }
    copilot_db.save_turn({
        "agent": agent["id"],
        "param": param,
        "zone": zone,
        "query": q,
        "answer": answer,
        "source": source,
        "llm_fallback": llm_fallback,
        "hits": [{"ref": h.get("ref"), "type": h.get("type"), "score": h.get("score")} for h in hits],
        "chart": chart,
    })
    return out
