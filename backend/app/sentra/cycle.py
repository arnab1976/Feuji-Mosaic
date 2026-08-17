"""
SENTRA cycle helpers — board of merged observations, Flink decompose of one
reading_id, query transform, multi-agent activation.
"""
from __future__ import annotations
from typing import Dict, List, Optional

from ..domain import PARAMETERS
from ..layers import contextualize as ctx
from . import agent
from . import copilot as copilot_mod


def board(param: str, zone: str, rows: List[Dict]) -> Dict:
    zone = (zone or "trip").lower()
    if zone not in ("trip", "alarm"):
        return {"error": "scenario must be trip or alarm", "events": []}
    if param not in PARAMETERS:
        return {"error": f"unknown parameter {param}", "events": []}
    result = ctx.run_observations(param, zone, rows, store_fn=None)
    events = list(result.get("events") or [])
    fallback = False
    if not events:
        gold_row = None
        p = PARAMETERS[param]
        for row in rows or []:
            if row.get("param") == param or row.get("tag") == p.tag:
                gold_row = row
                break
        scen = ctx.run_scenario(param, zone, gold_row)
        if scen.get("event"):
            ev = scen["event"]
            if not ev.get("reading_id"):
                ev["reading_id"] = f"scenario-{param}-{zone}"
            reading = scen.get("reading") or {}
            if not reading.get("reading_id"):
                reading["reading_id"] = ev["reading_id"]
            events = [ev]
            result["trace"] = scen.get("trace") or []
            result["reading"] = reading
            result["error"] = None
            fallback = True
    flat = [ctx.flatten_event(e) for e in events]
    p = PARAMETERS[param]
    return {
        "param": param,
        "zone": zone,
        "tag": result.get("tag") or p.tag,
        "name": result.get("name") or p.short,
        "unit": result.get("unit") or p.unit,
        "counts": result.get("counts") or {},
        "events": flat,
        "matched": len(flat),
        "fallback": fallback,
        "reading_ids": [e.get("reading_id") for e in flat if e.get("reading_id")],
        "error": result.get("error"),
        "agents": list(agent.AGENTS.values()),
    }


def decompose(param: str, zone: str, reading_id: str, rows: List[Dict]) -> Dict:
    zone = (zone or "trip").lower()
    if param not in PARAMETERS:
        return {"error": f"unknown parameter {param}"}
    result = ctx.run_observations(param, zone, rows, store_fn=None)
    events = list(result.get("events") or [])
    observations = list(result.get("observations") or [])
    want = str(reading_id or "").strip()
    obs = next((o for o in observations if str(o.get("reading_id") or "") == want), None)
    ev = next((e for e in events if str(e.get("reading_id") or "") == want), None)
    if obs is None and ev is None:
        board_res = board(param, zone, rows)
        if board_res.get("fallback") and board_res.get("events"):
            gold_row = None
            p = PARAMETERS[param]
            for row in rows or []:
                if row.get("param") == param or row.get("tag") == p.tag:
                    gold_row = row
                    break
            scen = ctx.run_scenario(param, zone, gold_row)
            if scen.get("event"):
                ev = scen["event"]
                if not ev.get("reading_id"):
                    ev["reading_id"] = board_res["events"][0].get("reading_id")
                obs = scen.get("reading")
                result["trace"] = scen.get("trace") or []
        if obs is None and ev is None:
            return {"error": f"reading_id {want or '(empty)'} not in {zone} {param} set"}
    if obs is not None:
        joined = ctx.contextualize(obs)
        event = joined.get("event") or ev
        trace = joined.get("trace") or []
        reading = obs
    else:
        event = ev
        trace = result.get("trace") or []
        reading = {
            "tag": event.get("tag"),
            "value": event.get("val"),
            "timestamp": event.get("timestamp"),
            "reading_id": event.get("reading_id"),
            "param": event.get("param"),
        }
    if not event:
        return {"error": "Could not contextualize this reading"}
    queries = agent.event_to_queries(event)
    return {
        "param": param,
        "zone": zone,
        "reading_id": event.get("reading_id"),
        "reading": reading,
        "event": event,
        "trace": trace,
        "merged": ctx.flatten_event(event),
        "queries": queries,
        "error": None,
    }


def activate(param: str, zone: str, reading_id: str, rows: List[Dict],
             store_fn=None) -> Dict:
    dec = decompose(param, zone, reading_id, rows)
    if dec.get("error") or not dec.get("event"):
        return dec
    event = dec["event"]
    if store_fn:
        store_fn(event)
    ag = agent.run_agent(event)
    dec["agent"] = ag
    copilot_mod.remember_si(param, zone, dec)
    return dec
