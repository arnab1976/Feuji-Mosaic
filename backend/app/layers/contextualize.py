"""
Layer 3 — Contextualize (THE CORE).

Production stack: Apache Flink (stream processing / enrichment joins) +
asset/info model (JSON/Neo4j) + MES & SAP connectors.

This module performs the exact keyed-lookup chain a Flink job would run in
state, per event:

    A. Consume   -> read the raw reading (carries tag + timestamp = the keys)
    B. Enrich    -> chained keyed lookups:
                       tag            --(Asset Model)--> asset, spec
                       asset + time   --(MES)-->         batch, product, phase
                       product        --(SAP)-->         material, spec source
                       tag            --(RDBMS/Files)-->  calibration, shift
    C. Compute   -> derive status (OVER / UNDER / OK) from value vs spec
    D. Emit      -> the contextualized event

Lookups prefer uploaded (and PostgreSQL-saved) tables, then fall back to
the built-in reference_data mocks. Gold-lake readings from Ingest are the
preferred input stream.
"""
from __future__ import annotations
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from ..domain import TAG_TO_PARAM, PARAMETERS, PARAM_IDS
from .context_sources import CATALOG

SCENARIO_ZONES = ("control", "alarm", "trip")


def _as_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, str):
        v = v.strip().rstrip("%").replace(",", "").strip()
        if not v:
            return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _canon_ts(raw) -> str:
    """One second-resolution clock so file, Excel and lake copies of a reading match."""
    if raw is None or raw == "":
        return ""
    if hasattr(raw, "strftime"):
        try:
            return raw.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OverflowError, TypeError):
            pass
    s = str(raw).strip().replace("T", " ").replace("Z", "")
    s = s.replace(" UTC", "").replace(" utc", "").strip()
    cut = None
    for i, ch in enumerate(s):
        if i >= 11 and ch in "+-" and i + 1 < len(s) and s[i + 1].isdigit():
            cut = i
            break
    if cut is not None:
        s = s[:cut]
    if "." in s:
        s = s.split(".", 1)[0]
    s = " ".join(s.split())[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return s


def _canon_val(raw):
    n = _as_float(raw)
    if n is not None:
        return round(n, 4)
    if raw is None:
        return ""
    return str(raw).strip()


def _band_status(value: float, spec) -> str:
    try:
        lo, hi = float(spec[0]), float(spec[1])
    except (TypeError, ValueError, IndexError):
        return "OK"
    if value > hi:
        return "OVER"
    if value < lo:
        return "UNDER"
    return "OK"


def _as_list(v) -> Optional[List]:
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return list(v)
    return None


def gold_row_as_reading(row: Dict) -> Dict:
    """Map a Medallion Gold-lake row onto the Flink consume contract."""
    value = row.get("value")
    if value is None:
        value = row.get("val")
    return {
        "tag": row.get("tag"),
        "value": value,
        "unit": row.get("unit"),
        "timestamp": _canon_ts(row.get("timestamp") or row.get("ts") or row.get("ts_raw")),
        "param": row.get("param"),
        "asset": row.get("asset"),
        "quality": row.get("quality"),
        "source": row.get("source") or "gold-lake",
        "reading_id": row.get("reading_id"),
    }


def contextualize(reading: Dict) -> Dict:
    """Run the full enrichment chain on one raw reading.

    Returns { event, trace } where trace is the ordered list of join steps.
    """
    tag = reading.get("tag")
    value = _as_float(reading.get("value"))
    if value is None:
        value = _as_float(reading.get("val"))
    ts = _canon_ts(reading.get("timestamp") or reading.get("ts") or reading.get("ts_raw")) or (
        reading.get("timestamp") or reading.get("ts")
    )
    trace: List[Dict] = []

    # ---- A. CONSUME ----
    trace.append({
        "flink_step": "A", "name": "Consume",
        "detail": f"read raw event off the stream: {tag} = {value}",
        "keys_in": {"tag": tag, "timestamp": ts},
        "source": reading.get("source") or "stream",
    })

    if not tag:
        return {"event": None, "trace": trace, "error": "reading has no tag"}
    if value is None:
        return {"event": None, "trace": trace, "error": f"tag {tag} has no numeric value"}

    # ---- B. ENRICH (chained keyed lookups) ----
    # B.1 — Asset Model, match on tag
    asset_rec = CATALOG.lookup_asset(tag)
    if asset_rec is None:
        return {"event": None, "trace": trace,
                "error": f"tag {tag} not found in asset model"}
    asset = asset_rec["asset"]
    spec = _as_list(asset_rec.get("control")) or [0.0, 0.0]
    origin = asset_rec.get("_origin") or "reference"
    trace.append({
        "flink_step": "B", "sub": "B.1", "source": "Asset Model",
        "origin": origin,
        "match_on": "tag", "lookup": f'asset_model["{tag}"]',
        "returns": {"asset": asset, "spec": spec, "origin": origin},
        "key_out": {"asset": asset},
        "detail": f"tag {tag} -> asset {asset}, spec {spec} ({origin})",
    })

    # B.2 — MES, match on asset + timestamp (temporal join)
    mes_rec = CATALOG.lookup_mes(asset, ts)
    if mes_rec is None:
        return {"event": None, "trace": trace,
                "error": f"asset {asset} not found in MES"}
    batch = mes_rec["batch"]
    product = mes_rec["product"]
    phase = mes_rec.get("phase")
    mes_origin = mes_rec.get("_origin") or "reference"
    trace.append({
        "flink_step": "B", "sub": "B.2", "source": "MES", "temporal": True,
        "origin": mes_origin,
        "match_on": "asset + timestamp", "lookup": f'mes["{asset}" @ {ts}]',
        "returns": {"batch": batch, "product": product, "phase": phase,
                    "time_match": mes_rec.get("_time_match"),
                    "origin": mes_origin},
        "key_out": {"batch": batch, "product": product},
        "detail": f"asset {asset} @ {ts} -> batch {batch}, product {product} ({mes_origin})",
    })

    # B.3 — SAP, match on product
    sap_rec = CATALOG.lookup_sap(product)
    material = sap_rec.get("material_no") if sap_rec else None
    spec_source = sap_rec.get("spec_source") if sap_rec else None
    sap_origin = (sap_rec or {}).get("_origin") or "reference"
    trace.append({
        "flink_step": "B", "sub": "B.3", "source": "SAP",
        "origin": sap_origin,
        "match_on": "product", "lookup": f'sap_master["{product}"]',
        "returns": {"material_no": material, "spec_source": spec_source,
                    "grade": sap_rec.get("grade") if sap_rec else None,
                    "origin": sap_origin},
        "detail": f"product {product} -> material {material}, spec {spec_source} ({sap_origin})",
    })

    # B.4 — RDBMS / Files, match on tag
    rd_rec = CATALOG.lookup_rdbms(tag)
    rd_origin = (rd_rec or {}).get("_origin") or "reference"
    trace.append({
        "flink_step": "B", "sub": "B.4", "source": "RDBMS / Files",
        "origin": rd_origin,
        "match_on": "tag", "lookup": f'calibration_db["{tag}"]',
        "returns": {"calibration_age_days": rd_rec.get("calibration_age_days") if rd_rec else None,
                    "operator_shift": mes_rec.get("operator_shift"),
                    "origin": rd_origin},
        "detail": "supporting context joined (calibration, shift)",
    })

    # ---- C. COMPUTE ----
    pid = (TAG_TO_PARAM.get(tag) or reading.get("param")
           or asset_rec.get("parameter") or tag)
    p = PARAMETERS.get(pid)
    status = _band_status(value, spec)
    zone = p.zone(value) if p else ("control" if status == "OK" else "alarm")
    delta = 0.0
    try:
        lo, hi = float(spec[0]), float(spec[1])
        if status == "OVER":
            delta = round(value - hi, 3)
        elif status == "UNDER":
            delta = round(lo - value, 3)
    except (TypeError, ValueError, IndexError):
        delta = 0.0
    unit = (p.unit if p else None) or asset_rec.get("unit") or reading.get("unit") or ""
    alarm = _as_list(asset_rec.get("alarm")) or (list(p.alarm) if p else None)
    trip = _as_list(asset_rec.get("trip")) or (list(p.trip) if p else None)
    trace.append({
        "flink_step": "C", "name": "Compute",
        "detail": f"{value} vs spec {list(spec)} -> status {status}"
                  + (f" by {delta}{unit}" if delta else ""),
        "returns": {"status": status, "zone": zone, "delta": delta},
    })

    # ---- D. EMIT ----
    sap_rec = sap_rec or {}
    rd_rec = rd_rec or {}
    event = {
        "param": pid,
        "short": p.short if p else pid,
        "name": p.name if p else (asset_rec.get("name") or pid),
        "tag": tag,
        "asset": asset,
        "asset_name": asset_rec.get("name"),
        "line": asset_rec.get("line"),
        "batch": batch,
        "product": product,
        "phase": phase,
        "mes_start": mes_rec.get("start"),
        "mes_end": mes_rec.get("end"),
        "material_no": material,
        "family": sap_rec.get("family"),
        "grade": sap_rec.get("grade"),
        "equipment": sap_rec.get("equipment"),
        "val": value,
        "unit": unit,
        "spec": list(spec),
        "alarm": alarm,
        "trip": trip,
        "status": status,
        "zone": zone,
        "delta": delta,
        "operator_shift": mes_rec.get("operator_shift"),
        "calibration_age_days": rd_rec.get("calibration_age_days"),
        "probe_calibration_date": rd_rec.get("probe_calibration_date"),
        "last_maintenance": rd_rec.get("last_maintenance"),
        "lab_note_ref": rd_rec.get("lab_note_ref"),
        "spec_source": spec_source,
        "timestamp": ts,
        "reading_id": reading.get("reading_id"),
        "drivers": (p.drivers[:3] if p else []),
        "lookup_origin": {
            "asset": origin,
            "mes": mes_origin,
            "sap": sap_origin,
            "rdbms": rd_origin,
        },
    }
    trace.append({
        "flink_step": "D", "name": "Emit",
        "detail": "emit contextualized event -> Gold store, dashboards, SENTRA",
        "returns": {"event_keys": list(event.keys())},
    })

    return {"event": event, "trace": trace, "error": None}


def contextualize_gold_lake(rows: List[Dict], store_fn=None) -> Dict:
    """Map latest-per-tag Gold-lake readings through the join chain."""
    latest: Dict[str, Dict] = {}
    for row in rows or []:
        tag = row.get("tag")
        if tag:
            latest[str(tag)] = row
    events: List[Dict] = []
    errors: List[Dict] = []
    for tag, row in latest.items():
        result = contextualize(gold_row_as_reading(row))
        if result.get("event"):
            if store_fn:
                store_fn(result["event"])
            events.append(result["event"])
        else:
            errors.append({"tag": tag, "error": result.get("error")})
    return {
        "mapped": len(events),
        "failed": len(errors),
        "tags": len(latest),
        "lake_rows": len(rows or []),
        "events": events,
        "errors": errors,
    }


def _bands_for(pid: str, tag: str):
    """Control / alarm / trip bands from uploaded asset model, else domain defaults."""
    p = PARAMETERS.get(pid)
    rec = CATALOG.lookup_asset(tag) if tag else None
    control = _as_list((rec or {}).get("control")) or (list(p.control) if p else [0.0, 1.0])
    alarm = _as_list((rec or {}).get("alarm")) or (list(p.alarm) if p else None)
    trip = _as_list((rec or {}).get("trip")) or (list(p.trip) if p else None)
    lo_c, hi_c = float(control[0]), float(control[1])
    width = abs(hi_c - lo_c) or 1.0
    if not alarm:
        alarm = [lo_c - width * 0.5, hi_c + width * 0.5]
    if not trip:
        trip = [lo_c - width, hi_c + width]
    return [float(control[0]), float(control[1])], [float(alarm[0]), float(alarm[1])], [float(trip[0]), float(trip[1])]


def value_in_zone(zone: str, control, alarm, trip) -> float:
    """Deterministic value that lands in the requested scenario band (high side)."""
    lo_c, hi_c = control
    lo_a, hi_a = alarm
    lo_t, hi_t = trip
    if zone == "control":
        return round((lo_c + hi_c) / 2.0, 3)
    if zone == "alarm":
        if hi_a > hi_c:
            return round((hi_c + hi_a) / 2.0, 3)
        if lo_a < lo_c:
            return round((lo_a + lo_c) / 2.0, 3)
        return round(hi_c + max(0.05, abs(hi_c) * 0.01), 3)
    if hi_t > hi_a:
        return round((hi_a + hi_t) / 2.0, 3)
    if lo_t < lo_a:
        return round((lo_t + lo_a) / 2.0, 3)
    return round(hi_a + max(0.1, abs(hi_a) * 0.02), 3)


def merged_view(result: Dict) -> Dict:
    """Flatten the join-chain trace into one merged contextualized record."""
    ev = result.get("event") or {}
    trace = result.get("trace") or []

    def _sub(code: str) -> Dict:
        hit = next((t for t in trace if t.get("sub") == code), None)
        return dict((hit or {}).get("returns") or {})

    return {
        "tag": ev.get("tag"),
        "param": ev.get("param"),
        "value": ev.get("val"),
        "unit": ev.get("unit"),
        "status": ev.get("status"),
        "zone": ev.get("zone"),
        "delta": ev.get("delta"),
        "spec": ev.get("spec"),
        "timestamp": ev.get("timestamp"),
        "asset_model": {
            "asset": ev.get("asset"),
            "line": ev.get("line"),
            "spec": ev.get("spec"),
            **_sub("B.1"),
        },
        "mes": {
            "batch": ev.get("batch"),
            "product": ev.get("product"),
            "phase": ev.get("phase"),
            "operator_shift": ev.get("operator_shift"),
            **_sub("B.2"),
        },
        "sap": {
            "material_no": ev.get("material_no"),
            "spec_source": ev.get("spec_source"),
            **_sub("B.3"),
        },
        "rdbms": {
            "calibration_age_days": ev.get("calibration_age_days"),
            **_sub("B.4"),
        },
        "event": ev,
    }


def zone_of(value: float, control, alarm, trip) -> str:
    """Same band logic as Parameter.zone, using explicit limits."""
    lo_c, hi_c = control
    lo_a, hi_a = alarm
    lo_t, hi_t = trip
    if lo_c <= value <= hi_c:
        return "control"
    if lo_t <= value <= lo_a or hi_a <= value <= hi_t:
        return "trip"
    if lo_a <= value < lo_c or hi_c < value <= hi_a:
        return "alarm"
    return "trip"


def reading_id_of(row: Dict, index: Optional[int] = None) -> str:
    """Stable identity of one shop-floor / lake reading — the merge grain."""
    rid = row.get("reading_id") or row.get("readingid")
    if rid not in (None, ""):
        return str(rid).strip()
    idx = row.get("row_index") or row.get("bronze_offset") or row.get("kafka_offset") or index
    if idx not in (None, ""):
        try:
            n = int(float(idx))
        except (TypeError, ValueError):
            n = None
        if n is not None:
            stem = str(row.get("filename") or row.get("source") or "R")
            stem = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in Path(stem).stem)[:24] or "R"
            return f"{stem}-{n:04d}"
    tag = str(row.get("tag") or row.get("file_tag") or "")
    raw = f"{tag}|{row.get('value')}|{row.get('timestamp') or row.get('ts') or row.get('ts_raw') or ''}"
    return "R-" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]


def natural_obs_key(row: Dict) -> tuple:
    """Same physical reading even when sources assign different reading_id prefixes."""
    tag = str(row.get("tag") or row.get("file_tag") or "").strip()
    raw = row.get("value")
    if raw is None:
        raw = row.get("val")
    ts = _canon_ts(row.get("timestamp") or row.get("ts") or row.get("ts_raw") or row.get("file_ts"))
    return (tag, _canon_val(raw), ts)


def _as_observation(row: Dict, pid: Optional[str] = None) -> Optional[Dict]:
    """One reading → one observation. Parameter comes from the reading's tag, not the loop."""
    tag = str(row.get("tag") or row.get("file_tag") or "").strip()
    resolved = TAG_TO_PARAM.get(tag)
    if not resolved:
        token = row.get("param")
        if token in PARAMETERS:
            resolved = token
            tag = tag or PARAMETERS[token].tag
    if pid:
        if resolved and resolved != pid:
            return None
        if not resolved:
            p = PARAMETERS.get(pid)
            if p and tag and tag != p.tag:
                return None
            resolved = pid
            tag = tag or (p.tag if p else "")
    if not resolved:
        return None
    p = PARAMETERS.get(resolved)
    value = _as_float(row.get("value"))
    if value is None:
        value = _as_float(row.get("val"))
    if not tag or value is None:
        return None
    ts = _canon_ts(row.get("timestamp") or row.get("ts") or row.get("ts_raw") or row.get("file_ts")) or (
        row.get("timestamp") or row.get("ts") or row.get("ts_raw")
    )
    control, alarm, trip = _bands_for(resolved, tag)
    zone = zone_of(value, control, alarm, trip)
    if p and list(p.control) == control:
        zone = p.zone(value)
    return {
        "reading_id": reading_id_of(row),
        "tag": tag,
        "value": value,
        "unit": row.get("unit") or row.get("file_unit") or (p.unit if p else ""),
        "timestamp": ts,
        "param": resolved,
        "asset": row.get("asset") or (p.asset if p else ""),
        "quality": row.get("quality") or "GOOD",
        "source": row.get("source") or "observation",
        "scenario": zone,
    }


def run_observations(pid: str, zone: str, rows: List[Dict], store_fn=None) -> Dict:
    """Classify every observation for a tag, contextualize those in the chosen scenario."""
    p = PARAMETERS.get(pid)
    if p is None:
        return {"event": None, "trace": [], "error": f"unknown parameter {pid}"}
    zone = (zone or "trip").lower()
    if zone not in SCENARIO_ZONES:
        return {"event": None, "trace": [], "error": "scenario must be control, alarm or trip"}

    classified: List[Dict] = []
    seen = set()
    for i, row in enumerate(rows or [], start=1):
        obs = _as_observation(row, pid)
        if not obs:
            continue
        key = natural_obs_key(obs)
        if key in seen:
            continue
        seen.add(key)
        classified.append(obs)

    counts = {z: 0 for z in SCENARIO_ZONES}
    counts["total"] = len(classified)
    for obs in classified:
        counts[obs["scenario"]] = counts.get(obs["scenario"], 0) + 1

    matched = [obs for obs in classified if obs["scenario"] == zone]
    events: List[Dict] = []
    errors: List[Dict] = []
    first_trace: List[Dict] = []
    first_reading = None
    seen_rid = set()
    seen_nat = set()
    for obs in matched:
        nat = natural_obs_key(obs)
        if nat in seen_nat:
            continue
        rid = str(obs.get("reading_id") or "").strip()
        if rid and rid in seen_rid:
            continue
        result = contextualize(obs)
        if result.get("event"):
            ev = result["event"]
            if rid:
                ev["reading_id"] = rid
                seen_rid.add(rid)
            seen_nat.add(nat)
            if store_fn:
                store_fn(ev)
            events.append(ev)
            if first_reading is None:
                first_reading = obs
                first_trace = result.get("trace") or []
        else:
            errors.append({"reading_id": obs.get("reading_id"), "tag": obs.get("tag"),
                           "timestamp": obs.get("timestamp"),
                           "error": result.get("error")})

    first_event = events[0] if events else None
    return {
        "param": pid,
        "tag": p.tag,
        "name": p.short,
        "unit": p.unit,
        "scenario": zone,
        "counts": counts,
        "observations": classified,
        "matched": len(events),
        "events": events,
        "errors": errors,
        "reading": first_reading,
        "event": first_event,
        "trace": first_trace,
        "merged": merged_view({"event": first_event, "trace": first_trace}) if first_event else None,
        "error": None if events else (
            f"No {zone} observations for {p.tag} "
            f"(found {counts['total']}: control {counts['control']}, "
            f"alarm {counts['alarm']}, trip {counts['trip']})"
            if classified else f"No observations found for {p.tag}"
        ),
    }


def flatten_event(ev: Dict) -> Dict:
    """One table row: observation + Asset + MES + SAP + RDBMS fields."""
    spec = ev.get("spec") or []
    window = ""
    if ev.get("mes_start") or ev.get("mes_end"):
        window = f"{ev.get('mes_start') or '—'} → {ev.get('mes_end') or '—'}"
    return {
        "reading_id": ev.get("reading_id"),
        "timestamp": ev.get("timestamp"),
        "param": ev.get("param"),
        "short": ev.get("short") or ev.get("param"),
        "name": ev.get("name") or ev.get("short") or ev.get("param"),
        "tag": ev.get("tag"),
        "value": ev.get("val"),
        "unit": ev.get("unit") or "",
        "status": ev.get("status"),
        "zone": ev.get("zone"),
        "delta": ev.get("delta"),
        "spec": spec,
        "asset": ev.get("asset"),
        "asset_name": ev.get("asset_name"),
        "line": ev.get("line"),
        "batch": ev.get("batch"),
        "product": ev.get("product"),
        "phase": ev.get("phase"),
        "operator_shift": ev.get("operator_shift"),
        "mes_window": window,
        "material_no": ev.get("material_no"),
        "family": ev.get("family"),
        "grade": ev.get("grade"),
        "equipment": ev.get("equipment"),
        "spec_source": ev.get("spec_source"),
        "calibration_age_days": ev.get("calibration_age_days"),
        "probe_calibration_date": ev.get("probe_calibration_date"),
        "last_maintenance": ev.get("last_maintenance"),
        "lab_note_ref": ev.get("lab_note_ref"),
    }


def run_excursion_board(rows: List[Dict], zones: Optional[List[str]] = None) -> Dict:
    """One reading_id → one merged row. Split trip & alarm across all parameters."""
    zones = [z for z in (zones or ["trip", "alarm"]) if z in SCENARIO_ZONES]
    classified: List[Dict] = []
    seen = set()
    for i, row in enumerate(rows or [], start=1):
        obs = _as_observation(row, pid=None)
        if not obs:
            continue
        nat = natural_obs_key(obs)
        if nat in seen:
            continue
        seen.add(nat)
        classified.append(obs)

    by_param: Dict[str, Dict] = {}
    for pid in PARAM_IDS:
        p = PARAMETERS[pid]
        subset = [o for o in classified if o.get("param") == pid]
        counts = {z: 0 for z in SCENARIO_ZONES}
        counts["total"] = len(subset)
        for o in subset:
            counts[o["scenario"]] = counts.get(o["scenario"], 0) + 1
        by_param[pid] = {
            "param": pid, "tag": p.tag, "short": p.short, "name": p.name,
            "unit": p.unit, "counts": counts,
            "trip": counts.get("trip", 0),
            "alarm": counts.get("alarm", 0),
        }

    board: Dict[str, Dict] = {}
    for zone in zones:
        events: List[Dict] = []
        for obs in classified:
            if obs.get("scenario") != zone:
                continue
            result = contextualize(obs)
            if result.get("event"):
                events.append(flatten_event(result["event"]))
        events.sort(key=lambda e: (
            str(e.get("param") or ""), str(e.get("timestamp") or ""),
            str(e.get("reading_id") or ""),
        ))
        board[zone] = {"count": len(events), "events": events}
    return {
        "zones": zones,
        "grain": "reading_id",
        "parameters": [by_param[pid] for pid in PARAM_IDS],
        "by_param": by_param,
        "trip": board.get("trip") or {"count": 0, "events": []},
        "alarm": board.get("alarm") or {"count": 0, "events": []},
        "total": sum((board.get(z) or {}).get("count", 0) for z in zones),
        "observation_rows": len(classified),
        "source_rows": len(rows or []),
    }


def run_scenario(pid: str, zone: str, gold_row: Optional[Dict] = None) -> Dict:
    """Build a scenario-band reading, join the four sources, return merged event."""
    p = PARAMETERS.get(pid)
    if p is None:
        return {"event": None, "trace": [], "error": f"unknown parameter {pid}"}
    zone = (zone or "trip").lower()
    if zone not in SCENARIO_ZONES:
        return {"event": None, "trace": [], "error": "scenario must be control, alarm or trip"}

    gold_row = gold_row or {}
    tag = gold_row.get("tag") or p.tag
    control, alarm, trip = _bands_for(pid, tag)
    value = value_in_zone(zone, control, alarm, trip)
    ts = (gold_row.get("timestamp") or gold_row.get("ts")
          or datetime.now(timezone.utc).replace(microsecond=0).isoformat())
    reading = {
        "tag": tag,
        "value": value,
        "unit": gold_row.get("unit") or p.unit,
        "timestamp": ts,
        "param": pid,
        "asset": gold_row.get("asset") or p.asset,
        "quality": gold_row.get("quality") or "GOOD",
        "source": "gold-lake+scenario" if gold_row.get("tag") else "scenario",
        "scenario": zone,
    }
    result = contextualize(reading)
    result["reading"] = reading
    result["scenario"] = zone
    result["param"] = pid
    result["merged"] = merged_view(result) if result.get("event") else None
    return result
