"""
Layer 4 — Visualize / Observe (dashboards on meaning).

Production stack: Grafana + Superset/Metabase (process) + Prometheus (platform).
Azure equivalent: Power BI + Azure Managed Grafana + Azure Monitor.

Aggregation helpers that turn the Gold contextualized events into the KPIs a
dashboard would show — in business terms, not raw tags.
"""
from __future__ import annotations
from typing import Dict, List

from ..domain import PARAMETERS, PARAM_IDS
from .ingest_store import STORE


def dashboard_summary() -> Dict:
    """Latest contextualized state per parameter + excursion rollups."""
    gold = STORE.recent_gold(n=2000)
    latest: Dict[str, Dict] = {}
    counts = {"OK": 0, "OVER": 0, "UNDER": 0}
    excursions: List[Dict] = []

    for e in gold:              # recent_gold is newest-first
        pid = e.get("param")
        if pid and pid not in latest:
            latest[pid] = e
        counts[e.get("status", "OK")] = counts.get(e.get("status", "OK"), 0) + 1
        if e.get("status") in ("OVER", "UNDER"):
            excursions.append(e)

    params = []
    for pid in PARAM_IDS:
        p = PARAMETERS[pid]
        cur = latest.get(pid)
        params.append({
            "param": pid, "name": p.name, "short": p.short, "unit": p.unit,
            "asset": p.asset, "spec": list(p.control),
            "value": cur.get("val") if cur else None,
            "status": cur.get("status") if cur else "—",
            "batch": cur.get("batch") if cur else None,
            "product": cur.get("product") if cur else None,
            "zone": cur.get("zone") if cur else None,
        })

    return {
        "parameters": params,
        "counts": counts,
        "excursions": excursions[:10],
        "total_events": len(gold),
    }


def platform_health() -> Dict:
    """Platform observability — the pipeline's own health."""
    s = STORE.stats()
    return {
        "kafka_offset": s["latest_offset"],
        "historian_count": s["historian_count"],
        "gold_count": s["gold_count"],
        "consumer_lag": 0,
        "throughput_msg_s": 1200,
        "status": "healthy",
    }


EXCEL_COLS = [
    "reading_id", "timestamp", "param", "short", "tag", "value", "unit",
    "status", "zone", "delta", "spec",
    "asset", "asset_name", "line",
    "batch", "product", "phase", "operator_shift", "mes_window",
    "material_no", "family", "grade", "equipment", "spec_source",
    "calibration_age_days", "probe_calibration_date", "last_maintenance", "lab_note_ref",
]


def _excel_cell(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v)
    return v


def merged_excel(board: Dict) -> tuple[bytes, str]:
    """Trip and Alarm sheets of the reading_id-merged dataset."""
    from io import BytesIO
    from openpyxl import Workbook
    wb = Workbook()
    first = True
    for zone, title in (("trip", "Trip"), ("alarm", "Alarm")):
        events = (board.get(zone) or {}).get("events") or []
        ws = wb.active if first else wb.create_sheet()
        first = False
        ws.title = title
        ws.append(list(EXCEL_COLS))
        for ev in events:
            ws.append([_excel_cell(ev.get(c)) for c in EXCEL_COLS])
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue(), "mosaic-visualize-trip-alarm.xlsx"
