"""
MOSAIC — reference data (the context sources).

In production these live in SAP, MES, RDBMS and file shares, and Flink holds
them as enrichment/lookup state. Here we mock them as in-memory tables keyed
exactly the way Flink would key them, so the contextualization join chain is
real and inspectable:

    tag  --(Asset Model)-->  asset, spec
    asset+time --(MES)-->    batch, product, phase
    product --(SAP)-->       material, spec limits, equipment
    tag  --(RDBMS/Files)-->  calibration, shift, lab notes
"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, Optional

from .domain import PARAMETERS


# ---- ASSET MODEL: tag -> asset identity + spec (static, in-memory) ----
ASSET_MODEL: Dict[str, Dict] = {
    p.tag: {
        "asset": p.asset,
        "parameter": p.id,
        "name": p.name,
        "unit": p.unit,
        "control": list(p.control),
        "alarm": list(p.alarm),
        "trip": list(p.trip),
        "line": {"BR-12": "Line 3", "FIL-07": "Line 3",
                 "WFI-02": "Utility", "CR-A1": "Cleanroom A"}.get(p.asset, "Line 3"),
    }
    for p in PARAMETERS.values()
}

# ---- MES: which batch is running on which asset (time-varying) ----
# In production these come as batch start/stop events; here each asset has a
# "current batch" that the temporal join resolves against the reading time.
MES_BATCHES: Dict[str, Dict] = {
    "BR-12":  {"batch": "OMZ-114", "product": "Omez",   "phase": "growth",
               "start": "2026-08-13T06:00:00", "end": "2026-08-13T18:00:00",
               "operator_shift": "B"},
    "FIL-07": {"batch": "NIS-208", "product": "Nise",   "phase": "filtration",
               "start": "2026-08-13T05:30:00", "end": "2026-08-13T14:00:00",
               "operator_shift": "B"},
    "WFI-02": {"batch": "WFI-DAILY", "product": "WFI",  "phase": "distribution",
               "start": "2026-08-13T00:00:00", "end": "2026-08-13T23:59:00",
               "operator_shift": "B"},
    "CR-A1":  {"batch": "MIN-051", "product": "Mintop", "phase": "packaging",
               "start": "2026-08-13T07:00:00", "end": "2026-08-13T19:00:00",
               "operator_shift": "B"},
}

# ---- SAP: product/material master + authoritative spec ----
SAP_MASTER: Dict[str, Dict] = {
    "Omez":   {"material_no": "M-0091", "family": "PPI",   "grade": "USP",
               "equipment": "BR-12", "spec_source": "SOP-BSC-001"},
    "Nise":   {"material_no": "M-0142", "family": "NSAID", "grade": "USP",
               "equipment": "FIL-07", "spec_source": "SOP-BSC-001"},
    "WFI":    {"material_no": "M-0003", "family": "Utility", "grade": "USP-WFI",
               "equipment": "WFI-02", "spec_source": "SOP-BSC-001"},
    "Mintop": {"material_no": "M-0210", "family": "Topical", "grade": "USP",
               "equipment": "CR-A1", "spec_source": "SOP-BSC-001"},
}

# ---- RDBMS / Files: supporting context, keyed by tag ----
RDBMS_FILES: Dict[str, Dict] = {
    p.tag: {
        "probe_calibration_date": "2026-08-01",
        "calibration_age_days": 12,
        "drift_mv": 0.1,
        "last_maintenance": "2026-07-20",
        "lab_note_ref": f"LAB-{p.id.upper()}-2026-0{i+1}",
    }
    for i, p in enumerate(PARAMETERS.values())
}


# ---- the keyed lookups (exactly what Flink would do in state) ----
def lookup_asset_model(tag: str) -> Optional[Dict]:
    """Match on: tag."""
    return ASSET_MODEL.get(tag)


def lookup_mes(asset: str, ts: Optional[str] = None) -> Optional[Dict]:
    """Match on: asset + timestamp (temporal join).

    Returns the batch whose [start, end] window contains the reading time.
    Falls back to the current batch if ts is omitted.
    """
    rec = MES_BATCHES.get(asset)
    if rec is None:
        return None
    if ts:
        try:
            t = datetime.fromisoformat(ts.replace("Z", ""))
            start = datetime.fromisoformat(rec["start"])
            end = datetime.fromisoformat(rec["end"])
            if not (start <= t <= end):
                # outside the window — in a real plant this would find the
                # neighbouring batch; here we still return it but flag it
                rec = {**rec, "_time_match": "outside-window"}
            else:
                rec = {**rec, "_time_match": "in-window"}
        except (ValueError, TypeError):
            rec = {**rec, "_time_match": "unparsed-ts"}
    return rec


def lookup_sap(product: str) -> Optional[Dict]:
    """Match on: product."""
    return SAP_MASTER.get(product)


def lookup_rdbms_files(tag: str) -> Optional[Dict]:
    """Match on: tag (or asset)."""
    return RDBMS_FILES.get(tag)
