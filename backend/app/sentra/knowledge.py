"""
SENTRA — knowledge base (RAG source).

Production stack: sentence-transformers (BGE-small) embeddings + Qdrant/Chroma
vector store + Ollama (local LLM).
Azure equivalent: Azure AI Search + Azure OpenAI.

To keep the reference app dependency-light and runnable offline, retrieval here
uses a transparent TF-IDF cosine scorer over the same SOP/CAPA/OEM knowledge.
The interface (`search`) matches what a vector store would expose, so swapping
in Qdrant + BGE-small later is a drop-in change.
"""
from __future__ import annotations
import math
import re
from collections import Counter
from typing import Dict, List


# ---- the governed knowledge: SOP / CAPA / OEM chunks, tagged by parameter ----
SEED: List[Dict] = [
    # ---- SOP ----
    {"id": "SOP-BSC-001-temp", "type": "SOP", "ref": "SOP-BSC-001 §5", "param": "temp",
     "text": "Reactor temperature upward excursion: reduce the coolant-loop setpoint in "
             "increments of no more than 4 percent; wait one residence time (~90s) between "
             "increments and observe the trend. Verify chiller CW-3 discharge is at or below 8C. "
             "If heat-exchanger differential pressure exceeds 35 kPa, raise a cleaning work order."},
    {"id": "SOP-BSC-001-ph", "type": "SOP", "ref": "SOP-BSC-001 §6", "param": "ph",
     "text": "pH is GxP-critical: all pH setpoint changes require QA electronic signature before "
             "execution under 21 CFR Part 11. No autonomous pH adjustment is permitted. Stage base "
             "dosing pump P-2 at no more than 3 percent increments; CO2 accumulation is the most "
             "frequent driver of downward pH drift."},
    {"id": "SOP-BSC-001-press", "type": "SOP", "ref": "SOP-BSC-001 §7", "param": "press",
     "text": "Rising differential pressure on FIL-07 indicates progressive filter blockage. "
             "Initiate a filter integrity check; open exhaust vent V-12 in increments up to 10 "
             "percent. Compare against the bubble-point recorded at installation; replace filter "
             "if it fails bubble point."},
    {"id": "SOP-BSC-001-cond", "type": "SOP", "ref": "SOP-BSC-001 §8", "param": "cond",
     "text": "WFI conductivity: cross-check feed composition against the approved recipe. If TOC "
             "correlates with the rise, trigger hot-recirculation. Review resin-bed regeneration "
             "cycle count; if above 150 the bed is exhausted. Conductivity outside 600-1000 uS/cm "
             "renders water non-conforming; isolate the loop."},
    {"id": "SOP-BSC-001-hum", "type": "SOP", "ref": "SOP-BSC-001 §9", "param": "hum",
     "text": "Cleanroom humidity: adjust the HVAC fan-speed setpoint in increments up to 6 percent. "
             "Inspect cooling-coil temperature; a warm coil cannot dehumidify. Check HEPA "
             "differential pressure. Repeated door openings cause transient spikes that are not "
             "equipment faults."},
    # ---- CAPA ----
    {"id": "CAPA-2231", "type": "CAPA", "ref": "CAPA-2231", "param": "temp",
     "text": "Prior temperature excursion on BR-12 traced to heat-exchanger plate fouling reducing "
             "cooling capacity. Corrective action: cleaned the plate pack, reduced coolant setpoint "
             "4 percent during recovery, added a dP trending alarm at 30 kPa. Effective."},
    {"id": "CAPA-1987", "type": "CAPA", "ref": "CAPA-1987", "param": "ph",
     "text": "pH drift to 6.55 required a batch hold. Root cause: base dosing pump P-2 actuation "
             "lag under rapid CO2 accumulation. Corrective action: replaced actuator, revised SOP "
             "to stage dosing at 3 percent increments with QA e-signature."},
    {"id": "CAPA-2104", "type": "CAPA", "ref": "CAPA-2104", "param": "press",
     "text": "Differential pressure rose to 116 kPa during filtration; filter had exceeded "
             "validated throughput. Corrective action: filter changed, throughput limit added to "
             "the batch record as a mandatory check."},
    {"id": "CAPA-2310", "type": "CAPA", "ref": "CAPA-2310", "param": "cond",
     "text": "WFI conductivity reached 965 uS/cm due to a feed composition error. Corrective "
             "action: recipe verification step added with a second-person check before charging."},
    {"id": "CAPA-2056", "type": "CAPA", "ref": "CAPA-2056", "param": "hum",
     "text": "Cleanroom RH fell to 34 percent in winter; cooling coil running warm with low outdoor "
             "humidity. Corrective action: HVAC control loop retuned, humidification setpoint "
             "seasonally adjusted."},
    # ---- OEM ----
    {"id": "OEM-CW3-42", "type": "OEM", "ref": "OEM-MAN-CW3 §4.2", "param": "temp",
     "text": "Chiller CW-3 reduced cooling capacity: check condenser fouling, refrigerant charge, "
             "expansion valve. Maximum permitted coolant setpoint reduction is 5 percent per 90s "
             "interval; exceeding it risks thermal shock and voids the exchanger warranty."},
    {"id": "OEM-CW3-51", "type": "OEM", "ref": "OEM-MAN-CW3 §5.1", "param": "press",
     "text": "Filter skid FIL-07: rising dP indicates blockage, compare to bubble point. Falling dP "
             "may indicate a seal breach or bypass — verify the seal integrity index and escalate "
             "immediately as a containment concern."},
    {"id": "OEM-CW3-63", "type": "OEM", "ref": "OEM-MAN-CW3 §6.3", "param": "hum",
     "text": "HVAC air handling: the cooling coil is the primary dehumidifier. Coil approach above "
             "3C indicates fouling or low chilled-water flow. HEPA dP above 92 percent of loading "
             "restricts airflow and presents as an RH excursion."},
    {"id": "OEM-P2-DOSING", "type": "OEM", "ref": "OEM-P2-DOSING", "param": "ph",
     "text": "Base dosing pump P-2: stage stroke at no more than 3 percent per step. Continuous "
             "open-loop dosing is not permitted. If the actuator lag exceeds 8 seconds, take the "
             "loop to manual and escalate to QA before any further pH change."},
    {"id": "IDX-MASTER-01", "type": "MASTER_INDEX", "ref": "RAG_KB_INDEX §1", "param": None,
     "text": "Master Index maps each shop-floor tag to approved knowledge: TT-1202B → SOP-BSC-001 §5 "
             "/ CAPA-2231 / OEM-MAN-CW3; AT-3401 → SOP-BSC-001 §6 / CAPA-1987 / OEM-P2-DOSING; "
             "PT-2201 → SOP-BSC-001 §7 / CAPA-2104; CT-5501 → SOP-BSC-001 §8 / CAPA-2310; "
             "MT-6601 → SOP-BSC-001 §9 / CAPA-2056. Regulatory overlay is 21 CFR Part 11 for all GxP tags."},
    {"id": "REG-21CFR11", "type": "REGULATORY", "ref": "21 CFR Part 11", "param": None,
     "text": "21 CFR Part 11: electronic records and signatures for GxP actions. pH setpoint changes, "
             "trip-zone remedies, and any override of an autonomous agent require a unique QA "
             "e-signature, meaning of signature, and a WORM audit trail. Human Escalation owns "
             "actions the parameter agent cannot ground with near-perfect RAG confidence."},
    {"id": "REG-SOP-PH-UNDER", "type": "SOP", "ref": "SOP-PH-TRIP-UNDER", "param": "ph",
     "text": "pH UNDER trip/alarm: do not raise base flow autonomously. Stage P-2 at <= 3 percent "
             "increments, wait for trend confirmation, and obtain QA e-signature before execution. "
             "If CO2 accumulation is the driver, hold the batch and escalate."},
    {"id": "REG-SOP-PH-OVER", "type": "SOP", "ref": "SOP-PH-TRIP-OVER", "param": "ph",
     "text": "pH OVER trip/alarm: pause acid/base dosing, verify probe calibration age, and do not "
             "correct with a single large acid shot. Escalate to Human Escalation if the probe is "
             "outside calibration or the batch is in a critical phase."},
]

_WORD = re.compile(r"[a-z0-9]+")

KNOWLEDGE: List[Dict] = []
_IDF: Dict[str, float] = {}
_VECS: List = []


def _tok(text: str) -> List[str]:
    return _WORD.findall(text.lower())


def _build_index():
    docs = [_tok(k["text"]) for k in KNOWLEDGE]
    df = Counter()
    for d in docs:
        for w in set(d):
            df[w] += 1
    n = max(len(docs), 1)
    idf = {w: math.log((n + 1) / (c + 1)) + 1 for w, c in df.items()}
    vecs = []
    for d in docs:
        tf = Counter(d)
        ln = max(len(d), 1)
        vec = {w: (tf[w] / ln) * idf.get(w, 0) for w in tf}
        norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
        vecs.append((vec, norm))
    return idf, vecs


def rebuild() -> None:
    global _IDF, _VECS
    _IDF, _VECS = _build_index()


def reset_to_seed() -> None:
    global KNOWLEDGE
    KNOWLEDGE = [dict(x) for x in SEED]
    rebuild()


def add_chunks(chunks: List[Dict]) -> int:
    """Append chunks (skip duplicate ids) and rebuild the TF-IDF index."""
    existing = {k["id"] for k in KNOWLEDGE}
    added = 0
    for ch in chunks or []:
        cid = ch.get("id")
        if not cid or cid in existing:
            continue
        KNOWLEDGE.append(ch)
        existing.add(cid)
        added += 1
    if added:
        rebuild()
    return added


def remove_rag_kind(rag_kind: str) -> int:
    """Drop previously indexed uploads for one RAG slot so a re-run replaces them."""
    global KNOWLEDGE
    if not rag_kind:
        return 0
    before = len(KNOWLEDGE)
    KNOWLEDGE = [k for k in KNOWLEDGE if k.get("rag_kind") != rag_kind]
    removed = before - len(KNOWLEDGE)
    if removed:
        rebuild()
    return removed


reset_to_seed()


def search(query: str, top_k: int = 4, param: str | None = None) -> List[Dict]:
    """Return the top-k knowledge chunks for a query (TF-IDF cosine).

    If `param` is given, matching-parameter chunks get a relevance boost so an
    excursion retrieves its own SOP/CAPA/OEM first.
    """
    q = _tok(query)
    tf = Counter(q)
    qvec = {w: (tf[w] / max(len(q), 1)) * _IDF.get(w, 0) for w in tf}
    qnorm = math.sqrt(sum(v * v for v in qvec.values())) or 1.0

    scored = []
    for i, (vec, norm) in enumerate(_VECS):
        dot = sum(qvec.get(w, 0) * v for w, v in vec.items())
        score = dot / (qnorm * norm)
        if param and KNOWLEDGE[i].get("param") == param:
            score += 0.15  # relevance boost for the parameter in question
        scored.append((score, i))
    scored.sort(reverse=True)

    out = []
    for score, i in scored[:top_k]:
        k = KNOWLEDGE[i]
        out.append({
            "id": k["id"], "type": k["type"], "ref": k["ref"],
            "param": k.get("param"), "score": round(float(score), 3),
            "text": k["text"],
        })
    return out


def stats() -> Dict:
    by_source = Counter(k.get("source") or "seed" for k in KNOWLEDGE)
    return {
        "chunks": len(KNOWLEDGE),
        "by_type": dict(Counter(k["type"] for k in KNOWLEDGE)),
        "by_source": dict(by_source),
        "documents": sorted({k.get("filename") for k in KNOWLEDGE if k.get("filename")}),
    }
