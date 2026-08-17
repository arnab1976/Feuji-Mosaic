"""
MOSAIC — Manufacturing Operations Stream Aggregation, Integration &
Contextualization.

FastAPI backend that exposes every layer of the platform as an API:
  Layer 1  Connectivity   /api/simulate, /api/emit
  Layer 2  Ingest & Store  /api/ingest, /api/ingest/step/{n}, /api/historian
  Layer 3  Contextualize   /api/contextualize   (returns the join-chain trace)
  Layer 4  Visualize       /api/dashboard, /api/platform, /api/visualize/merged
  Layer 5  SENTRA          /api/sentra/run, /api/sentra/search, /api/rag/*
  Layer 6  Govern          /api/govern/commit, /api/audit

Run:  uvicorn app.main:app --reload --port 8000
Docs: http://localhost:8000/docs
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional, List

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from fastapi import FastAPI, Query, Request, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
import json

from .domain import PARAMETERS, PARAM_IDS
from .layers import connectivity, contextualize as ctx
from .layers.ingest_store import STORE
from .layers import visualize
from .layers import govern
from .layers import floor_db
from .layers import context_db
from .layers.context_sources import CATALOG, KINDS as CONTEXT_KINDS, parse_table, sample_csv
from .sentra import knowledge, agent
from .sentra import rag_kb
from .sentra import cycle as sentra_cycle
from .sentra import copilot as sentra_copilot

app = FastAPI(title="MOSAIC Platform API",
              description="Manufacturing Operations Stream Aggregation, "
                          "Integration & Contextualization",
              version="1.0.0")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ---------------------------------------------------------------- models
class ReadingIn(BaseModel):
    model_config = ConfigDict(extra="allow")
    tag: str
    value: float
    unit: Optional[str] = None
    timestamp: Optional[str] = None


class CommitIn(BaseModel):
    tag: str
    value: float
    timestamp: Optional[str] = None
    actor_role: str = "qa"
    signer: Optional[str] = None
    meaning: Optional[str] = None
    action: str = "commit"


class IngestStepIn(BaseModel):
    model_config = ConfigDict(extra="allow")
    readings: Optional[list] = None
    tag: Optional[str] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    timestamp: Optional[str] = None


class ConnectStepIn(BaseModel):
    param: Optional[str] = "all"
    zone: Optional[str] = None
    source: Optional[str] = "simulator"


class SentraSelectIn(BaseModel):
    param: str
    zone: str = "trip"
    reading_id: Optional[str] = None


class CopilotIn(BaseModel):
    query: str
    agent: str = "escalation"
    zone: str = "trip"
    want_chart: bool = False


# ---------------------------------------------------------------- meta
@app.get("/api/health")
def health():
    pg = floor_db.ping()
    return {"status": "ok", "platform": "MOSAIC", "layers": 8,
            "parameters": PARAM_IDS, "postgres": pg}


@app.on_event("startup")
def restore_floor():
    """Reload last saved shop-floor and context-source datasets from PostgreSQL."""
    try:
        saved = floor_db.load_active()
    except Exception:
        saved = None
    if saved and saved.get("rows"):
        connectivity.FLOOR.load(
            saved["rows"], saved["filename"],
            persisted=True, dataset_id=saved["id"],
        )
    try:
        sources = context_db.load_slots()
    except Exception:
        sources = None
    for rec in (sources or []):
        try:
            CATALOG.restore_slot(rec)
        except (ValueError, TypeError, KeyError):
            continue
    try:
        rag_kb.restore()
    except Exception:
        pass
    try:
        from .sentra import copilot_db
        copilot_db.ensure_schema()
    except Exception:
        pass


@app.get("/api/parameters")
def parameters():
    return {"parameters": [PARAMETERS[p].to_dict() for p in PARAM_IDS]}


# ---------------------------------------------------------------- L1 connectivity
@app.get("/api/emit")
def emit(param: Optional[str] = None, zone: Optional[str] = None):
    """Next raw sensor reading(s) — simulated or from the uploaded floor file."""
    if param:
        if param not in PARAMETERS:
            return JSONResponse({"error": "unknown parameter"}, status_code=400)
        return {"readings": [connectivity.emit_reading(param, zone)],
                "source": connectivity.FLOOR.mode}
    return {"readings": connectivity.emit_all(zone),
            "source": connectivity.FLOOR.mode}


@app.get("/api/connect/status")
def connect_status():
    st = connectivity.FLOOR.status()
    st["postgres"] = floor_db.ping()
    try:
        st["saved"] = floor_db.list_datasets(5)
    except Exception:
        st["saved"] = []
    return st


@app.get("/api/connect/sample.csv")
def connect_sample_csv():
    return PlainTextResponse(connectivity.sample_csv(), media_type="text/csv")


@app.get("/api/connect/sample.xlsx")
def connect_sample_xlsx():
    return Response(
        connectivity.sample_xlsx(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=mosaic-floor-sample.xlsx"},
    )


@app.post("/api/connect/upload")
async def connect_upload(request: Request):
    """Load shop-floor Excel / CSV / JSON so Connectivity streams your values."""
    ctype = (request.headers.get("content-type") or "").lower()
    try:
        if "multipart/form-data" in ctype:
            form = await request.form()
            up = form.get("file")
            filename = getattr(up, "filename", None) or ""
            if not filename:
                return JSONResponse({"error": "Choose an Excel, CSV or JSON file"}, status_code=400)
            data = await up.read()
            if not data:
                return JSONResponse({"error": "File is empty"}, status_code=400)
            rows = connectivity.parse_floor_file(filename, data)
            connectivity.stash_upload(filename, data)
            return connectivity.FLOOR.load(rows, filename)
        body = await request.json()
        rows = connectivity.parse_floor_payload(body.get("csv"), body.get("readings"))
        return connectivity.FLOOR.load(rows, body.get("filename"))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except json.JSONDecodeError:
        return JSONResponse({"error": "Could not parse JSON"}, status_code=400)


@app.post("/api/connect/load-local")
def connect_load_local():
    """Load a shop-floor file from MOSAIC folders without the Windows Open dialog."""
    try:
        return connectivity.load_local_floor()
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except OSError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/connect/simulate")
def connect_simulate():
    """Switch back to the built-in OPC-UA simulator."""
    return connectivity.FLOOR.use_simulated()


@app.post("/api/connect/save")
def connect_save():
    """Persist the current uploaded dataset in PostgreSQL so it survives restarts."""
    rows, filename = connectivity.FLOOR.snapshot_rows()
    if not rows:
        return JSONResponse({"error": "Upload a file first, then Save"}, status_code=400)
    pg = floor_db.ping()
    if not pg.get("ok"):
        return JSONResponse(
            {"error": "PostgreSQL is not reachable. Start it with: docker compose up -d postgres-floor",
             "postgres": pg},
            status_code=503,
        )
    try:
        saved = floor_db.save_dataset(filename or "upload", rows)
    except Exception as e:
        return JSONResponse({"error": str(e), "postgres": pg}, status_code=500)
    return connectivity.FLOOR.mark_persisted(saved["id"], saved["filename"]) | {
        "saved": saved,
        "postgres": pg,
        "message": f"Saved {saved['row_count']} rows to PostgreSQL",
    }


@app.post("/api/connect/delete")
def connect_delete():
    """Permanently delete the shop-floor dataset from PostgreSQL, memory, and lake files."""
    pg = floor_db.ping()
    if not pg.get("ok"):
        return JSONResponse(
            {"error": "PostgreSQL is not reachable. Start it with: docker compose up -d postgres-floor",
             "postgres": pg},
            status_code=503,
        )
    try:
        deleted = floor_db.delete_all()
    except Exception as e:
        return JSONResponse({"error": str(e), "postgres": pg}, status_code=500)
    st = connectivity.FLOOR.clear_upload()
    files = connectivity.purge_stashed_upload()
    lake = STORE.purge()
    return st | {
        "deleted": deleted,
        "purged_files": files,
        "purged_lake": lake.get("purged_files"),
        "postgres": pg,
        "message": "Permanently deleted from PostgreSQL, MOSAIC memory, and the ingest lake.",
    }


@app.post("/api/connect/reset")
def connect_reset():
    connectivity.FLOOR.reset_steps()
    return connectivity.FLOOR.status()


@app.post("/api/connect/stream/tick")
def connect_stream_tick(body: ConnectStepIn):
    """Replay the next uploaded Excel/CSV/JSON row(s) as a live 1 Hz stream."""
    try:
        return connectivity.stream_tick(body.param)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/connect/step/{step}")
def connect_step(step: int, body: ConnectStepIn):
    """Run one hop of the Connectivity path (1 OPC-UA → 2 MQTT → 3 Node-RED → 4 subscribe)."""
    try:
        return connectivity.run_step(step, body.param, body.zone, body.source)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


# ---------------------------------------------------------------- L2 ingest
@app.post("/api/ingest")
def ingest(body: IngestStepIn):
    """Run the full Ingest & Store pipeline (Kafka → Bronze → Silver → Gold lake)."""
    dumped = body.model_dump(exclude={"readings"})
    reading = dumped if dumped.get("tag") is not None and dumped.get("value") is not None else None
    try:
        return STORE.run_pipeline(reading=reading, readings=body.readings)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/ingest/status")
def ingest_status():
    return STORE.status()


@app.post("/api/ingest/reset")
def ingest_reset():
    return STORE.reset_steps()


@app.post("/api/ingest/step/{step}")
def ingest_step(step: int, body: Optional[IngestStepIn] = None):
    """Run one hop: 1 Kafka → 2 TimescaleDB → 3 MinIO Bronze → 4 DuckDB Silver → 5 Gold lake."""
    reading = None
    readings = None
    if body:
        readings = body.readings
        dumped = body.model_dump(exclude={"readings"})
        if dumped.get("tag") is not None and dumped.get("value") is not None:
            reading = dumped
    try:
        return STORE.run_step(step, reading=reading, readings=readings)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/ingest/excel/{zone}")
def ingest_excel(zone: str):
    """Download the current Bronze, Silver or Gold lake table as Excel."""
    try:
        data, filename = STORE.excel_bytes(zone)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/api/ingest/gold-lake")
def ingest_gold_lake():
    """Latest-per-tag Gold lake snapshot for Contextualize to consume."""
    return STORE.gold_lake_handoff()


@app.get("/api/historian")
def historian(n: int = 50):
    return {"readings": STORE.recent_historian(n)}


# ---------------------------------------------------------------- L3 contextualize (CORE)
@app.get("/api/context/status")
def context_status():
    st = CATALOG.status()
    st["gold_lake"] = STORE.gold_lake_handoff()
    return st


@app.get("/api/context/sample/{kind}.csv")
def context_sample_csv(kind: str):
    kind = (kind or "").lower()
    if kind not in CONTEXT_KINDS:
        return JSONResponse({"error": "kind must be asset, mes, sap or rdbms"}, status_code=400)
    names = {
        "asset": "mosaic-asset-model.csv",
        "mes": "mosaic-mes.csv",
        "sap": "mosaic-sap.csv",
        "rdbms": "mosaic-rdbms.csv",
    }
    return PlainTextResponse(
        sample_csv(kind),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{names[kind]}"'},
    )


@app.post("/api/context/upload")
async def context_upload_multi(request: Request):
    """Load one or more files of any type. Each file becomes its own source card."""
    ctype = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" not in ctype:
        return JSONResponse({"error": "Upload one or more files"}, status_code=400)
    try:
        form = await request.form()
        uploads = list(form.getlist("files")) + list(form.getlist("file"))
        seen = set()
        files = []
        for up in uploads:
            filename = getattr(up, "filename", None) or ""
            if not filename or id(up) in seen:
                continue
            seen.add(id(up))
            files.append(up)
        if not files:
            return JSONResponse({"error": "Choose one or more files"}, status_code=400)
        loaded = []
        errors = []
        for up in files:
            filename = getattr(up, "filename", None) or "upload"
            data = await up.read()
            if not data:
                errors.append(f"{filename}: empty file")
                continue
            try:
                CATALOG.ingest_bytes(filename, data)
                loaded.append(filename)
            except ValueError as e:
                errors.append(f"{filename}: {e}")
        if not loaded:
            return JSONResponse({"error": "; ".join(errors) or "No usable files"}, status_code=400)
        st = CATALOG.status()
        st["loaded"] = loaded
        st["errors"] = errors
        st["message"] = (
            f"Loaded {len(loaded)} file(s) as source cards"
            + (f" · {len(errors)} skipped" if errors else "")
        )
        return st
    except json.JSONDecodeError:
        return JSONResponse({"error": "Could not parse JSON"}, status_code=400)


@app.post("/api/context/replace/{slot}")
async def context_replace(slot: str, request: Request):
    """Replace the file on one source card. Any extension is accepted."""
    ctype = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" not in ctype:
        return JSONResponse({"error": "Upload a file"}, status_code=400)
    try:
        form = await request.form()
        up = form.get("file")
        filename = getattr(up, "filename", None) or ""
        if not filename:
            return JSONResponse({"error": "Choose a file"}, status_code=400)
        data = await up.read()
        return CATALOG.ingest_bytes(filename, data, slot=slot)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/context/file/{slot}")
def context_file(slot: str):
    """Serve an uploaded image / PDF / binary for preview or download."""
    rec = CATALOG.file_payload(slot)
    if not rec:
        return JSONResponse({"error": "No file on this card"}, status_code=404)
    return Response(
        content=rec["data"],
        media_type=rec["content_type"],
        headers={"Content-Disposition": f'inline; filename="{rec["filename"]}"'},
    )


@app.post("/api/context/upload/{kind}")
async def context_upload(kind: str, request: Request):
    """Load one lookup table (asset / mes / sap / rdbms) from Excel, CSV or JSON."""
    kind = (kind or "").lower()
    if kind not in CONTEXT_KINDS:
        return JSONResponse({"error": "kind must be asset, mes, sap or rdbms"}, status_code=400)
    ctype = (request.headers.get("content-type") or "").lower()
    try:
        if "multipart/form-data" in ctype:
            form = await request.form()
            up = form.get("file")
            filename = getattr(up, "filename", None) or ""
            if not filename:
                return JSONResponse({"error": "Choose an Excel, CSV or JSON file"}, status_code=400)
            data = await up.read()
            rows = parse_table(filename, data)
            return CATALOG.load(kind, rows, filename)
        body = await request.json()
        rows = body.get("rows") or body.get("records") or []
        filename = body.get("filename") or f"{kind}.json"
        if isinstance(rows, str):
            rows = parse_table(filename, rows.encode("utf-8"))
        return CATALOG.load(kind, rows, filename)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except json.JSONDecodeError:
        return JSONResponse({"error": "Could not parse JSON"}, status_code=400)


@app.post("/api/context/save/{slot}")
def context_save_slot(slot: str):
    """Persist one source card in PostgreSQL."""
    snap = CATALOG.snapshot()
    rec = snap.get(slot)
    if not rec or not (rec.get("rows") or rec.get("file_bytes")):
        return JSONResponse({"error": "Upload this source first, then Save"}, status_code=400)
    pg = context_db.ping()
    if not pg.get("ok"):
        return JSONResponse(
            {"error": "PostgreSQL is not reachable. Start it with: docker compose up -d postgres-floor",
             "postgres": pg},
            status_code=503,
        )
    try:
        saved = context_db.save_slot(
            slot, rec.get("kind") or "other",
            rec.get("filename") or slot, rec.get("rows") or [],
            rec.get("description") or "",
            file_bytes=rec.get("file_bytes"),
            content_type=rec.get("content_type") or "",
            is_file=bool(rec.get("is_file")),
        )
        CATALOG.mark_persisted(slot, saved["id"], rec.get("filename") or slot)
    except Exception as e:
        return JSONResponse({"error": str(e), "postgres": pg}, status_code=500)
    st = CATALOG.status()
    st["saved"] = saved
    st["postgres"] = pg
    st["message"] = f"Saved {saved['row_count']} rows for {saved['filename']} to PostgreSQL"
    return st


@app.post("/api/context/delete/{slot}")
def context_delete_slot(slot: str):
    """Permanently delete one source from PostgreSQL and memory."""
    pg = context_db.ping()
    if not pg.get("ok"):
        return JSONResponse(
            {"error": "PostgreSQL is not reachable. Start it with: docker compose up -d postgres-floor",
             "postgres": pg},
            status_code=503,
        )
    try:
        deleted = context_db.delete_slot(slot)
    except Exception as e:
        return JSONResponse({"error": str(e), "postgres": pg}, status_code=500)
    st = CATALOG.drop_slot(slot)
    st["deleted"] = deleted
    st["postgres"] = pg
    st["message"] = "Source deleted from PostgreSQL. Upload the file again to restore it."
    return st


@app.post("/api/context/save")
def context_save():
    """Persist every uploaded lookup table in PostgreSQL."""
    snap = CATALOG.snapshot()
    if not snap:
        return JSONResponse(
            {"error": "Upload at least one source first"},
            status_code=400,
        )
    pg = context_db.ping()
    if not pg.get("ok"):
        return JSONResponse(
            {"error": "PostgreSQL is not reachable. Start it with: docker compose up -d postgres-floor",
             "postgres": pg},
            status_code=503,
        )
    saved = []
    try:
        for slot, rec in snap.items():
            s = context_db.save_slot(
                slot, rec.get("kind") or "other",
                rec.get("filename") or slot, rec.get("rows") or [],
                rec.get("description") or "",
                file_bytes=rec.get("file_bytes"),
                content_type=rec.get("content_type") or "",
                is_file=bool(rec.get("is_file")),
            )
            CATALOG.mark_persisted(slot, s["id"], rec.get("filename") or slot)
            saved.append(s)
    except Exception as e:
        return JSONResponse({"error": str(e), "postgres": pg}, status_code=500)
    st = CATALOG.status()
    st["saved"] = saved
    st["postgres"] = pg
    st["message"] = (
        f"Saved {sum(s['row_count'] for s in saved)} rows across "
        f"{len(saved)} source(s) to PostgreSQL"
    )
    return st


@app.post("/api/contextualize")
def contextualize_reading(reading: ReadingIn, store: bool = Query(True)):
    """Run the keyed-lookup enrichment chain — Layer 3 (CORE).

    Returns the contextualized event AND the step-by-step Flink trace.
    """
    result = ctx.contextualize(reading.model_dump())
    if result.get("event") and store:
        STORE.write_gold(result["event"])
    return result


def _floor_rows(param: Optional[str] = None) -> list:
    """Shop-floor observations, optionally filtered to one parameter.

    Grain is the physical reading (tag + value + timestamp), not reading_id.
    Floor file, stream copy and Gold lake often carry the same 7 points with
    different prefixes (mosaic-floor-data-* vs Excel-*). Keep the first source.
    """
    p = PARAMETERS.get(param) if param else None
    seen = set()
    rows: list = []

    def _add(row):
        if not row:
            return
        rec = dict(row)
        if rec.get("value") is None and rec.get("val") is not None:
            rec["value"] = rec.get("val")
        if not rec.get("timestamp"):
            rec["timestamp"] = rec.get("ts") or rec.get("ts_raw") or rec.get("file_ts")
        rec["timestamp"] = ctx._canon_ts(rec.get("timestamp")) or rec.get("timestamp")
        rec["reading_id"] = ctx.reading_id_of(rec, len(rows) + 1)
        tag = rec.get("tag") or (p.tag if p else None)
        if p and tag != p.tag and rec.get("param") != param:
            return
        natural = ctx.natural_obs_key(rec)
        if natural in seen:
            return
        seen.add(natural)
        rows.append(rec)

    for r in connectivity.FLOOR.rows:
        _add(r)
    for r in connectivity.FLOOR.last_stream or []:
        _add(r)
    # Lake is the ingested copy of the same floor file — only when nothing is on the floor.
    if not rows:
        for r in STORE.gold_lake_rows():
            _add(r)
    return rows


@app.get("/api/contextualize/observations")
def contextualize_observations(
    param: str = "hum",
    zone: str = "trip",
    store: bool = Query(True),
):
    """Pull every observation for a tag, keep those in the chosen scenario, join four sources."""
    if param not in PARAMETERS:
        return JSONResponse({"error": "unknown parameter"}, status_code=400)
    zone = (zone or "trip").lower()
    if zone not in ctx.SCENARIO_ZONES:
        return JSONResponse({"error": "scenario must be control, alarm or trip"}, status_code=400)
    result = ctx.run_observations(
        param, zone, _floor_rows(param), STORE.write_gold if store else None
    )
    result["sources"] = CATALOG.status()["sources"]
    return result


@app.get("/api/contextualize/scenario")
def contextualize_scenario(
    param: str = "temp",
    zone: str = "trip",
    store: bool = Query(True),
):
    """Force a reading into control / alarm / trip, then join Asset → MES → SAP → RDBMS."""
    if param not in PARAMETERS:
        return JSONResponse({"error": "unknown parameter"}, status_code=400)
    zone = (zone or "trip").lower()
    if zone not in ctx.SCENARIO_ZONES:
        return JSONResponse({"error": "scenario must be control, alarm or trip"}, status_code=400)
    gold_row = None
    p = PARAMETERS[param]
    for row in STORE.gold_lake_handoff().get("snapshot") or []:
        if row.get("param") == param or row.get("tag") == p.tag:
            gold_row = row
            break
    result = ctx.run_scenario(param, zone, gold_row)
    if result.get("event") and store:
        STORE.write_gold(result["event"])
    result["sources"] = CATALOG.status()["sources"]
    return result


@app.post("/api/contextualize/from-lake")
def contextualize_from_lake():
    """Join the Ingest Gold lake (latest per tag) to the four context sources."""
    handoff = STORE.gold_lake_handoff()
    if not handoff.get("ready"):
        return JSONResponse(
            {"error": "Finish Ingest hop 5 (Gold lake) first so Contextualize has readings to map"},
            status_code=400,
        )
    result = ctx.contextualize_gold_lake(handoff.get("snapshot") or [], STORE.write_gold)
    result["gold_lake"] = {
        "count": handoff.get("count"),
        "tags": handoff.get("tags"),
        "gold_path": handoff.get("gold_path"),
    }
    result["sources"] = CATALOG.status()["sources"]
    return result


@app.get("/api/gold")
def gold(n: int = 50, param: Optional[str] = None):
    return {"events": STORE.recent_gold(n, param)}


# ---------------------------------------------------------------- L4 visualize
@app.get("/api/dashboard")
def dashboard():
    return visualize.dashboard_summary()


@app.get("/api/platform")
def platform():
    return visualize.platform_health()


@app.get("/api/visualize/merged")
def visualize_merged():
    """All trip & alarm observations, joined to Asset, MES, SAP and RDBMS."""
    result = ctx.run_excursion_board(_floor_rows())
    result["sources"] = CATALOG.status()["sources"]
    result["platform"] = visualize.platform_health()
    return result


@app.get("/api/visualize/excel")
def visualize_excel():
    """Download trip & alarm merged datasets as Excel."""
    board = ctx.run_excursion_board(_floor_rows())
    data, filename = visualize.merged_excel(board)
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


# ---------------------------------------------------------------- L5 SENTRA
@app.get("/api/sentra/search")
def sentra_search(q: str, param: Optional[str] = None, k: int = 4):
    return {"results": knowledge.search(q, top_k=k, param=param)}


@app.get("/api/sentra/board")
def sentra_board(param: str = "temp", zone: str = "trip"):
    """Merged contextualized rows for one parameter × trip|alarm."""
    if param not in PARAMETERS:
        return JSONResponse({"error": "unknown parameter"}, status_code=400)
    zone = (zone or "trip").lower()
    if zone not in ("trip", "alarm"):
        return JSONResponse({"error": "scenario must be trip or alarm"}, status_code=400)
    return sentra_cycle.board(param, zone, _floor_rows(param))


@app.post("/api/sentra/decompose")
def sentra_decompose(inp: SentraSelectIn):
    """Flink-decompose one selected reading_id and emit retrieval queries."""
    if inp.param not in PARAMETERS:
        return JSONResponse({"error": "unknown parameter"}, status_code=400)
    zone = (inp.zone or "trip").lower()
    if zone not in ("trip", "alarm"):
        return JSONResponse({"error": "scenario must be trip or alarm"}, status_code=400)
    if not (inp.reading_id or "").strip():
        return JSONResponse({"error": "Select a reading_id first"}, status_code=400)
    result = sentra_cycle.decompose(inp.param, zone, inp.reading_id.strip(), _floor_rows(inp.param))
    if result.get("error") and not result.get("event"):
        return JSONResponse({"error": result["error"]}, status_code=400)
    return result


@app.post("/api/sentra/agent")
def sentra_activate(inp: SentraSelectIn):
    """Activate the parameter's LLM multi-agent and synthesise a RemedyCard."""
    if inp.param not in PARAMETERS:
        return JSONResponse({"error": "unknown parameter"}, status_code=400)
    zone = (inp.zone or "trip").lower()
    if zone not in ("trip", "alarm"):
        return JSONResponse({"error": "scenario must be trip or alarm"}, status_code=400)
    if not (inp.reading_id or "").strip():
        return JSONResponse({"error": "Select a reading_id first"}, status_code=400)
    result = sentra_cycle.activate(
        inp.param, zone, inp.reading_id.strip(), _floor_rows(inp.param), STORE.write_gold
    )
    if result.get("error") and not result.get("event"):
        return JSONResponse({"error": result["error"]}, status_code=400)
    return result


@app.post("/api/sentra/run")
def sentra_run(reading: ReadingIn):
    """Contextualize a reading then run the SENTRA agent on the event."""
    result = ctx.contextualize(reading.dict())
    if not result.get("event"):
        return JSONResponse({"error": result.get("error")}, status_code=400)
    STORE.write_gold(result["event"])
    ag = agent.run_agent(result["event"])
    return {"event": result["event"], "trace": result["trace"], "agent": ag}


# ---------------------------------------------------------------- L6 govern
@app.get("/api/govern/evaluate")
def govern_evaluate(param: str = "ph", zone: str = "alarm"):
    """Policy decision for a parameter — no commit yet."""
    if param not in PARAMETERS:
        return JSONResponse({"error": "unknown parameter"}, status_code=400)
    zone = (zone or "alarm").lower()
    rd = connectivity.simulate_reading(param, zone if zone in ("control", "alarm", "trip") else "alarm")
    result = ctx.contextualize(rd)
    if not result.get("event"):
        return JSONResponse({"error": result.get("error")}, status_code=400)
    ag = agent.run_agent(result["event"])
    return {
        "reading": rd,
        "event": result["event"],
        "agent": ag,
        "policy": govern.policy_view(result["event"], ag),
    }


@app.post("/api/govern/commit")
def govern_commit(inp: CommitIn):
    """Full pipeline: contextualize -> SENTRA -> governance decision + audit."""
    reading = {"tag": inp.tag, "value": inp.value, "timestamp": inp.timestamp}
    result = ctx.contextualize(reading)
    if not result.get("event"):
        return JSONResponse({"error": result.get("error")}, status_code=400)
    STORE.write_gold(result["event"])
    ag = agent.run_agent(result["event"])
    decision = govern.decide_and_commit(
        result["event"], ag, inp.actor_role,
        signer=inp.signer, meaning=inp.meaning, action=inp.action,
    )
    return {
        "event": result["event"],
        "agent": ag,
        "decision": decision,
        "policy": govern.policy_view(result["event"], ag),
    }


@app.get("/api/audit")
def audit(n: int = 30):
    return {"entries": govern.AUDIT.recent(n), "verify": govern.AUDIT.verify()}


@app.get("/api/copilot/context")
def copilot_context(agent: str = "escalation", zone: str = "trip"):
    """Last SCADA Intel remedy + top questions + stored chat for one agent."""
    zone = (zone or "trip").lower()
    if zone not in ("trip", "alarm"):
        zone = "trip"
    return sentra_copilot.context(agent, zone, _floor_rows())


@app.get("/api/copilot/history")
def copilot_history(agent: Optional[str] = None, n: int = 40):
    from .sentra import copilot_db
    return {"turns": copilot_db.recent(agent, n), "postgres": copilot_db.ping()}


@app.post("/api/copilot/chat")
def copilot_chat(inp: CopilotIn):
    """RAG-first Co-pilot. LLM only if CAPA/OEM/SOP/Regulatory/Index cannot answer."""
    zone = (inp.zone or "trip").lower()
    if zone not in ("trip", "alarm"):
        zone = "trip"
    result = sentra_copilot.chat(
        inp.query, inp.agent, zone, _floor_rows(), want_chart=bool(inp.want_chart),
    )
    if result.get("error"):
        return JSONResponse({"error": result["error"]}, status_code=400)
    return result


# ---------------------------------------------------------------- RAG knowledge base
@app.get("/api/rag/status")
def rag_status():
    return rag_kb.status()


@app.get("/api/rag/sample/{kind}.pdf")
def rag_sample_pdf(kind: str):
    try:
        data, filename = rag_kb.sample_pdf(kind)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return Response(
        content=data,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@app.post("/api/rag/ingest")
async def rag_ingest(
    files: List[UploadFile] = File(...),
    doc_type: str = Form("auto"),
):
    if not files:
        return JSONResponse({"error": "Drop one or more PDFs"}, status_code=400)
    jobs = []
    errors = []
    for f in files:
        name = f.filename or "document.pdf"
        data = await f.read()
        try:
            jobs.append(rag_kb.ingest_file(name, data, doc_type))
        except Exception as e:
            errors.append({"filename": name, "error": str(e)})
    return {"jobs": jobs, "errors": errors, "index": knowledge.stats()}


@app.post("/api/rag/upload")
async def rag_upload_multi(request: Request):
    """Load one or more RAG files of any type. Each file becomes a source card."""
    ctype = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" not in ctype:
        return JSONResponse({"error": "Upload one or more files"}, status_code=400)
    form = await request.form()
    uploads = list(form.getlist("files")) + list(form.getlist("file"))
    seen = set()
    files = []
    for up in uploads:
        filename = getattr(up, "filename", None) or ""
        if not filename or id(up) in seen:
            continue
        seen.add(id(up))
        files.append(up)
    if not files:
        return JSONResponse({"error": "Choose one or more files"}, status_code=400)
    loaded = []
    errors = []
    for up in files:
        filename = getattr(up, "filename", None) or "upload"
        data = await up.read()
        if not data:
            errors.append(f"{filename}: empty file")
            continue
        try:
            rag_kb.CATALOG.ingest_bytes(filename, data)
            loaded.append(filename)
        except ValueError as e:
            errors.append(f"{filename}: {e}")
    if not loaded:
        return JSONResponse({"error": "; ".join(errors) or "No usable files"}, status_code=400)
    st = rag_kb.status()
    st["loaded"] = loaded
    st["errors"] = errors
    st["message"] = (
        f"Loaded {len(loaded)} file(s) as RAG cards"
        + (f" · {len(errors)} skipped" if errors else "")
    )
    return st


@app.post("/api/rag/replace/{slot}")
async def rag_replace(slot: str, file: UploadFile = File(...)):
    name = file.filename or "upload"
    data = await file.read()
    try:
        rag_kb.CATALOG.ingest_bytes(name, data, slot=slot)
        return rag_kb.status()
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/rag/file/{slot}")
def rag_file(slot: str):
    rec = rag_kb.CATALOG.file_payload(slot)
    if not rec:
        return JSONResponse({"error": "No file on this card"}, status_code=404)
    return Response(
        content=rec["data"],
        media_type=rec["content_type"],
        headers={"Content-Disposition": f'inline; filename="{rec["filename"]}"'},
    )


@app.post("/api/rag/upload/{kind}")
async def rag_upload(kind: str, file: UploadFile = File(...)):
    kind = (kind or "").lower().replace(" ", "_").replace("-", "_")
    if kind not in rag_kb.KINDS:
        return JSONResponse(
            {"error": "kind must be capa, master_index, oem, regulatory or sop"},
            status_code=400,
        )
    name = file.filename or f"{kind}.pdf"
    data = await file.read()
    try:
        return rag_kb.CATALOG.receive(kind, name, data)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/rag/save/{slot}")
def rag_save_slot(slot: str):
    try:
        return rag_kb.save_one(slot)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/rag/delete/{slot}")
def rag_delete_slot(slot: str):
    try:
        return rag_kb.delete_one(slot)
    except ConnectionError as e:
        return JSONResponse({"error": str(e)}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/rag/save")
def rag_save():
    try:
        return rag_kb.save_all()
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/rag/pipeline")
def rag_pipeline():
    try:
        return rag_kb.run_pipeline()
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------- serve frontend
_FRONTEND = Path(__file__).resolve().parents[2] / "frontend"
if _FRONTEND.exists():
    @app.get("/")
    def index():
        return FileResponse(
            str(_FRONTEND / "index.html"),
            headers={"Cache-Control": "no-store, max-age=0"},
        )
    app.mount("/app", StaticFiles(directory=str(_FRONTEND), html=True), name="frontend")
