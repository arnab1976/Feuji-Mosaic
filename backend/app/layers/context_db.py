"""
PostgreSQL persistence for Contextualize lookup sources
(Asset Model, MES, SAP, RDBMS/Files).
"""
from __future__ import annotations
import json
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import Json, execute_values

from . import floor_db

KINDS = ("asset", "mes", "sap", "rdbms")


def ping() -> Dict:
    return floor_db.ping()


def _connect():
    return floor_db._connect()


def ensure_schema() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS context_datasets (
        id SERIAL PRIMARY KEY,
        kind TEXT NOT NULL,
        filename TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS context_rows (
        id SERIAL PRIMARY KEY,
        dataset_id INTEGER NOT NULL REFERENCES context_datasets(id) ON DELETE CASCADE,
        seq INTEGER NOT NULL,
        payload JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS context_rows_dataset_seq
        ON context_rows (dataset_id, seq);
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute("ALTER TABLE context_datasets ADD COLUMN IF NOT EXISTS slot TEXT")
            cur.execute("ALTER TABLE context_datasets ADD COLUMN IF NOT EXISTS description TEXT")
            cur.execute("ALTER TABLE context_datasets ADD COLUMN IF NOT EXISTS content_type TEXT")
            cur.execute("ALTER TABLE context_datasets ADD COLUMN IF NOT EXISTS is_file BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE context_datasets ADD COLUMN IF NOT EXISTS file_bytes BYTEA")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS context_datasets_slot_active "
                "ON context_datasets (slot, is_active)"
            )
        conn.commit()


def save_kind(kind: str, filename: str, rows: List[Dict]) -> Dict:
    return save_slot(kind, kind, filename, rows)


def save_slot(slot: str, kind: str, filename: str, rows: List[Dict],
              description: str = "", file_bytes: Optional[bytes] = None,
              content_type: str = "", is_file: bool = False) -> Dict:
    if not slot:
        raise ValueError("Missing source slot")
    if not rows and not file_bytes:
        raise ValueError(f"Nothing to save for {filename or slot} — upload a file first")
    kind = (kind or "other").lower()
    rows = rows or [{
        "filename": filename,
        "content_type": content_type or "application/octet-stream",
        "size_bytes": len(file_bytes or b""),
    }]
    ensure_schema()
    blob = psycopg2.Binary(file_bytes) if file_bytes else None
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE context_datasets SET is_active = FALSE "
                "WHERE slot = %s AND is_active = TRUE",
                (slot,),
            )
            cur.execute(
                "INSERT INTO context_datasets "
                "(kind, filename, row_count, is_active, slot, description, "
                " content_type, is_file, file_bytes) "
                "VALUES (%s, %s, %s, TRUE, %s, %s, %s, %s, %s) RETURNING id, saved_at",
                (kind, filename or "upload", len(rows), slot, description or "",
                 content_type or "", bool(is_file or file_bytes), blob),
            )
            dataset_id, saved_at = cur.fetchone()
            execute_values(
                cur,
                "INSERT INTO context_rows (dataset_id, seq, payload) VALUES %s",
                [(dataset_id, i, Json(r)) for i, r in enumerate(rows)],
            )
        conn.commit()
    return {
        "id": dataset_id,
        "slot": slot,
        "kind": kind,
        "filename": filename,
        "row_count": len(rows),
        "saved_at": saved_at.isoformat() if hasattr(saved_at, "isoformat") else str(saved_at),
    }


def delete_slot(slot: str) -> Dict:
    """Permanently remove one source (and its rows) from PostgreSQL."""
    if not slot:
        raise ValueError("Missing source slot")
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM context_datasets "
                "WHERE slot = %s OR (slot IS NULL AND kind = %s)",
                (slot, slot),
            )
            ids = [r[0] for r in cur.fetchall()]
            cur.execute(
                "DELETE FROM context_datasets "
                "WHERE slot = %s OR (slot IS NULL AND kind = %s)",
                (slot, slot),
            )
            n = cur.rowcount
        conn.commit()
    return {"slot": slot, "deleted": n, "ids": ids}


def load_active() -> Dict[str, Dict]:
    """Return {kind: latest active dataset} for Flink join restore."""
    out: Dict[str, Dict] = {}
    for rec in load_slots():
        kind = rec.get("kind") or "other"
        if kind not in out:
            out[kind] = rec
    return out


def load_slots() -> List[Dict]:
    """Every active source card (one row per slot)."""
    ensure_schema()
    out: List[Dict] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, kind, filename, row_count, saved_at, slot, description, "
                "content_type, is_file, file_bytes "
                "FROM context_datasets WHERE is_active = TRUE ORDER BY id ASC"
            )
            datasets = cur.fetchall()
            seen_slots = set()
            for (dataset_id, kind, filename, row_count, saved_at, slot, description,
                 content_type, is_file, file_bytes) in datasets:
                slot_id = slot or kind
                if slot_id in seen_slots:
                    continue
                seen_slots.add(slot_id)
                cur.execute(
                    "SELECT payload FROM context_rows WHERE dataset_id = %s ORDER BY seq",
                    (dataset_id,),
                )
                rows = []
                for (payload,) in cur.fetchall():
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    rows.append(payload)
                blob = bytes(file_bytes) if file_bytes is not None else None
                out.append({
                    "id": dataset_id,
                    "slot": slot_id,
                    "kind": kind,
                    "filename": filename,
                    "description": description or "",
                    "content_type": content_type or "",
                    "is_file": bool(is_file or blob),
                    "file_bytes": blob,
                    "row_count": row_count or len(rows),
                    "saved_at": saved_at.isoformat() if hasattr(saved_at, "isoformat") else str(saved_at),
                    "rows": rows,
                    "persisted": True,
                })
    return out
