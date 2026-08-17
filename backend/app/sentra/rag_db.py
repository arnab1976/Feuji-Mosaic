"""
PostgreSQL persistence for Build RAG KB documents
(CAPA, Master Index, OEM, Regulatory, SOP, and extra files).
"""
from __future__ import annotations
from typing import Dict, List, Optional

from psycopg2 import Binary

from ..layers import floor_db

KINDS = ("capa", "master_index", "oem", "regulatory", "sop")


def ping() -> Dict:
    return floor_db.ping()


def _connect():
    return floor_db._connect()


def ensure_schema() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS rag_documents (
        id SERIAL PRIMARY KEY,
        kind TEXT NOT NULL,
        filename TEXT NOT NULL,
        content BYTEA NOT NULL,
        byte_count INTEGER NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS rag_documents_kind_active
        ON rag_documents (kind, is_active);
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute("ALTER TABLE rag_documents ADD COLUMN IF NOT EXISTS slot TEXT")
            cur.execute("ALTER TABLE rag_documents ADD COLUMN IF NOT EXISTS description TEXT")
            cur.execute("ALTER TABLE rag_documents ADD COLUMN IF NOT EXISTS content_type TEXT")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS rag_documents_slot_active "
                "ON rag_documents (slot, is_active)"
            )
        conn.commit()


def save_kind(kind: str, filename: str, data: bytes) -> Dict:
    return save_slot(kind, kind, filename, data)


def save_slot(slot: str, kind: str, filename: str, data: bytes,
              description: str = "", content_type: str = "") -> Dict:
    if not slot:
        raise ValueError("Missing RAG slot")
    payload = data or b""
    if not payload:
        raise ValueError(f"Nothing to save for {filename or slot} — upload a file first")
    kind = (kind or "other").lower()
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE rag_documents SET is_active = FALSE "
                "WHERE slot = %s AND is_active = TRUE",
                (slot,),
            )
            cur.execute(
                "INSERT INTO rag_documents "
                "(kind, filename, content, byte_count, is_active, slot, description, content_type) "
                "VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s) RETURNING id, saved_at",
                (kind, filename or "upload", Binary(payload), len(payload),
                 slot, description or "", content_type or ""),
            )
            dataset_id, saved_at = cur.fetchone()
        conn.commit()
    return {
        "id": dataset_id,
        "slot": slot,
        "kind": kind,
        "filename": filename,
        "byte_count": len(payload),
        "saved_at": saved_at.isoformat() if hasattr(saved_at, "isoformat") else str(saved_at),
    }


def delete_slot(slot: str) -> Dict:
    if not slot:
        raise ValueError("Missing RAG slot")
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM rag_documents "
                "WHERE slot = %s OR (slot IS NULL AND kind = %s)",
                (slot, slot),
            )
            ids = [r[0] for r in cur.fetchall()]
            cur.execute(
                "DELETE FROM rag_documents "
                "WHERE slot = %s OR (slot IS NULL AND kind = %s)",
                (slot, slot),
            )
            n = cur.rowcount
        conn.commit()
    return {"slot": slot, "deleted": n, "ids": ids}


def load_active() -> Dict[str, Dict]:
    """Latest active document per kind (join/pipeline restore)."""
    out: Dict[str, Dict] = {}
    for rec in load_slots():
        kind = rec.get("kind") or "other"
        if kind not in out:
            out[kind] = rec
    return out


def load_slots() -> List[Dict]:
    """Every active RAG document card."""
    ensure_schema()
    out: List[Dict] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, kind, filename, content, byte_count, saved_at, "
                "slot, description, content_type "
                "FROM rag_documents WHERE is_active = TRUE ORDER BY id ASC"
            )
            seen = set()
            for (dataset_id, kind, filename, content, byte_count, saved_at,
                 slot, description, content_type) in cur.fetchall():
                slot_id = slot or kind
                if slot_id in seen:
                    continue
                seen.add(slot_id)
                raw = bytes(content) if content is not None else b""
                out.append({
                    "id": dataset_id,
                    "slot": slot_id,
                    "kind": kind,
                    "filename": filename,
                    "description": description or "",
                    "content_type": content_type or "",
                    "data": raw,
                    "byte_count": byte_count or len(raw),
                    "saved_at": saved_at.isoformat() if hasattr(saved_at, "isoformat") else str(saved_at),
                    "persisted": True,
                })
    return out
