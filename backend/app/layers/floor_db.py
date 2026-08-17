"""
PostgreSQL persistence for uploaded shop-floor datasets.

Saved rows are replayed by Connectivity (and later layers) so the user does
not have to re-upload Excel/CSV/JSON after a restart.
"""
from __future__ import annotations
import os
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_values

PG_HOST = os.environ.get("MOSAIC_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("MOSAIC_PG_PORT", "5434"))
PG_USER = os.environ.get("MOSAIC_PG_USER", "mosaic")
PG_PASSWORD = os.environ.get("MOSAIC_PG_PASSWORD", "mosaic")
PG_DB = os.environ.get("MOSAIC_PG_DB", "mosaic")


def dsn() -> str:
    return (f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
            f"user={PG_USER} password={PG_PASSWORD}")


def ping() -> Dict:
    try:
        with psycopg2.connect(dsn(), connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"ok": True, "host": PG_HOST, "port": PG_PORT, "database": PG_DB}
    except Exception as e:
        return {"ok": False, "host": PG_HOST, "port": PG_PORT,
                "database": PG_DB, "error": str(e)}


def _connect():
    conn = psycopg2.connect(dsn(), connect_timeout=5)
    conn.autocommit = False
    return conn


def ensure_schema() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS floor_datasets (
        id SERIAL PRIMARY KEY,
        filename TEXT NOT NULL,
        saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        row_count INTEGER NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE
    );
    CREATE TABLE IF NOT EXISTS floor_readings (
        id SERIAL PRIMARY KEY,
        dataset_id INTEGER NOT NULL REFERENCES floor_datasets(id) ON DELETE CASCADE,
        seq INTEGER NOT NULL,
        param TEXT NOT NULL,
        tag TEXT NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        unit TEXT,
        ts TEXT,
        uns_topic TEXT,
        asset TEXT,
        name TEXT,
        quality TEXT,
        source TEXT
    );
    CREATE INDEX IF NOT EXISTS floor_readings_dataset_seq
        ON floor_readings (dataset_id, seq);
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def save_dataset(filename: str, rows: List[Dict]) -> Dict:
    if not rows:
        raise ValueError("Nothing to save — upload a file first")
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE floor_datasets SET is_active = FALSE WHERE is_active = TRUE")
            cur.execute(
                "INSERT INTO floor_datasets (filename, row_count, is_active) "
                "VALUES (%s, %s, TRUE) RETURNING id, saved_at",
                (filename or "upload", len(rows)),
            )
            dataset_id, saved_at = cur.fetchone()
            payload = [
                (
                    dataset_id,
                    i,
                    r.get("param"),
                    r.get("tag"),
                    float(r.get("value")),
                    r.get("unit"),
                    r.get("timestamp"),
                    r.get("uns_topic"),
                    r.get("asset"),
                    r.get("name"),
                    r.get("quality"),
                    r.get("source"),
                )
                for i, r in enumerate(rows)
            ]
            execute_values(
                cur,
                "INSERT INTO floor_readings "
                "(dataset_id, seq, param, tag, value, unit, ts, uns_topic, "
                "asset, name, quality, source) VALUES %s",
                payload,
            )
        conn.commit()
    return {
        "id": dataset_id,
        "filename": filename,
        "row_count": len(rows),
        "saved_at": saved_at.isoformat() if hasattr(saved_at, "isoformat") else str(saved_at),
        "postgres": ping(),
    }


def _row_to_reading(rec) -> Dict:
    (param, tag, value, unit, ts, uns_topic, asset, name, quality, source) = rec
    return {
        "param": param,
        "tag": tag,
        "value": float(value),
        "unit": unit,
        "timestamp": ts,
        "uns_topic": uns_topic,
        "asset": asset,
        "name": name,
        "quality": quality or "GOOD",
        "source": source or "uploaded/postgres",
    }


def load_active() -> Optional[Dict]:
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, filename, row_count, saved_at FROM floor_datasets "
                "WHERE is_active = TRUE ORDER BY id DESC LIMIT 1"
            )
            ds = cur.fetchone()
            if not ds:
                return None
            dataset_id, filename, row_count, saved_at = ds
            cur.execute(
                "SELECT param, tag, value, unit, ts, uns_topic, asset, name, quality, source "
                "FROM floor_readings WHERE dataset_id = %s ORDER BY seq",
                (dataset_id,),
            )
            rows = [_row_to_reading(r) for r in cur.fetchall()]
    return {
        "id": dataset_id,
        "filename": filename,
        "row_count": row_count or len(rows),
        "saved_at": saved_at.isoformat() if hasattr(saved_at, "isoformat") else str(saved_at),
        "rows": rows,
    }


def list_datasets(limit: int = 10) -> List[Dict]:
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, filename, row_count, saved_at, is_active "
                "FROM floor_datasets ORDER BY id DESC LIMIT %s",
                (limit,),
            )
            return [
                {
                    "id": r[0],
                    "filename": r[1],
                    "row_count": r[2],
                    "saved_at": r[3].isoformat() if hasattr(r[3], "isoformat") else str(r[3]),
                    "is_active": r[4],
                }
                for r in cur.fetchall()
            ]


def delete_all() -> Dict:
    """Permanently remove every shop-floor dataset and its readings."""
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM floor_datasets")
            n_ds = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(*) FROM floor_readings")
            n_rd = int(cur.fetchone()[0] or 0)
            cur.execute("TRUNCATE TABLE floor_readings, floor_datasets RESTART IDENTITY CASCADE")
        conn.commit()
    return {
        "deleted_datasets": n_ds,
        "deleted_readings": n_rd,
        "postgres": ping(),
    }
