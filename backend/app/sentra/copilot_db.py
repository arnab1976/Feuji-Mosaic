"""PostgreSQL persistence for SCADA Co-pilot chat turns."""
from __future__ import annotations
from typing import Dict, List, Optional

from psycopg2.extras import Json

from ..layers import floor_db


def ping() -> Dict:
    return floor_db.ping()


def _connect():
    return floor_db._connect()


def ensure_schema() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS copilot_turns (
        id SERIAL PRIMARY KEY,
        agent TEXT NOT NULL,
        param TEXT,
        zone TEXT,
        query TEXT NOT NULL,
        answer TEXT,
        source TEXT,
        llm_fallback BOOLEAN NOT NULL DEFAULT FALSE,
        hits JSONB,
        chart JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS copilot_turns_agent_created
        ON copilot_turns (agent, created_at DESC);
    CREATE TABLE IF NOT EXISTS copilot_si (
        agent TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def save_turn(row: Dict) -> Optional[int]:
    try:
        ensure_schema()
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO copilot_turns
                        (agent, param, zone, query, answer, source, llm_fallback, hits, chart)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        row.get("agent"),
                        row.get("param"),
                        row.get("zone"),
                        row.get("query"),
                        row.get("answer"),
                        row.get("source"),
                        bool(row.get("llm_fallback")),
                        Json(row.get("hits") or []),
                        Json(row.get("chart")),
                    ),
                )
                tid = cur.fetchone()[0]
            conn.commit()
        return tid
    except Exception:
        return None


def recent(agent: Optional[str] = None, n: int = 40) -> List[Dict]:
    try:
        ensure_schema()
        with _connect() as conn:
            with conn.cursor() as cur:
                if agent:
                    cur.execute(
                        "SELECT id, agent, param, zone, query, answer, source, "
                        "llm_fallback, hits, chart, created_at "
                        "FROM copilot_turns WHERE agent = %s "
                        "ORDER BY id DESC LIMIT %s",
                        (agent, n),
                    )
                else:
                    cur.execute(
                        "SELECT id, agent, param, zone, query, answer, source, "
                        "llm_fallback, hits, chart, created_at "
                        "FROM copilot_turns ORDER BY id DESC LIMIT %s",
                        (n,),
                    )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        out = []
        for r in reversed(rows):
            ts = r.get("created_at")
            r["created_at"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts or "")
            out.append(r)
        return out
    except Exception:
        return []


def save_si(agent: str, payload: Dict) -> None:
    """Persist the latest SCADA Intelligence remedy so Co-pilot can refer to it after restart."""
    if not agent or not payload:
        return
    try:
        ensure_schema()
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO copilot_si (agent, payload, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (agent) DO UPDATE
                      SET payload = EXCLUDED.payload, updated_at = NOW()
                    """,
                    (agent, Json(payload)),
                )
            conn.commit()
    except Exception:
        return


def load_si(agent: str) -> Optional[Dict]:
    if not agent:
        return None
    try:
        ensure_schema()
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM copilot_si WHERE agent = %s",
                    (agent,),
                )
                row = cur.fetchone()
        if not row or not row[0]:
            return None
        payload = row[0]
        return dict(payload) if isinstance(payload, dict) else None
    except Exception:
        return None
