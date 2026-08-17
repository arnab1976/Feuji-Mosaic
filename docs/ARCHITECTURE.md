# MOSAIC — Architecture

MOSAIC is a six-layer Manufacturing Intelligence platform. Data flows bottom to
top; **Security & Governance** runs across all layers.

```
  6  Govern & Harden      identity · policy · hash-chained audit   (cross-cutting)
  5  SENTRA Intelligence  agents · RAG · governed response          ← CORE
  4  Visualize            dashboards on meaning · observability
  3  Contextualize        tag→asset→batch→product→spec join chain   ← CORE
  2  Ingest & Store       event log · historian · Medallion lake
  1  Connectivity         OPC-UA · edge · Unified Namespace
  0  Edge / OT            sensors · PLCs · SCADA (on-prem)
```

## The core: data contextualization (Layer 3)

A raw SCADA reading is just `{ tag, value, timestamp }`. Contextualization is a
chain of **keyed lookups** that a Flink job runs in state, per event:

```
A. Consume   read the raw event (tag + timestamp are the keys)
B. Enrich    tag          --(Asset Model)--> asset, spec
             asset + time --(MES, temporal)--> batch, product, phase
             product      --(SAP)-->          material, spec source
             tag          --(RDBMS/Files)-->  calibration, shift
C. Compute   value vs spec -> status (OVER / UNDER / OK)
D. Emit      the contextualized event
```

Each source hands the key for the next lookup (`tag → asset → product`). The MES
join is **temporal** — it matches the batch whose time window contains the
reading's timestamp, which is why the reading must carry a timestamp. See
`backend/app/layers/contextualize.py`; the API returns the full step-by-step
trace at `POST /api/contextualize`.

## The five parameters

| id | name | tag | asset | control band | unit |
|----|------|-----|-------|--------------|------|
| temp | Reactor Temperature | TT-1202B | BR-12 | 36.5–37.5 | °C |
| ph | pH | AT-3401 | BR-12 | 6.8–7.2 | pH |
| press | Differential Pressure | PT-2201 | FIL-07 | 100–110 | kPa |
| cond | Conductivity (WFI) | CT-5501 | WFI-02 | 700–900 | µS/cm |
| hum | Humidity (Cleanroom) | MT-6601 | CR-A1 | 40–55 | % |

Each has control / alarm / trip bands, a scoring model (context) and the driver
variables that explain a breach. See `backend/app/domain.py`.

## SENTRA (Layer 5)

SENTRA consumes contextualized events (not raw readings) and runs
**perceive → diagnose → retrieve (RAG) → reason → govern**. pH is GxP-critical
and always requires a QA e-signature; trip-zone excursions always escalate.
See `backend/app/sentra/`.

## Governance (Layer 6)

Keycloak-style RBAC, an OPA-style policy gate, and a genuine SHA-256
**hash-chained audit** (`GET /api/audit` includes a `verify` that re-hashes the
chain to prove it hasn't been tampered with). See `backend/app/layers/govern.py`.
