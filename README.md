# MOSAIC

**M**anufacturing **O**perations **S**tream **A**ggregation, **I**ntegration & **C**ontextualization

A runnable reference implementation of a Manufacturing Intelligence platform.
It turns raw SCADA telemetry into **contextualized, business-meaningful events**
and then into **governed action** via the **SENTRA** agent layer — built entirely
on free / open-source tools, with the Azure equivalent documented at every layer.

Covers all five parameters: **Temperature, pH, Humidity, Pressure, Conductivity.**

---

## What's in the box

A FastAPI backend implementing all six platform layers, and a professional
single-page frontend that drives them live:

| Layer | Role | This repo | Production (free) | Azure |
|------|------|-----------|-------------------|-------|
| 1 Connectivity | data off the floor | sensor simulator | OPC-UA · Node-RED · MQTT | IoT Edge / IoT Hub |
| 2 Ingest & Store | capture & historize | in-proc historian + lake | Kafka · TimescaleDB · MinIO | Event Hubs · ADX · ADLS |
| 3 **Contextualize** | **the core** | keyed-lookup join chain | **Apache Flink** · asset model | Stream Analytics · Digital Twins |
| 4 Visualize | dashboards on meaning | dashboard API | Grafana · Superset · Prometheus | Power BI · Managed Grafana |
| 5 **SENTRA** | **autonomous response** | RAG + agent | Ollama · Qdrant · LangGraph | AI Foundry · OpenAI · AI Search |
| 6 Govern | zero-trust | RBAC · policy · hash-chained audit | Keycloak · OPA · Postgres | Entra ID · Azure Policy |

> **Why a single-process reference?** A full Kafka+Flink+Ollama+Keycloak cluster
> needs ~10 containers and a workstation to run. This repo implements the *same
> logic* each layer performs — the real contextualization join chain, real
> threshold logic, a real RAG retriever, a real hash-chained audit — so it runs
> on a laptop in seconds and is easy to demo, read and extend. `docker-compose.yml`
> names the real production services for when you scale out.

---

## Quick start

Requires **Python 3.10+**.

```bash
cd backend
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Then open **http://localhost:8000** for the portal, or
**http://localhost:8000/docs** for the interactive API.

### In Cursor / Antigravity
Open the `mosaic` folder, let it detect the Python environment, then run the
`uvicorn` command above in the integrated terminal (or use `scripts/run_local.sh`).

---

## Try it

- **Home** — the architecture and full free/open-source stack.
- **Data Contextualization** — step through the keyed-lookup chain, driven by the
  live API trace.
- **Contextualize (layer 3)** — pick any of the 5 parameters and a scenario, then
  watch the 4 Flink steps enrich one reading field by field.
- **SENTRA Intelligence** — run the agent on a live excursion; see RAG citations
  and the governance decision.
- **Live Dashboard** — generate a batch and see all 5 parameters contextualized.
- **Journey** — one reading through all six layers, end to end.

---

## API surface (all under `/api`)

```
GET  /api/health                 platform + parameters
GET  /api/parameters             the 5 parameters and their bands
GET  /api/emit?param=&zone=      Layer 1 — simulate a raw reading
POST /api/ingest                 Layer 2 — capture to historian
GET  /api/historian              Layer 2 — recent raw readings
POST /api/contextualize          Layer 3 — enrich (returns join-chain trace)
GET  /api/gold                   Layer 3 — contextualized events
GET  /api/dashboard              Layer 4 — KPIs across 5 parameters
GET  /api/platform               Layer 4 — platform observability
GET  /api/sentra/search?q=       Layer 5 — RAG over SOP/CAPA/OEM
POST /api/sentra/run             Layer 5 — full agent cycle
POST /api/govern/commit          Layer 6 — pipeline + governance + audit
GET  /api/audit                  Layer 6 — hash-chained audit + verify
```

See `docs/ARCHITECTURE.md` for the layer-by-layer design and
`docs/PRODUCTION.md` for mapping each layer to its real deployment.

---

## Project layout

```
mosaic/
├── backend/
│   ├── app/
│   │   ├── domain.py            5 parameters, bands, asset model
│   │   ├── reference_data.py    mock SAP / MES / RDBMS / asset model + lookups
│   │   ├── layers/
│   │   │   ├── connectivity.py  Layer 1 — sensor simulator
│   │   │   ├── ingest_store.py  Layer 2 — historian + Gold store
│   │   │   ├── contextualize.py Layer 3 — the keyed-lookup chain (CORE)
│   │   │   ├── visualize.py     Layer 4 — dashboard aggregation
│   │   │   └── govern.py        Layer 6 — RBAC, policy, hash-chained audit
│   │   ├── sentra/
│   │   │   ├── knowledge.py     RAG knowledge base + TF-IDF retriever
│   │   │   └── agent.py         Layer 5 — the SENTRA agent
│   │   └── main.py             FastAPI app (serves the frontend too)
│   └── requirements.txt
├── frontend/
│   ├── index.html
│   └── assets/  style.css · app.js
├── docs/  ARCHITECTURE.md · PRODUCTION.md
├── scripts/  run_local.sh · seed.py
└── docker-compose.yml          names the real production services
```

MOSAIC is the platform; **SENTRA is the intelligent application that rides on top
and consumes its contextualized events.**
