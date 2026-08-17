# MOSAIC — From reference app to production

The reference app runs single-process for easy demo/dev. To scale out, replace
each layer's in-process component with its real service (all free/open-source),
or with the Azure managed equivalent.

| Layer | Reference (this repo) | Production (free/OSS) | Azure managed |
|-------|----------------------|------------------------|---------------|
| 1 Connectivity | `connectivity.py` simulator | OPC-UA server, Node-RED/Telegraf, Mosquitto MQTT | Azure IoT Edge, IoT Hub |
| 2 Ingest & Store | `ingest_store.py` in-proc | Redpanda/Kafka, TimescaleDB/QuestDB, MinIO, DuckDB | Event Hubs, Azure Data Explorer, ADLS Gen2 |
| 3 Contextualize | `contextualize.py` join chain | **Apache Flink**, JSON/Neo4j asset model | Azure Stream Analytics, Azure Digital Twins (DTDL) |
| 4 Visualize | `visualize.py` + dashboard view | Grafana, Superset, Metabase, Prometheus | Power BI, Managed Grafana, Azure Monitor |
| 5 SENTRA | `sentra/` TF-IDF + rules | Ollama, sentence-transformers (BGE-small), Qdrant, LangGraph | Azure AI Foundry, Azure OpenAI, Azure AI Search |
| 6 Govern | `govern.py` RBAC + hash chain | Keycloak, OPA, Postgres (WORM) | Microsoft Entra ID, Azure Policy, immutable storage |

## Suggested migration order

1. **Layer 3 first.** Move the join chain into a real Flink job reading from
   Kafka; keep the asset model in JSON, then graduate to Azure Digital Twins.
   The reference `contextualize()` is written to mirror the four Flink steps, so
   the port is mechanical.
2. **Layers 1–2.** Point an OPC-UA simulator + Node-RED at Mosquitto → Redpanda;
   land Bronze in TimescaleDB + MinIO.
3. **Layer 5.** Swap the TF-IDF retriever for Qdrant + BGE-small and Ollama.
   The `knowledge.search()` interface already matches a vector store.
4. **Layers 4 & 6.** Grafana on the Gold tables; Keycloak + OPA in front.

`docker-compose.yml` names all of these services with profiles (`l1`…`l6`).
