# Geo Data Connector (GDC) — Lakeflow community connector

Ingest analysis-ready geospatial data from [Geo Data Connector](https://gdc.smartdatahub.io)
into Databricks. Each enabled GDC subscription appears as a table; a pipeline run
triggers a fresh GDC ingestion (retry-safe), downloads the delivered GeoParquet, and
loads it into your target Delta table.

Delivered data is engineered for Databricks Spatial SQL out of the box:

- `geom_wgs84` — geometry normalized to EPSG:4326 (WKB) for `ST_*` functions
- `h3_indices` / `h3_indices_coarse` — precomputed H3 cell arrays for `h3_*` functions

## Connection parameters

| Parameter | Required | Description |
|---|---|---|
| `pat` | yes (secret) | GDC personal access token — create a free account at gdc.smartdatahub.io, generate under Settings |
| `base_url` | no | API base; defaults to `https://gdca.api.smartdatahub.io/v1` |

## Plans

Works on every plan. Free tier: 10 active subscriptions, 10,000 rows per ingestion,
10 runs/day (a daily pipeline schedule fits comfortably).

## Status

**Prototype** — snapshot mode; core delivered columns with source attributes as JSON.
Full per-dataset schema derivation is planned.
