"""Geo Data Connector (GDC) — Lakeflow community connector (prototype).

Snapshot-mode connector over the GDC public API (https://www.smartdatahub.io/docs/gdc-web-api/).
Each enabled GDC subscription is exposed as a table. A read triggers an on-demand
ingestion run (retry-safe: an already-running or still-fresh run is a success),
polls to completion, downloads the delivered GeoParquet, and yields its rows.

Delivered data is analysis-ready for Databricks Spatial SQL: use the geom_wgs84
column (EPSG:4326, WKB) with ST_* functions and the precomputed h3_indices arrays
with h3_* functions.
"""

import io
import re
import time
from typing import Dict, Iterator, List, Tuple

import requests
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

DEFAULT_BASE_URL = "https://gdca.api.smartdatahub.io/v1"
POLL_INTERVAL_S = 15
MAX_POLLS = 120  # 30 min ceiling
TERMINAL_STATUSES = {"SUCCESS", "FAILED", "INACTIVE"}


class GdcLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Geo Data Connector."""

    def __init__(self, options: dict) -> None:
        super().__init__(options)
        self._pat = options["pat"]
        self._base = (options.get("base_url") or DEFAULT_BASE_URL).rstrip("/")
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self._pat}"
        self._tables: Dict[str, str] = {}  # table name -> subscription_id

    # -- helpers -----------------------------------------------------------

    def _get(self, path: str, **kw) -> dict:
        r = self._session.get(f"{self._base}{path}", timeout=60, **kw)
        r.raise_for_status()
        return r.json()

    def _refresh_tables(self) -> None:
        tasks = self._get("/tasks").get("tasks", [])
        self._tables = {}
        for t in tasks:
            if not t.get("enabled", True):
                continue
            name = re.sub(r"[^0-9A-Za-z_]", "_", t.get("source_dataset_name", "dataset")).strip("_").lower()
            sub = t["subscription_id"]
            key = name if name not in self._tables else f"{name}_{sub[:8]}"
            self._tables[key] = sub

    def _subscription_for(self, table_name: str) -> str:
        if table_name not in self._tables:
            self._refresh_tables()
        return self._tables[table_name]

    # -- LakeflowConnect interface -----------------------------------------

    def list_tables(self) -> List[str]:
        self._refresh_tables()
        return sorted(self._tables)

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        # Prototype: the standardized delivered core columns. Full per-dataset
        # schema derivation (source attributes) is the next iteration.
        return StructType(
            [
                StructField("geom_wgs84", BinaryType()),
                StructField("h3_indices", ArrayType(StringType())),
                StructField("h3_resolution", IntegerType()),
                StructField("h3_indices_coarse", ArrayType(StringType())),
                StructField("attributes_json", StringType()),
            ]
        )

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> dict:
        return {"primary_keys": [], "cursor_field": None, "ingestion_type": "snapshot"}

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        sub = self._subscription_for(table_name)

        # Trigger a run. 409 (already running / not ready) is retry-safe by design.
        r = self._session.post(f"{self._base}/tasks/{sub}/execute", timeout=60)
        if r.status_code not in (200, 201, 202, 409):
            r.raise_for_status()

        status = None
        for _ in range(MAX_POLLS):
            task = self._get(f"/tasks/{sub}")
            status = task.get("ingestion_status")
            if status in TERMINAL_STATUSES:
                break
            time.sleep(POLL_INTERVAL_S)
        if status != "SUCCESS":
            raise RuntimeError(f"GDC ingestion for {table_name} ended {status}")

        files = self._get(f"/data/{sub}/files").get("files", [])
        if not files:
            raise RuntimeError(f"no delivered files for {table_name}")

        def rows() -> Iterator[dict]:
            import pyarrow.parquet as pq  # provided by the Databricks runtime

            for f in files:
                data = self._session.get(f["url"], timeout=300)
                data.raise_for_status()
                table = pq.read_table(io.BytesIO(data.content))
                core = {"geom_wgs84", "h3_indices", "h3_resolution", "h3_indices_coarse"}
                for batch in table.to_pylist():
                    yield {
                        "geom_wgs84": batch.get("geom_wgs84"),
                        "h3_indices": batch.get("h3_indices"),
                        "h3_resolution": batch.get("h3_resolution"),
                        "h3_indices_coarse": batch.get("h3_indices_coarse"),
                        "attributes_json": str({k: v for k, v in batch.items() if k not in core}),
                    }

        return rows(), None  # None offset: whole snapshot in one batch
