# Setting up a Lakeflow pipeline with the GDC connector (own-repo / dogfood path)

Verified procedure (2026-07-23, first working run: 844 rows). Two steps below are
workarounds for current custom-connector wizard defects — marked **[workaround]**;
they should disappear as the framework matures.

## Prerequisites

- Databricks workspace with Unity Catalog; permissions to create connections and
  pipelines.
- A GDC personal access token (free account: gdc.smartdatahub.io → Settings).
- Target catalog + schema where the pipeline may create tables.

## Steps

**1. Start the wizard.** Data Ingestion → **Custom connector**. Configuration page:
   - Source name: `gdc`
   - Git repository URL: `https://github.com/smartdatahub/gdc-lakeflow-connector`

**2. Create the connection.** Page 2 → **+ Create connection** → Auth type
   **Static Credential**:
   - Connection name: e.g. `gdc_connection`
   - Additional Options (key/value):
     - `pat` → your GDC token
     - `base_url` → `https://gdca.api.smartdatahub.io/v1` (or the dev base for testing)
   Select the created connection → Next.

**3. Ingestion setup.** Pipeline name, event-log catalog/schema, and a **Root path**
   folder (e.g. `/Shared/gdc-pipeline`) → create the pipeline.

**4. [workaround] Complete the root-folder setup.** The editor may show
   *"Error while fetching root folder"* — click **Configure** and complete the
   new-ETL-editor setup dialog (keep the proposed folder name; set Location to your
   chosen root path; step 2 will have nothing to drag; finish). This creates the
   `src/` structure the wizard should have generated.

**5. Add the framework dependency.** Pipeline Settings → Environment → add pip
   dependency:
   `git+https://github.com/databrickslabs/lakeflow-community-connectors.git`
   (the library is not on PyPI yet.)

**6. [workaround] Verify the connector source is present.** The wizard's clone into
   the root folder may be partial. Check that
   `<root>/<pipeline-folder>/sources/gdc/` contains `__init__.py`, `gdc.py`, and
   `_generated_gdc_python_source.py`; if missing, import them from the repo (CLI:
   `databricks workspace import <path> --file <local> --format AUTO`, or drag-drop
   in the UI).

**7. Author the driver notebook.** The pipeline references `src/ingest.py` — create
   it as a **Python notebook** (plain `.py` workspace files are rejected) with:

   ```python
   import sys
   sys.path.append("/Workspace/<root>/<pipeline-folder>/sources/gdc")

   from databricks.labs.community_connector.pipeline import ingest
   from _generated_gdc_python_source import register_lakeflow_source

   spark.conf.set(
       "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

   pipeline_spec = {
       "connection_name": "gdc_connection",
       "objects": [
           {"table": {
               "source_table": "<table name>",
               "destination_catalog": "<catalog>",
               "destination_schema": "<schema>",
               "destination_table": "<target table>",
           }},
       ],
   }

   register_lakeflow_source(spark)
   ingest(spark, pipeline_spec)
   ```

   Table names are your enabled GDC subscriptions' dataset names, lowercased with
   non-alphanumerics replaced by `_` (e.g. `citypageweather-realtime` →
   `citypageweather_realtime`). Duplicate names get an 8-char subscription-id
   suffix.

**8. Run.** Each run triggers a GDC ingestion (retry-safe: an already-running or
   still-fresh run is treated as success), polls to completion, downloads the
   delivered GeoParquet, and merges into the destination table
   (`apply_changes_from_snapshot` on the synthesized `row_id`). Free-tier note:
   runs count against the 10-runs/day cap; a daily pipeline schedule fits.

## After changing connector code

Regenerate the merged module and update both copies:

```bash
# in a clone of databrickslabs/lakeflow-community-connectors with sources/gdc copied in
python tools/scripts/merge_python_source.py gdc
# commit the refreshed _generated_gdc_python_source.py to this repo
# re-import gdc.py + _generated_gdc_python_source.py into the pipeline's sources/gdc/
```
