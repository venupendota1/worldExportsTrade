from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from datetime import datetime, timedelta
import json

# ── CHANGE THESE TWO VALUES ──────────────────────────────────────────────────
DATABRICKS_JOB_ID = 532044085711838
ADLS_ACCOUNT_NAME = "worldexportsdata"
# ────────────────────────────────────────────────────────────────────────────

# ── CONNECTION IDs ────────────────────────────────────────────────────────────
DATABRICKS_CONN_ID = "databricks_default"
ADLS_CONN_ID       = "azure_data_lake_default"
# ────────────────────────────────────────────────────────────────────────────

# ── ADLS CONFIG ───────────────────────────────────────────────────────────────
ADLS_CONTAINER = "world-exports"
RAW_FOLDER     = "rawData"
# ────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "world_exports_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_expected_filename(ds: str):
    """
    Builds the expected filename using yesterday's date.
    ds format: "YYYY-MM-DD"  e.g. "2026-03-03"
    returns filename : "world_exports_raw_02_03_2026.csv"
    returns full_path: "rawData/world_exports_raw_02_03_2026.csv"
    """
    today     = datetime.strptime(ds, "%Y-%m-%d")
    yesterday = today - timedelta(days=1)
    date_str  = yesterday.strftime("%d_%m_%Y")
    filename  = f"world_exports_raw_{date_str}.csv"
    full_path = f"{RAW_FOLDER}/{filename}"
    return filename, full_path


def log_pipeline_start(**context):
    """
    Task 1 — Validates Azure credentials, logs expected file, pushes
    the blob path into XCom so the sensor can read it safely.

    FIX: blob_name is now built here in pure Python using context["ds"]
         and pushed to XCom — no Jinja templating in the sensor at all.
         This avoids the 'execution_date is undefined' Jinja error entirely.
    """
    ds                  = context["ds"]
    filename, full_path = get_expected_filename(ds)

    print("=" * 60)
    print("   WORLD EXPORTS MEDALLION PIPELINE — STARTING")
    print("=" * 60)
    print(f"  DAG run date        : {ds}")
    print(f"  Looking for file    : {filename}")
    print(f"  Full ADLS path      : {ADLS_CONTAINER}/{full_path}")
    print(f"  Databricks Job ID   : {DATABRICKS_JOB_ID}")

    # ── Credential pre-flight check ──────────────────────────────────────────
    try:
        conn  = BaseHook.get_connection(ADLS_CONN_ID)
        extra = json.loads(conn.extra) if conn.extra else {}

        client_id     = conn.login
        client_secret = conn.password
        tenant_id     = extra.get("tenant_id", None)
        account_name  = extra.get("account_name", None)

        print("\n  Azure Credential Check:")
        print(f"    Client ID     : {'✅ found' if client_id     else '❌ MISSING'}")
        print(f"    Client Secret : {'✅ found' if client_secret  else '❌ MISSING'}")
        print(f"    Tenant ID     : {'✅ found' if tenant_id      else '❌ MISSING'}")
        print(f"    Account Name  : {'✅ found' if account_name   else '❌ MISSING'}")

        missing = []
        if not client_id:     missing.append("Login (Client ID)")
        if not client_secret: missing.append("Password (Client Secret)")
        if not tenant_id:     missing.append("tenant_id in Extra JSON")
        if not account_name:  missing.append("account_name in Extra JSON")

        if missing:
            raise ValueError(
                f"Connection '{ADLS_CONN_ID}' is missing: {', '.join(missing)}. "
                f"Fix in Airflow UI → Admin → Connections → {ADLS_CONN_ID}"
            )

    except Exception as e:
        raise RuntimeError(f"Pre-flight credential check failed: {e}")

    print(f"\n  ✅ All credentials verified.")
    print(f"  ✅ Blob path pushed to XCom: {full_path}")
    print("=" * 60)

    # Push blob path to XCom — sensor will pull this in the next task
    # XCom key: "blob_name"
    context["ti"].xcom_push(key="blob_name", value=full_path)

    return full_path


class DynamicWasbBlobSensor(BaseSensorOperator):
    """
    FIX: Custom sensor that pulls the blob_name from XCom at runtime.

    Why custom?
        WasbBlobSensor expects blob_name at DAG parse time.
        But our blob_name changes daily (based on yesterday's date).
        Jinja templates in WasbBlobSensor fail in some Airflow 2.x versions.

        Solution: subclass BaseSensorOperator, pull blob_name from XCom
        at poke time (runtime), then use WasbHook directly to check ADLS.

    Think of it like a protocol in Swift — we implement the poke() method
    and Airflow calls it on our schedule.
    """

    def __init__(self, wasb_conn_id: str, container_name: str, **kwargs):
        super().__init__(**kwargs)
        self.wasb_conn_id   = wasb_conn_id
        self.container_name = container_name

    def poke(self, context: Context) -> bool:
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        # Pull the blob path that log_pipeline_start pushed to XCom
        blob_name = context["ti"].xcom_pull(
            task_ids="log_pipeline_start",
            key="blob_name"
        )

        if not blob_name:
            raise ValueError(
                "blob_name not found in XCom. "
                "Make sure log_pipeline_start ran successfully before this task."
            )

        self.log.info(f"Checking ADLS for blob: {self.container_name}/{blob_name}")

        # Use WasbHook to check if the file exists in ADLS
        hook   = WasbHook(wasb_conn_id=self.wasb_conn_id)
        exists = hook.check_for_blob(self.container_name, blob_name)

        if exists:
            self.log.info(f"✅ File found: {self.container_name}/{blob_name}")
        else:
            self.log.info(f"⏳ File not yet available. Will retry in {self.poke_interval}s")

        return exists


def log_pipeline_success(**context):
    """Task 4 — Logs success summary after Databricks job completes."""
    ds                  = context["ds"]
    filename, full_path = get_expected_filename(ds)

    print("=" * 60)
    print("   PIPELINE COMPLETE — ALL LAYERS WRITTEN ✅")
    print("=" * 60)
    print(f"  File processed      : {filename}")
    print(f"  DAG run date        : {ds}")
    print(f"  Completed at        : {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")
    print()
    print("  Layers written to ADLS:")
    print(f"  ├── bronze/world_exports/          ← raw ingestion")
    print(f"  ├── silver/world_exports_cleaned/  ← cleaned data")
    print(f"  └── gold/")
    print(f"       ├── kpi_by_country/")
    print(f"       ├── kpi_by_category/")
    print(f"       ├── kpi_yearly_trend/")
    print(f"       ├── kpi_by_region/")
    print(f"       └── kpi_top_exporters/")
    print()
    print(f"  ADLS Account        : {ADLS_ACCOUNT_NAME}")
    print(f"  Container           : {ADLS_CONTAINER}")
    print("  Databricks Dashboard is ready to refresh ✅")
    print("=" * 60)


with DAG(
    dag_id="world_exports_medallion_pipeline",
    default_args=default_args,
    description="Daily 10AM: Watches ADLS for dated CSV → Triggers Databricks Medallion Job",
    schedule="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "databricks", "adls", "world-exports", "pyspark"],
) as dag:

    # ── TASK 1: Credential check + push blob path to XCom ────────────────────
    start_log = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
    )

    # ── TASK 2: Custom Sensor — pulls blob path from XCom, checks ADLS ───────
    # No Jinja. No execution_date. Pure Python at runtime.
    # Pokes every 60s, gives up after 2 hours, frees Docker worker between pokes.
    wait_for_file = DynamicWasbBlobSensor(
        task_id="wait_for_dated_file_in_adls",
        wasb_conn_id=ADLS_CONN_ID,
        container_name=ADLS_CONTAINER,
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
    )

    # ── TASK 3: Trigger Databricks Workflow Job ───────────────────────────────
    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="trigger_medallion_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # ── TASK 4: Log success ───────────────────────────────────────────────────
    success_log = PythonOperator(
        task_id="log_pipeline_success",
        python_callable=log_pipeline_success,
    )

    # ── PIPELINE ORDER ────────────────────────────────────────────────────────
    #
    #   [log_pipeline_start]           → builds blob path, pushes to XCom
    #          ↓
    #   [wait_for_dated_file_in_adls]  → pulls blob path from XCom, pokes ADLS
    #          ↓
    #   [trigger_medallion_job]         → triggers Databricks Bronze→Silver→Gold
    #          ↓
    #   [log_pipeline_success]          → logs completion summary
    #
    start_log >> wait_for_file >> trigger_databricks_job >> success_log
