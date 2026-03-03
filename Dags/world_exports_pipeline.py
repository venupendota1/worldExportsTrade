from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json

# ── CHANGE THESE TWO VALUES ──────────────────────────────────────────────────
DATABRICKS_JOB_ID = 532044085711838
ADLS_ACCOUNT_NAME = "worldexportsdata"
# ────────────────────────────────────────────────────────────────────────────

# ── CONNECTION IDs (must match what you set in Airflow UI) ───────────────────
DATABRICKS_CONN_ID = "databricks_default"
ADLS_CONN_ID       = "azure_data_lake_default"
# ────────────────────────────────────────────────────────────────────────────

# ── ADLS PATHS ───────────────────────────────────────────────────────────────
ADLS_CONTAINER  = "world-exports"
RAW_FILE_PATH   = "rawData/world_exports_raw.csv"
# ────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "world_exports_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def log_pipeline_start(**context):
    """
    Logs pipeline start details.
    Also validates that the Azure connection is properly configured
    before the sensor even starts — fail fast if credentials are missing.
    """
    print("=" * 60)
    print("   WORLD EXPORTS MEDALLION PIPELINE — STARTING")
    print("=" * 60)

    # Pull the Azure connection and verify it has credentials
    # This is a dry-run check — no actual Azure call yet
    try:
        conn = BaseHook.get_connection(ADLS_CONN_ID)

        # Read Extra JSON (contains tenant_id and account_name)
        extra = json.loads(conn.extra) if conn.extra else {}

        client_id    = conn.login
        tenant_id    = extra.get("tenant_id", "NOT SET")
        account_name = extra.get("account_name", "NOT SET")

        print(f"  Azure Connection ID : {ADLS_CONN_ID}")
        print(f"  Client ID found     : {'✅ YES' if client_id else '❌ MISSING'}")
        print(f"  Client Secret found : {'✅ YES' if conn.password else '❌ MISSING'}")
        print(f"  Tenant ID found     : {'✅ YES' if tenant_id != 'NOT SET' else '❌ MISSING'}")
        print(f"  Storage Account     : {account_name}")

        if not client_id or not conn.password or tenant_id == "NOT SET":
            raise ValueError(
                "Azure connection is incomplete! "
                "Go to Airflow UI → Admin → Connections → azure_data_lake_default "
                "and fill in Login (Client ID), Password (Client Secret), "
                "and Extra JSON with tenant_id."
            )

    except Exception as e:
        raise RuntimeError(f"Connection check failed: {e}")

    print(f"\n  Watching ADLS path  : {ADLS_CONTAINER}/{RAW_FILE_PATH}")
    print(f"  Databricks Job ID   : {DATABRICKS_JOB_ID}")
    print(f"  Execution date      : {context['ds']}")
    print("=" * 60)


def log_pipeline_success(**context):
    """Logs success summary after Databricks job completes."""
    print("=" * 60)
    print("   PIPELINE COMPLETE — ALL LAYERS WRITTEN ✅")
    print("=" * 60)
    print("  Bronze → raw data ingested as Delta table")
    print("  Silver → cleaned and validated Delta table")
    print("  Gold   → KPI aggregations ready for Dashboard")
    print(f"\n  ADLS Location : {ADLS_ACCOUNT_NAME} / {ADLS_CONTAINER}/")
    print("  ├── bronze/world_exports/")
    print("  ├── silver/world_exports_cleaned/")
    print("  └── gold/")
    print("       ├── kpi_by_country/")
    print("       ├── kpi_by_category/")
    print("       ├── kpi_yearly_trend/")
    print("       ├── kpi_by_region/")
    print("       └── kpi_top_exporters/")
    print("=" * 60)


with DAG(
    dag_id="world_exports_medallion_pipeline",
    default_args=default_args,
    description="ADLS FileSensor → Databricks Medallion Job (Bronze → Silver → Gold)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["medallion", "databricks", "adls", "world-exports", "pyspark"],
) as dag:

    # ── TASK 1: Validate credentials + log start ─────────────────────────────
    start_log = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
    )

    # ── TASK 2: WasbBlobSensor ──────────────────────────────────────────────
    # Authenticates using Service Principal from 'azure_data_lake_default'
    # (Client ID + Client Secret + Tenant ID — all from Airflow connection)
    # Pokes ADLS every 30 seconds, times out after 1 hour
    wait_for_file = WasbBlobSensor(
    task_id="wait_for_file",
    container_name=ADLS_CONTAINER,
    blob_name=RAW_FILE_PATH,
    wasb_conn_id=ADLS_CONN_ID,
    timeout=300,
    poke_interval=30,
    )

    # ── TASK 3: Trigger Databricks Workflow Job ───────────────────────────────
    # DatabricksRunNowOperator:
    #   - Calls Databricks REST API: POST /api/2.1/jobs/run-now
    #   - Authenticates using token from 'databricks_default' connection
    #   - Triggers your existing Workflow Job (Bronze → Silver → Gold tasks)
    #   - wait_for_termination=True means Airflow WAITS until the job finishes
    #   - polling_period_seconds=30 means it checks job status every 30 seconds
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
    #   [log_pipeline_start]
    #          ↓
    #   [wait_for_raw_file_in_adls]   ← ADLS Sensor (Azure Service Principal auth)
    #          ↓
    #   [trigger_medallion_job]        ← Databricks REST API (token auth)
    #          ↓
    #   [log_pipeline_success]
    #
    start_log >> wait_for_file >> trigger_databricks_job >> success_log
