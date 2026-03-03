from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context
from datetime import datetime, timedelta
import json
import time

# ── CHANGE THESE THREE VALUES ─────────────────────────────────────────────────
DATABRICKS_JOB_ID  = 532044085711838
DATABRICKS_CLUSTER_ID = "0303-120105-trzn1py3"
ADLS_ACCOUNT_NAME  = "worldexportsdata"
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
    Builds filename using yesterday's date.
    ds = "2026-03-03" → "rawData/world_exports_raw_02_03_2026.csv"
    """
    today     = datetime.strptime(ds, "%Y-%m-%d")
    yesterday = today - timedelta(days=1)
    date_str  = yesterday.strftime("%d_%m_%Y")
    filename  = f"world_exports_raw_{date_str}.csv"
    full_path = f"{RAW_FOLDER}/{filename}"
    return filename, full_path


def log_pipeline_start(**context):
    """
    Task 1 — Validates Azure credentials and logs expected filename.
    Pushes blob path to XCom for the sensor to use.
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
    print(f"  Cluster ID          : {DATABRICKS_CLUSTER_ID}")

    # Credential pre-flight check
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
                f"Connection '{ADLS_CONN_ID}' is missing: {', '.join(missing)}."
            )

    except Exception as e:
        raise RuntimeError(f"Pre-flight credential check failed: {e}")

    print("\n  ✅ All credentials verified.")
    print("=" * 60)

    # Push blob path to XCom for the sensor
    context["ti"].xcom_push(key="blob_name", value=full_path)
    return full_path


def ensure_cluster_running(**context):
    
    hook   = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
    client = hook  # DatabricksHook has direct API methods

    print("=" * 60)
    print("   CLUSTER STATUS CHECK")
    print("=" * 60)
    print(f"  Cluster ID : {DATABRICKS_CLUSTER_ID}")

    # ── Step 1: Get current cluster state ────────────────────────────────────
    # Calls Databricks REST API: GET /2.0/clusters/get
    cluster_info = hook._do_api_call(
        ("GET", "2.0/clusters/get"),
        {"cluster_id": DATABRICKS_CLUSTER_ID}
    )

    state       = cluster_info.get("state", "UNKNOWN")
    cluster_name = cluster_info.get("cluster_name", "unknown")

    print(f"  Cluster name  : {cluster_name}")
    print(f"  Current state : {state}")

    # ── Step 2: Handle each state ─────────────────────────────────────────────

    if state == "RUNNING":
        # Already up — nothing to do
        print("  ✅ Cluster is already RUNNING. Proceeding to job trigger.")
        return

    elif state == "TERMINATED":
        # Most common case — Community Edition auto-terminated
        # Start it and wait
        print("  🟡 Cluster is TERMINATED. Starting it now...")

        hook._do_api_call(
            ("POST", "2.0/clusters/start"),
            {"cluster_id": DATABRICKS_CLUSTER_ID}
        )
        print("  ⏳ Start command sent. Waiting for cluster to be RUNNING...")
        _wait_for_cluster_running(hook)

    elif state in ("PENDING", "RESTARTING"):
        # Already in the process of starting — just wait
        print(f"  🟡 Cluster is {state}. Waiting for it to be RUNNING...")
        _wait_for_cluster_running(hook)

    elif state == "TERMINATING":
        # Mid-shutdown — wait for it to fully stop, then start
        print("  🟡 Cluster is TERMINATING. Waiting for full stop first...")
        _wait_for_state(hook, target_state="TERMINATED")
        print("  🟡 Now starting the cluster...")
        hook._do_api_call(
            ("POST", "2.0/clusters/start"),
            {"cluster_id": DATABRICKS_CLUSTER_ID}
        )
        _wait_for_cluster_running(hook)

    elif state == "ERROR":
        # Something is wrong with the cluster itself
        state_message = cluster_info.get("state_message", "No details available")
        raise RuntimeError(
            f"Cluster {DATABRICKS_CLUSTER_ID} is in ERROR state: {state_message}. "
            f"Please fix the cluster in Databricks UI before retrying."
        )

    else:
        raise RuntimeError(
            f"Cluster is in unexpected state: {state}. "
            f"Please check Databricks UI."
        )


def _wait_for_cluster_running(hook, timeout_minutes=15, poll_interval_seconds=30):
    
    timeout_seconds = timeout_minutes * 60
    elapsed         = 0

    while elapsed < timeout_seconds:
        cluster_info = hook._do_api_call(
            ("GET", "2.0/clusters/get"),
            {"cluster_id": DATABRICKS_CLUSTER_ID}
        )
        state = cluster_info.get("state", "UNKNOWN")
        print(f"  ⏳ Cluster state: {state} (waited {elapsed}s so far...)")

        if state == "RUNNING":
            print(f"  ✅ Cluster is RUNNING after {elapsed}s!")
            return

        if state == "ERROR":
            msg = cluster_info.get("state_message", "No details")
            raise RuntimeError(f"Cluster entered ERROR state while starting: {msg}")

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise RuntimeError(
        f"Cluster did not start within {timeout_minutes} minutes. "
        f"Check Databricks UI for details."
    )


def _wait_for_state(hook, target_state, timeout_minutes=10, poll_interval_seconds=20):
    """Waits until cluster reaches a specific target state."""
    timeout_seconds = timeout_minutes * 60
    elapsed         = 0

    while elapsed < timeout_seconds:
        cluster_info = hook._do_api_call(
            ("GET", "2.0/clusters/get"),
            {"cluster_id": DATABRICKS_CLUSTER_ID}
        )
        state = cluster_info.get("state", "UNKNOWN")
        print(f"  ⏳ Waiting for {target_state}. Current: {state} ({elapsed}s)")

        if state == target_state:
            return

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise RuntimeError(f"Cluster did not reach {target_state} within {timeout_minutes} minutes.")


class DynamicWasbBlobSensor(BaseSensorOperator):

    def __init__(self, wasb_conn_id: str, container_name: str, **kwargs):
        super().__init__(**kwargs)
        self.wasb_conn_id   = wasb_conn_id
        self.container_name = container_name

    def poke(self, context: Context) -> bool:
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        blob_name = context["ti"].xcom_pull(
            task_ids="log_pipeline_start",
            key="blob_name"
        )

        if not blob_name:
            raise ValueError("blob_name not found in XCom.")

        self.log.info(f"Checking ADLS: {self.container_name}/{blob_name}")

        hook   = WasbHook(wasb_conn_id=self.wasb_conn_id)
        exists = hook.check_for_blob(self.container_name, blob_name)

        if exists:
            self.log.info(f"✅ File found: {self.container_name}/{blob_name}")
        else:
            self.log.info(f"⏳ File not yet available. Retrying in {self.poke_interval}s")

        return exists


def log_pipeline_success(**context):
    """Task 5 — Logs success summary."""
    ds                  = context["ds"]
    filename, full_path = get_expected_filename(ds)

    print("=" * 60)
    print("   PIPELINE COMPLETE — ALL LAYERS WRITTEN ✅")
    print("=" * 60)
    print(f"  File processed : {filename}")
    print(f"  Completed at   : {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}")
    print()
    print("  Layers written to ADLS:")
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
    description="Daily 10AM: ADLS Sensor → Cluster Check → Databricks Medallion Job",
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

    # ── TASK 2: Wait for yesterday's dated file in ADLS ──────────────────────
    wait_for_file = DynamicWasbBlobSensor(
        task_id="wait_for_dated_file_in_adls",
        wasb_conn_id=ADLS_CONN_ID,
        container_name=ADLS_CONTAINER,
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
    )

    # ── TASK 3: Ensure cluster is RUNNING before triggering job ──────────────
    # Checks state → starts if terminated → waits up to 15 mins
    ensure_cluster = PythonOperator(
        task_id="ensure_cluster_running",
        python_callable=ensure_cluster_running,
    )

    # ── TASK 4: Trigger Databricks Workflow Job ───────────────────────────────
    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="trigger_medallion_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # ── TASK 5: Log success ───────────────────────────────────────────────────
    success_log = PythonOperator(
        task_id="log_pipeline_success",
        python_callable=log_pipeline_success,
    )

    # ── PIPELINE ORDER ────────────────────────────────────────────────────────
    #
    #   [log_pipeline_start]            → validates creds, pushes blob path
    #          ↓
    #   [wait_for_dated_file_in_adls]   → pokes ADLS every 60s for dated CSV
    #          ↓
    #   [ensure_cluster_running]         → checks cluster, starts if terminated
    #          ↓
    #   [trigger_medallion_job]           → triggers Databricks Bronze→Silver→Gold
    #          ↓
    #   [log_pipeline_success]            → logs completion summary
    #
    start_log >> wait_for_file >> ensure_cluster >> trigger_databricks_job >> success_log
