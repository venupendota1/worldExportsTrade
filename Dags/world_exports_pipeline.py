from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from datetime import datetime, timedelta

# 1. Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Initialize the DAG
with DAG(
    'world_exports_analytics_pipeline',
    default_args=default_args,
    description='Pipeline to sense, clean and process global trade data',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['azure', 'databricks', 'exports'],
) as dag:

    # TASK 1: Sensor - Wait for the file to arrive in Azure Blob Storage
    # This checks the 'raw' container for any file starting with 'trade_data'
    wait_for_file = WasbPrefixSensor(
        task_id='wait_for_raw_csv',
        container_name='raw',
        prefix='trade_data',
        wasb_conn_id='azure_blob_default', # Connection defined in Airflow UI
        timeout=60 * 60, # Wait for 1 hour
        poke_interval=60 # Check every 60 seconds
    )

    # TASK 2: Run Databricks Notebook
    # This triggers the cleaning and processing code you wrote earlier
    notebook_run = DatabricksSubmitRunOperator(
        task_id='run_cleaning_and_processing',
        databricks_conn_id='databricks_default', # Connection defined in Airflow UI
        existing_cluster_id='0000-YOUR-CLUSTER-ID', # Found in Databricks cluster URL
        notebook_task={
            'notebook_path': '/Users/your_email/World_Exports_Analytics',
        }
    )

    # Define the sequence
    wait_for_file >> notebook_run
