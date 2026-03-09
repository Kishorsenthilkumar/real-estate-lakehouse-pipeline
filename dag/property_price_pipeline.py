from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
import subprocess
import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ------------------------------
# Default DAG settings
# ------------------------------

default_args = {
    "owner": "kishor",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 8),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------
# Scraper Task
# ------------------------------

def run_scraper():
    """
    Runs the property scraper script.
    This script:
    - Scrapes 99acres using Apify
    - Stores raw JSON in ADLS with date partitioning
    """
    import sys
    import os
    # Get the workspace root directory
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scraper_path = os.path.join(workspace_root, "house_price_poc", "scraper.py")
    subprocess.run([sys.executable, scraper_path], check=True)


# ------------------------------
# Synapse SQL Task
# ------------------------------

def create_synapse_view():
    """
    Creates or alters a Synapse SQL view that reads from Delta Lake.
    The view queries the gold layer property_price_history table.
    """
    # Load environment variables
    synapse_server = os.getenv("SYNAPSE_SERVER")
    synapse_database = os.getenv("SYNAPSE_DATABASE")
    synapse_username = os.getenv("SYNAPSE_USERNAME")
    synapse_password = os.getenv("SYNAPSE_PASSWORD")
    
    if not all([synapse_server, synapse_database, synapse_username, synapse_password]):
        raise ValueError("Missing Synapse credentials in environment variables")
    
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={synapse_server};"
        f"Database={synapse_database};"
        f"UID={synapse_username};"
        f"PWD={synapse_password}"
    )

    cursor = conn.cursor()

    query = """
    CREATE OR ALTER VIEW property_price_history AS
    SELECT *
    FROM OPENROWSET(
        BULK 'https://storage.dfs.core.windows.net/gold/property_price_history/',
        FORMAT='DELTA'
    ) AS result
    """

    cursor.execute(query)
    conn.commit()
    conn.close()


# ------------------------------
# DAG Definition
# ------------------------------

with DAG(
    dag_id="real_estate_price_monitoring_pipeline",
    default_args=default_args,
    description="Daily pipeline for scraping 99acres property data, transforming with Spark/Delta Lake, and tracking price changes using SCD modeling",
    schedule="@daily",
    catchup=False,
    tags=["real-estate", "data-engineering"],
) as dag:

    # ------------------------------
    # Task 1: Run scraper
    # ------------------------------

    scrape_properties = PythonOperator(
        task_id="scrape_properties",
        python_callable=run_scraper
    )

    # ------------------------------
    # Task 2: Run Databricks notebook
    # ------------------------------

    transform_data = DatabricksSubmitRunOperator(
        task_id="run_databricks_transformation",
        databricks_conn_id="databricks_default",
        json={
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_D3_v2",
                "num_workers": 2
            },
            "notebook_task": {
                "notebook_path": "/Workspace/house_price_poc/notebooks/RealestateAnalytics"
            }
        }
    )

    # ------------------------------
    # Task 3: Create Synapse SQL view
    # ------------------------------

    synapse_view = PythonOperator(
        task_id="create_synapse_view",
        python_callable=create_synapse_view
    )

    # ------------------------------
    # Pipeline order
    # ------------------------------

    _ = scrape_properties >> transform_data >> synapse_view