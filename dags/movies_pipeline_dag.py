#Imports
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = "/usr/local/airflow"
SCRIPT_PATH = f"{AIRFLOW_HOME}/include/scripts"

# --- 1. Define the DAG ---
with DAG(
    dag_id="movie_roi_pipeline",
    start_date=datetime(2025, 1, 1),  # A fixed start date in the past
    schedule=None,  # This makes it "On Demand" / "Trigger Only"
    catchup=False,  # Prevents backfilling runs
    description="Runs the full ETL pipeline for movie ROI data.",
    tags=["movies", "etl", "roi", "business-project"],
) as dag:
    # --- 2. Define the Tasks ---

    task_extract = BashOperator(
        task_id="extract_raw_data",
        bash_command=f"python {SCRIPT_PATH}/extract.py",
    )

    task_transform = BashOperator(
        task_id="transform_clean_data",
        bash_command=f"python {SCRIPT_PATH}/transform.py",
    )

    # --- 3. Set the Task Dependencies ---
    task_extract >> task_transform
