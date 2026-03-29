from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import mlflow
import pandas as pd
import psycopg2
import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
from datetime import datetime, timedelta


# French weekday mapping
FRENCH_WEEKDAYS = {
    0: "Lundi",
    1: "Mardi",
    2: "Mercredi",
    3: "Jeudi",
    4: "Vendredi",
    5: "Samedi",
    6: "Dimanche"
}

#Postgres
pg_citibke_hook = PostgresHook(postgres_conn_id="postgres_citibike")


# ---- Fetch from MLflow ----
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_INTERNAL_URI")
MODEL_NAME = "citibike_forecast_model" 

def fetch_from_mlflow(**context):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    # Get the experiment
    experiment = mlflow.get_experiment_by_name("citibike_forecast")
    if experiment is None:
        print("Experiment not found")
        return None

    # --- Step 1: Get latest model version ---
    latest_versions = client.get_latest_versions(name=MODEL_NAME, stages=["None", "Staging", "Production"])
    if not latest_versions:
        print(f"No versions found for model {MODEL_NAME}")
        return None

    latest_version_info = max(latest_versions, key=lambda v: int(v.version))
    latest_run_id = latest_version_info.run_id
    print(f"Latest model version: {latest_version_info.version}, run_id: {latest_run_id}")

    # --- Step 2: Fetch runs from last 24h ---
    yesterday = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"attributes.start_time >= {yesterday} AND run_id = '{latest_run_id}'"
    )

    if runs.empty:
        print("No runs found for the latest model in the last 24 hours")
        return None

    # --- Step 3: Extract relevant fields ---
    df = pd.DataFrame({
        "run_id": runs["run_id"],
        "station_id": runs["params.station_selected"],
        "request_datetime": pd.to_datetime(runs["params.request_datetime"]),
        "hour": runs["params.hour_selected"].astype(int),
        "prediction": runs["metrics.predicted_bike_flow"].astype(float),
        "timestamp": pd.to_datetime(runs["start_time"], unit='ms')
    })

    path = "/tmp/mlflow_predictions.parquet"
    df.to_parquet(path, index=False)

    print(f"Fetched {len(df)} runs from MLflow for the latest model")
    return path


# ---- Insert into Postgres ----
def insert_into_postgres(**context):
    path = context["ti"].xcom_pull(task_ids="fetch_mlflow")

    if not path:
        return

    # Read parquet file
    df = pd.read_parquet(path)
    if df.empty:
        print("DataFrame is empty. Nothing to insert.")
        return

    # Add month and jour_semaine columns
    df['month'] = df['request_datetime'].dt.month
    df['jour_semaine'] = df['request_datetime'].dt.weekday.map(FRENCH_WEEKDAYS)

    #Connect to Postgres
    pg_conn = pg_citibke_hook.get_conn()
    pg_cur = pg_conn.cursor()

    # Create table if not exists
    pg_cur.execute("""
        CREATE TABLE IF NOT EXISTS mlflow_predictions (
            run_id TEXT PRIMARY KEY,
            station_id TEXT,
            request_datetime TIMESTAMP,
            hour INT,
            prediction FLOAT,
            timestamp TIMESTAMP,
            month INT,
            jour_semaine TEXT
        )
    """)

    # Prepare data for bulk insert
    records = [
        (
            row["run_id"],
            row["station_id"],
            row["request_datetime"],
            row["hour"],
            row["prediction"],
            row["timestamp"],
            row["month"],
            row["jour_semaine"]
        )
        for _, row in df.iterrows()
    ]
    
    # Bulk insert with conflict handling
    execute_values(
        pg_cur,
        """
        INSERT INTO mlflow_predictions (
            run_id, station_id, request_datetime, hour, prediction, timestamp, month, jour_semaine
        ) VALUES %s
        ON CONFLICT (run_id) DO NOTHING
        """,
        records
    )
    print(f"Inserting {len(df)} runs into Postgres")
    
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()


# ---- DAG ----
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="prediction_mlflow_to_postgres",
    default_args=default_args,
    schedule="0 14 * * *", 
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_mlflow",
        python_callable=fetch_from_mlflow,
        doc_md="Fetch the latest predictions from MLflow for the most recent model version. This task connects to the MLflow tracking server, retrieves the latest model version, and fetches all runs associated with that model from the last 24 hours. The relevant data is extracted and saved as a Parquet file for further processing."
    )

    insert_task = PythonOperator(
        task_id="insert_postgres",
        python_callable=insert_into_postgres,
        doc_md="Insert the fetched predictions into the Postgres database. This task reads the Parquet file generated by the previous task, processes the data to add necessary columns, and performs a bulk insert into the mlflow_predictions table in Postgres. The task also handles the creation of the table if it does not exist and ensures that duplicate entries are avoided by using a primary key constraint on run_id."
    )

    fetch_task >> insert_task