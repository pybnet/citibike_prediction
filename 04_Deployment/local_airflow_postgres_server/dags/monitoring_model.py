# Standard library
import os
import glob
import subprocess
from datetime import datetime, timedelta

# Data processing
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ML / Modeling
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from scipy import stats

# MLflow
import mlflow
from mlflow.tracking import MlflowClient

# Evidently (monitoring)
from evidently import Dataset, DataDefinition, Report
from evidently.presets import DataDriftPreset, RegressionPreset
from evidently.ui.workspace import CloudWorkspace

# Airflow
from airflow import DAG
from airflow.sdk import TaskGroup, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (PythonOperator, BranchPythonOperator)
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator

# Docker SDK
import docker
from docker.types import Mount


# DAG Configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# Evidently Cloud Configuration
EVIDENTLY_CLOUD_TOKEN = Variable.get("EVIDENTLY_CLOUD_TOKEN")
EVIDENTLY_CLOUD_PROJECT_ID = Variable.get("EVIDENTLY_CLOUD_PROJECT_ID")

# Configuration for MLflow
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_INTERNAL_URI")
DATA_DIR = '/Users/pierre-yvesbenevise/Documents/DataEngineer/Projects/velib_Final_Project_Fullstack_Lead/3_Model_&_Deployement/Pierre/local_airflow_postgres_server/data/citibike/retrain_data'

# Initialize PostgresHook
pg_hook = PostgresHook(postgres_conn_id="postgres_citibike")

# Helper Functions

def _load_files(data_logs_filename):
    """Load and prepare reference and current data for drift detection."""
    column_schema = {
        "station_id": "object",
        "month": "int32",
        "day": "int32",
        "hour": "int64",
        "temp": "float32",
        "precipitation_total": "float32",
        "relative_humidity": "float32",
        "average_wind_speed": "float32",
        "num_bikes_taken_lag_1": "float64",
        "num_bikes_dropped_lag_1": "float64",
        "net_flow_lag_1": "float64",
        "net_flow_lag_2": "float64",
        "net_flow_lag_24": "float64",
        "net_flow_roll_3": "float64",
        "net_flow_roll_24": "float64",
        "jour_semaine": "object",
        "coco_group": "object",
        "is_holiday": "bool",
        "coco": "int64",
        "net_flow": "int64",
    }

    file_path = "/opt/airflow/data/citibike/reference/top20_station_list.csv"
    top_stations = pd.read_csv(file_path, header=None, names=["station_id"])
    top_stations["station_id"] = top_stations["station_id"].astype(str).str.strip()

    cols_to_keep = list(column_schema.keys())
    reference = pd.read_parquet(
        "/opt/airflow/data/citibike/reference/last_month_data_reference.parquet",
        columns=cols_to_keep
    )
    reference = reference[reference["station_id"].isin(top_stations["station_id"])]
    reference_dataset = Dataset.from_pandas(reference, data_definition=DataDefinition())

    cols_to_keep = [col for col in column_schema.keys() if col in reference.columns]
    data_logs = pd.read_parquet(data_logs_filename, columns=cols_to_keep)
    data_logs = data_logs[data_logs["station_id"].isin(top_stations["station_id"])]
    data_logs_dataset = Dataset.from_pandas(data_logs, data_definition=DataDefinition())

    return reference_dataset, data_logs_dataset


def _detect_file(**context):
    """Sensor function to detect new data files for processing."""
    data_logs_list = glob.glob("/opt/airflow/data/citibike/data_drift/last_month_data_new.parquet")

    if not data_logs_list:
        print("No files 'last_month_data_new.parquet' in /opt/airflow/data/data_drift/")
        return False

    data_logs_filename = max(data_logs_list, key=os.path.getctime)
    print(f"✅ Found data file: {data_logs_filename}")

    ti = context["task_instance"]
    ti.xcom_push(key="data_logs_filename", value=data_logs_filename)
    print(f"📤 Pushed filename to XCom with key 'data_logs_filename'")
    return True


def _detect_data_drift(**context):
    ti = context["task_instance"]
    data_logs_filename = ti.xcom_pull(
        task_ids="drift_pipeline.detect_file",
        key="data_logs_filename"
    )

    if not data_logs_filename:
        print("⚠️ No data file found. Skipping drift detection.")
        ti.xcom_push(key="drift_detected", value=False)
        return False

    reference, data_logs = _load_files(data_logs_filename)
    data_drift_run = Report([DataDriftPreset()])
    result = data_drift_run.run(current_data=data_logs, reference_data=reference)
    report = result.dict()
    drift_flag = report["metrics"][0]["value"]["count"] > 0

    print(f"📊 Drift detected: {drift_flag}")
    ti.xcom_push(key="drift_detected", value=drift_flag)
    return drift_flag


def _data_drift_detected(**context):
    if not EVIDENTLY_CLOUD_TOKEN or not EVIDENTLY_CLOUD_PROJECT_ID:
        raise ValueError(
            "Evidently Cloud credentials not configured! "
            "Please set EVIDENTLY_CLOUD_TOKEN and EVIDENTLY_CLOUD_PROJECT_ID "
            "in Airflow Variables (Admin > Variables)"
        )

    ws = CloudWorkspace(
        token=EVIDENTLY_CLOUD_TOKEN,
        url="https://app.evidently.cloud"
    )
    project = ws.get_project(EVIDENTLY_CLOUD_PROJECT_ID)

    data_logs_filename = context["task_instance"].xcom_pull(
        task_ids="drift_pipeline.detect_file",
        key="data_logs_filename"
    )

    if not data_logs_filename:
        raise ValueError("No data file found in XCom from detect_file sensor.")

    reference, data_logs = _load_files(data_logs_filename)

    data_drift_report = Report([DataDriftPreset()])
    data_drift_result = data_drift_report.run(
        current_data=data_logs,
        reference_data=reference
    )

    ws.add_run(
        project_id=project.id,
        run=data_drift_result,
        include_data=True
    )
    print(f"✅ Drift report successfully uploaded to Evidently Cloud!")
    print(f"View it at: https://app.evidently.cloud/projects/{EVIDENTLY_CLOUD_PROJECT_ID}")


# FIX: renamed from branch_on_drift to _branch_on_drift to avoid collision
# with the BranchPythonOperator variable of the same name defined in the DAG
def _branch_on_drift(**context):
    ti = context['task_instance']
    drift_flag = ti.xcom_pull(
        task_ids="drift_pipeline.detect_data_drift",
        key="drift_detected"
    )
    if drift_flag:
        return "drift_pipeline.data_drift_detected"
    else:
        return "drift_pipeline.no_data_drift_detected"


def monitor_model_performance(**context):
    last_month = (pd.Timestamp.today().replace(day=1) - pd.Timedelta(days=1)).month

    sql = f"""
        SELECT h.station_id,
               h.hour,
               h.month,
               h.net_flow,
               h.jour_semaine,
               p.prediction AS net_flow_pred
        FROM historical_data h
        LEFT JOIN mlflow_predictions p
            ON h.station_id = p.station_id
            AND h.hour = p.hour
            AND h.month = p.month
            AND h.jour_semaine = p.jour_semaine
        WHERE h.month = {last_month}
          AND p.prediction IS NOT NULL
    """
    df = pg_hook.get_pandas_df(sql)

    if df.empty:
        print("No predictions available for last month.")
        # FIX: still push retrain=False so branch_retrain xcom_pull doesn't return None
        context['ti'].xcom_push(key="retrain", value=False)
        return

    y_true = df["net_flow"]
    y_pred = df["net_flow_pred"]

    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae  = float(mean_absolute_error(y_true, y_pred))
    r2   = float(r2_score(y_true, y_pred))

    print(f"Last month performance — RMSE: {rmse:.2f}, MAE: {mae:.2f}, R2: {r2:.3f}")

    experiment_name = "citibike_netflow_model_monitoring"

    # FIX: set tracking URI before set_experiment
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    client = MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    if exp and exp.lifecycle_stage == "deleted":
        client.restore_experiment(exp.experiment_id)

    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="monitor_performance"):
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae",  mae)
        mlflow.log_metric("r2",   r2)

        RMSE_THRESHOLD = 15
        retrain_flag = rmse > RMSE_THRESHOLD

        if retrain_flag:
            print("RMSE exceeds threshold — trigger retraining!")
        else:
            print("Performance within threshold — no retraining needed.")

        # FIX: use context['ti'] consistently (kwargs['ti'] works too but context is cleaner)
        context['ti'].xcom_push(key="retrain", value=retrain_flag)



def _detect_concept_drift(**context):
    """
    Detect concept drift using statistical tests on model residuals.

    Method: Two-sample Kolmogorov-Smirnov test comparing the residual
    distribution of the reference period vs the current period.
    A significant shift in residuals means the model's learned mapping
    has changed — i.e. concept drift — even if input features look the same.

    Additionally uses the Page-Hinkley test to detect the *point in time*
    when the drift started (sequential change-point detection on residuals).
    """
    ti = context["ti"]

    last_month = (pd.Timestamp.today().replace(day=1) - pd.Timedelta(days=1)).month
    ref_month  = last_month - 1 if last_month > 1 else 12

    # --- Load reference residuals (previous month actuals vs predictions) ---
    sql_ref = f"""
        SELECT h.net_flow AS target,
               p.prediction
        FROM historical_data h
        JOIN mlflow_predictions p
            ON h.station_id = p.station_id
            AND h.hour      = p.hour
            AND h.month     = p.month
            AND h.jour_semaine = p.jour_semaine
        WHERE h.month = {ref_month}
          AND p.prediction IS NOT NULL
        ORDER BY h.year, h.month, h.day, h.hour
    """

    # --- Load current residuals (last month actuals vs predictions) ---
    sql_cur = f"""
        SELECT h.net_flow AS target,
               p.prediction
        FROM historical_data h
        JOIN mlflow_predictions p
            ON h.station_id = p.station_id
            AND h.hour      = p.hour
            AND h.month     = p.month
            AND h.jour_semaine = p.jour_semaine
        WHERE h.month = {last_month}
          AND p.prediction IS NOT NULL
        ORDER BY h.year, h.month, h.day, h.hour
    """

    df_ref = pg_hook.get_pandas_df(sql_ref)
    df_cur = pg_hook.get_pandas_df(sql_cur)

    if df_ref.empty or df_cur.empty:
        print("⚠️ Not enough data for concept drift detection.")
        ti.xcom_push(key="concept_drift_detected", value=False)
        return False

    # Compute residuals: actual - predicted
    residuals_ref = (df_ref["target"] - df_ref["prediction"]).values
    residuals_cur = (df_cur["target"] - df_cur["prediction"]).values

    ref_mean = float(np.mean(residuals_ref))
    cur_mean = float(np.mean(residuals_cur))
    ref_std  = float(np.std(residuals_ref))
    cur_std  = float(np.std(residuals_cur))

    print(f"📐 Reference residuals — mean: {ref_mean:.4f}, std: {ref_std:.4f}")
    print(f"📐 Current residuals  — mean: {cur_mean:.4f}, std: {cur_std:.4f}")

    # -------------------------------------------------------------------
    # Test 1: Kolmogorov-Smirnov test
    # Tests whether the two residual distributions are drawn from the
    # same distribution. A low p-value means concept drift.
    # -------------------------------------------------------------------
    ks_stat, ks_pvalue = stats.ks_2samp(residuals_ref, residuals_cur)
    ks_drift = ks_pvalue < 0.05
    print(f"🔬 KS test — statistic: {ks_stat:.4f}, p-value: {ks_pvalue:.4f} → drift: {ks_drift}")

    # -------------------------------------------------------------------
    # Test 2: Welch's t-test
    # Tests whether the mean residual has shifted significantly.
    # Complementary to KS: KS catches shape changes, t-test catches bias shift.
    # -------------------------------------------------------------------
    t_stat, t_pvalue = stats.ttest_ind(residuals_ref, residuals_cur, equal_var=False)
    t_drift = t_pvalue < 0.05
    print(f"🔬 Welch t-test — statistic: {t_stat:.4f}, p-value: {t_pvalue:.4f} → drift: {t_drift}")

    # -------------------------------------------------------------------
    # Test 3: Page-Hinkley test (sequential change-point detection)
    # Scans through current residuals in order to find *when* drift started.
    # Raises an alarm when cumulative deviation from the reference mean
    # exceeds a threshold delta.
    # -------------------------------------------------------------------
    DELTA = 0.5    # sensitivity — lower = more sensitive
    LAMBDA = 10.0  # alarm threshold — lower = more sensitive
    cumsum = 0.0
    ph_min = 0.0
    ph_alarm_idx = None

    for i, r in enumerate(residuals_cur):
        cumsum += (r - ref_mean - DELTA)
        ph_min  = min(ph_min, cumsum)
        ph_stat = cumsum - ph_min
        if ph_stat > LAMBDA:
            ph_alarm_idx = i
            break

    ph_drift = ph_alarm_idx is not None
    if ph_drift:
        print(f"🔬 Page-Hinkley — alarm raised at observation {ph_alarm_idx} of {len(residuals_cur)}")
    else:
        print(f"🔬 Page-Hinkley — no alarm raised (max stat below threshold {LAMBDA})")

    # -------------------------------------------------------------------
    # Final decision: concept drift if at least 2 of 3 tests agree
    # (majority vote reduces false positives from individual tests)
    # -------------------------------------------------------------------
    votes = sum([ks_drift, t_drift, ph_drift])
    concept_drift = votes >= 2
    print(f"🧠 Concept drift decision — votes: {votes}/3 → drift: {concept_drift}")

    # --- Log to MLflow for traceability ---
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("citibike_netflow_model_monitoring")
    with mlflow.start_run(run_name="concept_drift_detection"):
        mlflow.log_metrics({
            "residual_mean_ref":   ref_mean,
            "residual_mean_cur":   cur_mean,
            "residual_std_ref":    ref_std,
            "residual_std_cur":    cur_std,
            "ks_statistic":        ks_stat,
            "ks_pvalue":           ks_pvalue,
            "t_statistic":         float(t_stat),
            "t_pvalue":            float(t_pvalue),
            "ph_alarm_index":      float(ph_alarm_idx) if ph_alarm_idx is not None else -1.0,
            "concept_drift_votes": float(votes),
        })
        mlflow.log_params({
            "ks_drift":    str(ks_drift),
            "t_drift":     str(t_drift),
            "ph_drift":    str(ph_drift),
            "final_drift": str(concept_drift),
            "ref_month":   str(ref_month),
            "cur_month":   str(last_month),
        })

    ti.xcom_push(key="concept_drift_detected", value=concept_drift)
    return concept_drift

def maybe_retrain(**context):
    ti = context["ti"]

    drift_flag         = ti.xcom_pull(task_ids="drift_pipeline.detect_data_drift",    key="drift_detected")         or False
    concept_drift_flag = ti.xcom_pull(task_ids="detect_concept_drift",                key="concept_drift_detected") or False
    performance_flag   = ti.xcom_pull(task_ids="monitor_model",                       key="retrain")                or False

    print(f"📊 Data drift flag:    {drift_flag}")
    print(f"🧠 Concept drift flag: {concept_drift_flag}")
    print(f"📉 Performance flag:   {performance_flag}")

    if drift_flag or concept_drift_flag or performance_flag:
        print("🚀 Trigger retraining")
        return "export_data_to_retrain"
    else:
        print("✅ Skip retraining")
        return "skip_retrain_task"


def export_historical_data_to_parquet():
    """Export all data from 'historical_data' table to a Parquet file in chunks."""
    output_folder = "/opt/airflow/data/citibike/retrain_data"
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, "historical_data.parquet")

    conn = pg_hook.get_conn()
    cursor = conn.cursor(name="historical_data_cursor")
    cursor.itersize = 10000

    sql = """
        SELECT h.*
        FROM historical_data h
        JOIN stations_reference s ON h.station_id = s.station_id
        ORDER BY h.station_id, h.year, h.month, h.day, h.hour;
    """
    cursor.execute(sql)

    writer = None
    total_rows = 0

    while True:
        rows = cursor.fetchmany(cursor.itersize)
        if not rows:
            break
        df_chunk = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        table = pa.Table.from_pandas(df_chunk)

        if writer is None:
            writer = pq.ParquetWriter(output_file, table.schema)
        writer.write_table(table)
        total_rows += len(df_chunk)
        print(f"✅ Exported chunk with {len(df_chunk)} rows")

    if writer:
        writer.close()
        print(f"✅ Export complete. Total rows: {total_rows}. File saved at {output_file}")
    else:
        print("⚠️ No data found in historical_data table")


def _clean_file(**context):
    # FIX: correct task_id — file was pushed by drift_pipeline.detect_file, not detect_file
    data_logs_filename = context["task_instance"].xcom_pull(
        task_ids="drift_pipeline.detect_file",
        key="data_logs_filename"
    )

    if data_logs_filename and os.path.exists(data_logs_filename):
        os.remove(data_logs_filename)
        print(f"🗑️  Cleaned up processed file: {data_logs_filename}")
    else:
        print(f"⚠️  File not found or already deleted: {data_logs_filename}")


# DAG Definition

with DAG(
    dag_id="monitoring_model",
    default_args=default_args,
    schedule="0 14 10 * *",
    catchup=False,
    description="Monitor Model performance, data drift and concept drift",
    tags=["monitoring", "data-quality", "evidently", "retraining"],
) as dag:

    start_task = EmptyOperator(
        task_id="start_pipeline",
        doc_md="Start of the monitoring pipeline."
    )

    # Drift Detection TaskGroup
    with TaskGroup("drift_pipeline") as drift_group:
        detect_file = PythonSensor(
            task_id="detect_file",
            python_callable=_detect_file,
            poke_interval=30,
            timeout=600,
            mode="poke",
            doc_md="Detects new data files for drift analysis."
        )

        detect_data_drift_task = PythonOperator(
            task_id="detect_data_drift",
            python_callable=_detect_data_drift,
            doc_md="Detect data drift and push drift flag to XCom"
        )

        # FIX: these two tasks must be defined inside the TaskGroup
        # so their fully qualified IDs match what branch_on_drift returns:
        # "drift_pipeline.data_drift_detected" and "drift_pipeline.no_data_drift_detected"
        data_drift_detected_task = PythonOperator(
            task_id="data_drift_detected",
            python_callable=_data_drift_detected,
            doc_md="Upload drift report to Evidently Cloud if drift detected"
        )

        no_data_drift_detected_task = EmptyOperator(
            task_id="no_data_drift_detected",
            doc_md="No data drift detected; continue normal operation"
        )

        # FIX: branch operator must also live inside the TaskGroup
        # so it can route to sibling tasks by their qualified IDs
        branch_on_drift_op = BranchPythonOperator(
            task_id="branch_on_drift",
            python_callable=_branch_on_drift,   # FIX: use renamed function
        )

        detect_file >> detect_data_drift_task >> branch_on_drift_op
        branch_on_drift_op >> [data_drift_detected_task, no_data_drift_detected_task]

    monitor_model_task = PythonOperator(
        task_id="monitor_model",
        python_callable=monitor_model_performance,
        doc_md="Monitor model performance and push retrain flag to XCom"
    )

    detect_concept_drift_task = PythonOperator(
        task_id="detect_concept_drift",
        python_callable=_detect_concept_drift,
        doc_md="Detect concept drift via KS test, Welch t-test and Page-Hinkley on residuals"
    )

    branch_retrain = BranchPythonOperator(
        task_id="branch_retrain",
        python_callable=maybe_retrain,
        # FIX: default trigger_rule="all_success" causes branch_retrain to be skipped
        # whenever one of the drift branch tasks is skipped by BranchPythonOperator.
        # "none_failed_min_one_success" runs as long as at least one upstream succeeded.
        trigger_rule="none_failed_min_one_success",
        doc_md="Decide whether to retrain based on drift OR performance flags"
    )

    export_data_to_retrain = PythonOperator(
        task_id="export_data_to_retrain",
        python_callable=export_historical_data_to_parquet,
        doc_md="Export historical data for retraining"
    )

    train_model = DockerOperator(
        task_id="train_model",
        image="pybnet/citibikeproject:v1.4",
        api_version="auto",
        auto_remove='success',
        command="python retrain.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "MLFLOW_S3_ENDPOINT_URL": Variable.get("MLFLOW_S3_ENDPOINT_INTERNAL_URL"),
            "AWS_ACCESS_KEY_ID": Variable.get("MINIO_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_AWS_SECRET_ACCESS_KEY"),
        },
        mounts=[Mount(source=DATA_DIR, target="/app/data", type="bind", read_only=False)],
        doc_md="Train model in Docker with latest data"
    )

    skip_retrain_task = EmptyOperator(task_id="skip_retrain_task")

    clean_file = PythonOperator(
        task_id="clean_file",
        python_callable=_clean_file,
        trigger_rule="none_failed_min_one_success",
        doc_md="Clean up processed data file"
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # DAG Dependencies
    start_task >> [drift_group, monitor_model_task, detect_concept_drift_task]

    # FIX: wait for both branches of the drift group to converge before branch_retrain
    [data_drift_detected_task, no_data_drift_detected_task, monitor_model_task, detect_concept_drift_task] >> branch_retrain

    branch_retrain >> [export_data_to_retrain, skip_retrain_task]

    export_data_to_retrain >> train_model >> clean_file
    skip_retrain_task >> clean_file
    clean_file >> end