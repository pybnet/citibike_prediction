import os
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Column schema
column_schema = {
    "station_id": "TEXT",
    "year": "INT",
    "month": "INT",
    "day": "INT",
    "hour": "BIGINT",
    "temp": "REAL",
    "precipitation_total": "REAL",
    "relative_humidity": "REAL",
    "average_wind_speed": "REAL",
    "num_bikes_taken_lag_1": "DOUBLE PRECISION",
    "num_bikes_dropped_lag_1": "DOUBLE PRECISION",
    "net_flow_lag_1": "DOUBLE PRECISION",
    "net_flow_lag_2": "DOUBLE PRECISION",
    "net_flow_lag_24": "DOUBLE PRECISION",
    "net_flow_roll_3": "DOUBLE PRECISION",
    "net_flow_roll_24": "DOUBLE PRECISION",
    "jour_semaine": "TEXT",
    "coco_group": "TEXT",
    "is_holiday": "BOOLEAN",
    "coco": "BIGINT",
    "net_flow": "BIGINT",
}

table_name = "historical_data"
historical_file = "/opt/airflow/data/citibike/reference/dataset_station_preprocessed.parquet"
chunk_size = 10000  # number of rows per batch

with DAG(
    "import_historical_citibike_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "historical"],
) as dag:

    def import_historical_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="postgres_citibike")

        # Drop & create table
        pg_hook.run(f"DROP TABLE IF EXISTS {table_name};")
        columns_sql = ", ".join(f"{col} {dtype}" for col, dtype in column_schema.items())
        pg_hook.run(f"CREATE TABLE {table_name} ({columns_sql});")
        print(f"Table {table_name} ready.")

        # Open Parquet with PyArrow
        parquet_file = pq.ParquetFile(historical_file)
        total_rows = 0

        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            df = batch.to_pandas()

            # Keep only columns in schema
            cols_to_keep = [col for col in column_schema.keys() if col in df.columns]
            df = df[cols_to_keep].copy()

            # Enforce column types
            for col, dtype in column_schema.items():
                if col in df.columns:
                    if dtype.upper() == "BOOLEAN":
                        df[col] = df[col].astype(bool)
                    elif "INT" in dtype.upper() or "BIGINT" in dtype.upper() or "REAL" in dtype.upper() or "DOUBLE" in dtype.upper():
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    else:
                        df[col] = df[col].astype(str)

            # Insert chunk
            pg_hook.insert_rows(
                table=table_name,
                rows=df.values.tolist(),
                target_fields=list(df.columns),
                commit_every=chunk_size,
            )

            total_rows += len(df)
            print(f"Imported chunk with {len(df)} rows")

        print(f"Total rows imported: {total_rows}")

    import_task = PythonOperator(
        task_id="import_historical_data",
        python_callable=import_historical_data,
        doc_md="Import historical CitiBike data from a Parquet file into the Postgres database. This task reads the Parquet file in batches using PyArrow, processes each batch to ensure it matches the defined schema, and inserts the data into the historical_data table in Postgres. The task also handles the creation of the table and logs the progress of the import process."
    )

    import_task