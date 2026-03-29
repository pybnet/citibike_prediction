from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import calendar
from pathlib import Path
import requests
import http.client
import json
import zipfile
import pandas as pd
import os
import io
import boto3
import shutil
import re
import holidays
import pyarrow.parquet as pq
import pyarrow as pa
import shutil


# CONFIG
DATA_URL_BASE = "https://s3.amazonaws.com/tripdata/"
LOCAL_DIR = "/tmp/data/"
S3_BUCKET = Variable.get("S3_BUCKET")
S3_RAW_FOLDER = "citibike/data/raw/"
S3_CLEAN_FOLDER = "citibike/data/clean/"
AWS_CONN_ID = "s3_citibike"
RAPIDAPI_KEY = Variable.get("RAPIDAPI_KEY")
WEATHER_STATION = "KNYC0"
TZ = "Europe/Berlin"

# AWS client for S3
s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

#Postgres
pg_hook = PostgresHook(postgres_conn_id="postgres_citibike")

def generate_holidays_to_postgres(): 
    
    # Generate US holidays
    us_holidays = holidays.US(years=range(2020, 2050))

    # Only keep date + flag
    df = pd.DataFrame({
        "date": pd.to_datetime(list(us_holidays.keys())),
        "is_holiday": True
    })

    records = list(df.itertuples(index=False, name=None))
    
    create_sql = """
    DROP TABLE IF EXISTS dim_holidays;
    CREATE TABLE dim_holidays (
        date DATE PRIMARY KEY,
        is_holiday BOOLEAN NOT NULL
    );
    """
    pg_hook.run(create_sql)
    
    insert_sql = "INSERT INTO dim_holidays (date, is_holiday) VALUES (%s, %s)"
    pg_hook.insert_rows(table="dim_holidays", rows=records, target_fields=["date", "is_holiday"])

def clean_local_dir():
    shutil.rmtree(LOCAL_DIR, ignore_errors=True)
    print(f"{LOCAL_DIR} cleaned and ready for new files.")

def download_citibike_station_data(**kwargs):
    #Information stations
    download_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

    # request the data
    response = requests.get(download_url)
    response.raise_for_status()
    print(f"Downloading: {download_url}")
    
    data = response.json()

    # convert to dataframe
    all_stations_informations = pd.DataFrame(data["data"]["stations"])[
        ["short_name", "station_id", "name", "capacity", "lat", "lon"]
    ]
    print(f"Downloaded station information for {len(all_stations_informations)} stations.")

    # Save locally
    os.makedirs(LOCAL_DIR, exist_ok=True)
    local_file = os.path.join(LOCAL_DIR, f"station-data.csv")
    all_stations_informations.to_csv(local_file, index=False)
    print(f"Station data saved to {local_file}")
    kwargs["ti"].xcom_push(key="station_data_file_path", value=local_file)

def load_top20_stations(**kwargs):
    file_path = "data/citibike/reference/top20_station_list.csv"

    top_stations = pd.read_csv(file_path, header=None, names=["station_id"])

    # Ensure station_id is string and strip any whitespace
    top_stations["station_id"] = top_stations["station_id"].astype(str).str.strip()
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stations_reference (
        station_id TEXT PRIMARY KEY
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # Convert to list of tuples
    rows = [(sid,) for sid in top_stations["station_id"].tolist()]

    # Insert into Postgres
    pg_hook.insert_rows(
        table="stations_reference",
        rows=rows,
        target_fields=["station_id"],
        replace=False  # avoids overwriting
    )

    print(f"Inserted {len(rows)} station_ids from CSV")

def load_station_data_to_postgres(**kwargs):   
         
    csv_file= kwargs['ti'].xcom_pull(key="station_data_file_path", task_ids="station_pipeline.download_citibike_station_data")
    df = pd.read_csv(csv_file)
    
    print(f"Number of rows in CSV: {len(df)}")
     
    # --- Create table and index ---
    pg_hook.run("DROP TABLE IF EXISTS public.citibike_stations CASCADE;")
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS citibike_stations (
            id SERIAL PRIMARY KEY,
            short_name VARCHAR(50),
            station_id VARCHAR(50),
            name VARCHAR(255),
            capacity INTEGER,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION
        );
    """)
    pg_hook.run("CREATE INDEX IF NOT EXISTS idx_short_name ON citibike_stations(short_name);")

    # --- Insert rows in batches ---
    pg_hook.insert_rows(
        table="citibike_stations",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
        commit_every=500,  # commit every 500 rows to avoid large transactions
    )
    print("Inserted station data into Postgres successfully.")

def download_citibike_previous_month_data(**kwargs):
    logical_date = kwargs["logical_date"]
    prev_month_date = (logical_date.replace(day=1) - timedelta(days=1))
    year_month = prev_month_date.strftime("%Y%m")

    file_name = f"{year_month}-citibike-tripdata.zip"
    local_path = f"/tmp/{file_name}"   # ZIP file location

    os.makedirs("/tmp", exist_ok=True)
    download_url = f"https://s3.amazonaws.com/tripdata/{file_name}"
    print(f"Downloading: {download_url}")

    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded to {local_path}")

    # Push the full ZIP file path to XCom
    kwargs["ti"].xcom_push(key="zip_file_path", value=local_path)
    return local_path

def unzip_file(**kwargs):
    ti = kwargs["ti"]
    zip_file_path = ti.xcom_pull(
        key="zip_file_path",
        task_ids="trip_pipeline.download_citibike_previous_month"
    )

    print("ZIP file path from XCom:", zip_file_path)
    if not zip_file_path or not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"ZIP file not found: {zip_file_path}")

    os.makedirs(LOCAL_DIR, exist_ok=True)
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(LOCAL_DIR)

    print(f"Extracted all CSVs to {LOCAL_DIR}")

def load_citibike_csvs_to_postgres(**kwargs):
    """
    Loads selected columns from all CSVs in LOCAL_DIR into a new Postgres table.
    Only keeps specific columns and creates table/index if necessary.
    """
    table_name = "citibike_trips_raw"
    columns_to_keep = [
        'started_at', 'ended_at', 'start_station_id', 'end_station_id',
        "rideable_type", 'member_casual', 'start_lat', 'start_lng', 'end_lat', 'end_lng'
    ]

    # Create table if not exists (adjust columns to match CSVs)
    pg_hook.run(f"""DROP TABLE IF EXISTS public.{table_name} CASCADE;""")
    pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
            id SERIAL PRIMARY KEY,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            start_station_id VARCHAR(50),
            end_station_id VARCHAR(50),
            rideable_type VARCHAR(50),
            member_casual VARCHAR(50),
            start_lat DOUBLE PRECISION,
            start_lng DOUBLE PRECISION,
            end_lat DOUBLE PRECISION,
            end_lng DOUBLE PRECISION
        );
    """)
    pg_hook.run(f"CREATE INDEX IF NOT EXISTS idx_started_at ON {table_name}(started_at);")
    pg_hook.run(f"CREATE INDEX IF NOT EXISTS idx_start_station ON {table_name}(start_station_id);")
    pg_hook.run(f"CREATE INDEX IF NOT EXISTS idx_end_station ON {table_name}(end_station_id);")

    # Pattern to match files like 202602-citibike-tripdata_1.csv
    pattern = re.compile(r"^\d{6}-citibike-tripdata_\d+\.csv$")

    # Iterate over CSV files in LOCAL_DIR
    for file_name in os.listdir(LOCAL_DIR):
        if pattern.match(file_name):
            csv_path = os.path.join(LOCAL_DIR, file_name)
            print(f"Loading {csv_path} into {table_name}")

            # Read CSV and keep only relevant columns
            df = pd.read_csv(csv_path, usecols=lambda x: x in columns_to_keep)
            print(f"Number of rows in {csv_path}: {len(df)}")
            
            # Convert datetime columns
            for col in ["started_at", "ended_at"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Insert into Postgres in batches
            pg_hook.insert_rows(
                table=table_name,
                rows=df.values.tolist(),
                target_fields=list(df.columns),
                commit_every=1000
            )
            print(f"Loaded {len(df)} rows from {file_name}")

def download_weather(**kwargs):
    # Get execution_date from context
    execution_date = kwargs.get("execution_date")  # Airflow passes this automatically
    if execution_date is None:
        execution_date = datetime.today()

    # Compute previous month
    prev_month_date = execution_date - relativedelta(months=1)
    month = prev_month_date.month
    year = prev_month_date.year

    print(f"Fetching weather for {year}-{month:02d}")

    # Compute first and last day of the previous month
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)
    
    conn = http.client.HTTPSConnection("meteostat.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': "meteostat.p.rapidapi.com"
    }
    
    url = f"/stations/hourly?station={WEATHER_STATION}&start={start_date.date()}&end={end_date.date()}&tz={TZ}"
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    weather_json = json.loads(res.read().decode("utf-8"))
    weather_data = weather_json["data"]
    weather_df = pd.DataFrame(weather_data)

    # Process dataframe
    weather_df["time"] = pd.to_datetime(weather_df["time"])
    weather_df = weather_df.rename(columns={
        "rhum": "relative_humidity",
        "prcp": "precipitation_total",
        "wspd": "average_wind_speed",
    })
    
    keep_columns = ["time",'temp', 'relative_humidity', 'precipitation_total', 'average_wind_speed', 'coco']
    weather_df = weather_df[keep_columns]
    
    # Save locally
    os.makedirs(LOCAL_DIR, exist_ok=True)
    month_partition = start_date.strftime("%Y%m")
    local_file = os.path.join(LOCAL_DIR, f"{month_partition}-weather-data.csv")
    weather_df.to_csv(local_file, index=False)
    print(f"Weather data saved to {local_file}")

    kwargs['ti'].xcom_push(key="weather_file_path", value=local_file)
    
def load_weather_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key="weather_file_path", task_ids="weather_pipeline.download_weather")

    # Read CSV from file
    weather_df = pd.read_csv(file_path, parse_dates=["time"])

    # --- Create table and index ---
    pg_hook.run("DROP TABLE IF EXISTS public.weather_data CASCADE;")
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            time TIMESTAMP,
            temp DOUBLE PRECISION,
            relative_humidity DOUBLE PRECISION,
            precipitation_total DOUBLE PRECISION,
            average_wind_speed DOUBLE PRECISION,
            coco VARCHAR(50)
        );
    """)
    pg_hook.run("CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_data(time);")

    # --- Insert rows in batches ---
    pg_hook.insert_rows(
        table="weather_data",
        rows=weather_df.values.tolist(),
        target_fields=list(weather_df.columns),
        commit_every=500,
    )
    print("Weather data inserted into Postgres successfully.")

def upload_to_s3():
    month_partition = (datetime.today() - timedelta(days=30)).strftime("%Y-%m")
    for file in os.listdir(LOCAL_DIR):
        if file.endswith(".csv"):
            local_file = os.path.join(LOCAL_DIR, file)
            s3_key = f"{S3_RAW_FOLDER}{month_partition}/{file}"

            # Use S3Hook to upload
            s3_hook.load_file(
                filename=local_file,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True  # overwrite if exists
            )
            print(f"Uploaded {local_file} to s3://{S3_BUCKET}/{s3_key}")
            
def drop_tables():
    TABLES_TO_DROP = [
    "public.int_biketrip_cleaned",
    "public.int_biketrip_features",
    "public.int_biketrip_hourly_drops",
    "public.int_biketrip_hourly_pickups",
    "public.int_biketrip_hourly_net_flow",
    "public.int_weather_hourly",
    "public.stg_weather_hourly"
    ]
    
    for table in TABLES_TO_DROP:
        sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
        pg_hook.run(sql)
        print(f"Dropped table {table} if it existed.")

def extract_last_month_data_to_parquet(**kwargs):

    # Get last month
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_month = (first_day_of_current_month - timedelta(days=1)).month

    # Define target table
    target_table = "last_month_data"

    # Drop if exists
    pg_hook.run(f"DROP TABLE IF EXISTS {target_table};")

    # Create table with same schema as historical_data
    create_sql = f"""
        CREATE TABLE {target_table} AS
        SELECT *
        FROM historical_data
        WHERE month = {last_month};
    """
    pg_hook.run(create_sql)
    print(f"Extracted data for month {last_month} into {target_table}.")
    
    # Chunked reading
    chunksize = 10000
    offset = 0
    total_rows = 0
    output_dir = "/opt/airflow/data/citibike/reference"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"last_month_data_reference.parquet")

    # Initialize Parquet writer
    writer = None

    while True:
        sql = f"SELECT * FROM {target_table} ORDER BY station_id, day, hour LIMIT {chunksize} OFFSET {offset};"
        chunk = pg_hook.get_pandas_df(sql)
        if chunk.empty:
            break

        table = pa.Table.from_pandas(chunk)
        if writer is None:
            writer = pq.ParquetWriter(output_file, table.schema)

        writer.write_table(table)
        total_rows += len(chunk)
        offset += len(chunk)
        print(f"Processed chunk with {len(chunk)} rows, total rows so far: {total_rows}")

    if writer:
        writer.close()
        print(f"Saved last-month data to {output_file}, total rows: {total_rows}")

        
def export_data_to_s3_parquet(chunksize=10000):
    sql = "SELECT * FROM public.mart_hourly_station_grid"
    month_partition = (datetime.today() - timedelta(days=30)).strftime("%Y-%m")
    file_name = f"{month_partition}-hourly_station.parquet"
    local_file = f"/tmp/{file_name}"

    # Remove old file if exists
    if os.path.exists(local_file):
        os.remove(local_file)

    conn = pg_hook.get_conn()

    chunk_counter = 0
    writer = None
    for chunk in pd.read_sql(sql, conn, chunksize=chunksize):
        if chunk.empty:
            continue

        table = pa.Table.from_pandas(chunk)

        if writer is None:
            writer = pq.ParquetWriter(local_file, table.schema)
        
        writer.write_table(table)
        chunk_counter += 1
        print(f"Processed chunk {chunk_counter} with {len(chunk)} rows")

    if writer:
        writer.close()
    else:
        print("No data to export.")
        return

    # Upload to S3
    s3_key = f"{S3_CLEAN_FOLDER}{file_name}"
    s3_hook.load_file(
        filename=local_file,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Exported {chunk_counter * chunksize} rows to S3 in Parquet format successfully!")

def export_mart_to_parquet(**context):
    """
    Export mart_hourly_station_grid to Parquet for data drift monitoring.
    Saves to /opt/airflow/data/citibike/data_drift/last_month_data_new.parquet
    """
    output_path = "/opt/airflow/data/citibike/data_drift/last_month_data_new.parquet"

    # Ensure folder exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Fetch data in chunks
    sql = "SELECT * FROM mart_hourly_station_grid"
    chunksize = 10000
    conn = pg_hook.get_conn()
    
    writer = None
    total_rows = 0

    for chunk in pd.read_sql(sql, conn, chunksize=chunksize):
        if chunk.empty:
            continue

        # Optional: enforce column types
        column_schema = {
            "station_id": "object",
            "year": "int32",
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

        cols_to_keep = [col for col in column_schema.keys() if col in chunk.columns]
        chunk = chunk[cols_to_keep].copy()

        for col, dtype in column_schema.items():
            if col in chunk.columns:
                if dtype == "bool":
                    chunk[col] = chunk[col].astype(bool)
                elif dtype.startswith("int") or dtype.startswith("float"):
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype(dtype)
                else:
                    chunk[col] = chunk[col].astype(dtype)

        chunk = chunk[list(column_schema.keys())]

        # Write chunk to Parquet
        table = pa.Table.from_pandas(chunk)
        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema)
        writer.write_table(table)

        total_rows += len(chunk)
        print(f"Processed chunk with {len(chunk)} rows")

    if writer:
        writer.close()
        print(f"Exported {total_rows} rows to {output_path}")
    else:
        print("No data to export.")

def load_historical_data(**kwargs):
    
    #Ensure unique constraint exists safely
    create_unique_sql = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'historical_data_unique'
            ) THEN
                ALTER TABLE historical_data
                ADD CONSTRAINT historical_data_unique
                UNIQUE (station_id, year, month, day, hour);
            END IF;
        END$$;
        """
    pg_hook.run(create_unique_sql)
    
    insert_sql = """
        INSERT INTO historical_data (
            station_id,
            year,
            month,
            day,
            hour,
            temp,
            precipitation_total,
            relative_humidity,
            average_wind_speed,
            num_bikes_taken_lag_1,
            num_bikes_dropped_lag_1,
            net_flow_lag_1,
            net_flow_lag_2,
            net_flow_lag_24,
            net_flow_roll_3,
            net_flow_roll_24,
            jour_semaine,
            coco_group,
            is_holiday,
            coco,
            net_flow
        )
        SELECT
            m.station_id,
            m.year,
            m.month,
            m.day,
            m.hour,
            m.temp,
            m.precipitation_total,
            m.relative_humidity,
            m.average_wind_speed,
            m.num_bikes_taken_lag_1,
            m.num_bikes_dropped_lag_1,
            m.net_flow_lag_1,
            m.net_flow_lag_2,
            m.net_flow_lag_24,
            m.net_flow_roll_3,
            m.net_flow_roll_24,
            m.jour_semaine,
            m.coco_group,
            m.is_holiday,
            m.coco,
            m.net_flow
        FROM mart_hourly_station_grid m
        ON CONFLICT (station_id, year, month, day, hour)
        DO NOTHING;

        TRUNCATE TABLE mart_hourly_station_grid;
        """

    pg_hook.run(insert_sql)

    print("Data moved from mart_hourly_station_grid to historical_data and mart_hourly_station_grid truncated")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="citibike_project_new_data_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 10),
    schedule="0 2 10 * *",  # 02:00 on 10th of every month
    catchup=False,
    tags=["citibike","trip", "station", "weather", "etl"],
) as dag:

    start_task = EmptyOperator(
            task_id="start_pipeline",
            doc_md="Starting point of the DAG. This task does not perform any action but serves as an anchor for the beginning of the workflow."
        )
    
    generate_dim_holidays = PythonOperator(
        task_id="generate_dim_holidays",
        python_callable=generate_holidays_to_postgres,
        doc_md="Generate a dimension table of US holidays for the next 30 years and load it into Postgres. This is used for enriching the data with holiday information in later steps."   
        )
    
    load_top20_stations_reference = PythonOperator(
        task_id="load_top20_stations",
        python_callable=load_top20_stations,
        doc_md="Load the top 20 stations reference data into Postgres. This is used for filtering and analysis in later steps."
        )   
    
    #Citibike trip data download and unzip are grouped together as they are sequential steps of the same logical task
    with TaskGroup("trip_pipeline") as trip_group:

        task_download_citibike_trip_data = PythonOperator(
            task_id="download_citibike_previous_month",
            python_callable=download_citibike_previous_month_data,
            doc_md="Download the previous month's Citibike trip data as a ZIP file from the public S3 bucket. The file is named in the format YYYYMM-citibike-tripdata.zip. The downloaded ZIP file path is pushed to XCom for use in the next step."
        )

        task_unzip = PythonOperator(
            task_id="unzip",
            python_callable=unzip_file,
            doc_md="Unzip the downloaded Citibike trip data ZIP file. The path to the ZIP file is retrieved from XCom. The extracted CSV files are saved to a local directory for further processing."
        )
        
        load_trip_data_task = PythonOperator(
            task_id="load_trip_data",
            python_callable=load_citibike_csvs_to_postgres,
            doc_md="Load the extracted Citibike trip data CSV files into a Postgres table. Only specific columns are kept for analysis. The table is created with appropriate data types and indexes for performance. Data is inserted in batches to handle large files efficiently."
        )

        task_download_citibike_trip_data >> task_unzip >> load_trip_data_task
        
    #Downloadeing weather data
    with TaskGroup("weather_pipeline") as weather_group:

        task_download_weather = PythonOperator(
                task_id="download_weather",
                python_callable=download_weather,
                doc_md="Download hourly weather data for the previous month from the Meteostat API via RapidAPI. The data includes temperature, humidity, precipitation, and wind speed. The retrieved data is saved as a CSV file locally, and the file path is pushed to XCom for use in the next step."
            )
        
        load_weather_task = PythonOperator(
            task_id="load_weather_data",
            python_callable=load_weather_data_to_postgres,
            doc_md="Load the downloaded weather data CSV file into a Postgres table. The table is created with appropriate data types and indexes for efficient querying. Data is inserted in batches to handle larger files, and the file path is retrieved from XCom."
        )
        
        task_download_weather >> load_weather_task
    
    with TaskGroup("station_pipeline") as station_group:

        task_download_station_info = PythonOperator(
            task_id="download_citibike_station_data",
            python_callable=download_citibike_station_data,
            doc_md = "Download the latest Citibike station information from the GBFS feed. The data includes station ID, name, capacity, and location. The retrieved data is saved as a CSV file locally, and the file path is pushed to XCom for use in the next step."
        )
        
        load_station_data_task = PythonOperator(
            task_id="load_station_data",
            python_callable=load_station_data_to_postgres,
            doc_md = "Load the downloaded Citibike station information CSV file into a Postgres table. The table is created with appropriate data types and indexes for efficient querying. Data is inserted in batches to handle larger files, and the file path is retrieved from XCom."
        )
        
        task_download_station_info >> load_station_data_task
    
    wait_task = EmptyOperator(
        task_id="gather_pipeline",
        doc_md="Wait for all data pipelines (trip, weather, station) to complete before proceeding to the cleaning and uploading steps. This ensures that all necessary data is available for the subsequent tasks."
    )
    
    with TaskGroup("clean_send_pipeline") as clean_send_group:

        task_upload_s3 = PythonOperator(
            task_id="upload_to_s3",
            python_callable=upload_to_s3,
            doc_md="Upload the cleaned CSV files from the local directory to an S3 bucket. The files are organized in S3 by month. The task uses the S3Hook to handle the file upload, and it replaces existing files with the same name in the target location."
        )
        
        task_upload_s3
        
    with TaskGroup("dbt_pipeline") as dbt_group:
      
        start_dbt = EmptyOperator(task_id="start_dbt")

        with TaskGroup("dbt_setup") as dbt_setup:
            #Install dependencies
            dbt_deps = BashOperator(
                task_id='dbt_deps',
                bash_command='cd /opt/airflow/dbt_project && dbt deps --profiles-dir /opt/airflow/dbt_config',
                doc_md="Install dbt dependencies using dbt deps command. This ensures that all required packages and dependencies for the dbt project are installed before running any models. The command is executed in the context of the dbt project directory, and it uses a specified profiles directory for configuration."
            )
            
            #Drop previous mart/intermediate tables
            drop_tables_previous_table = BashOperator(
                task_id='drop_biketrip_tables',
                bash_command='cd /opt/airflow/dbt_project && dbt run-operation drop_biketrip_tables --profiles-dir /opt/airflow/dbt_config',
                doc_md="Run a dbt macro to drop previous intermediate and mart tables related to biketrip data. This is done to ensure that the dbt models can run without conflicts from existing tables. The command executes a custom dbt operation defined in the project, which handles the dropping of specific tables in the database."
            )
            dbt_deps >> drop_tables_previous_table
            
        # Staging / Intermediate
        with TaskGroup("staging") as staging:
            run_staging_intermediate = BashOperator(
                task_id='run_staging_intermediate',
                bash_command=(
                    'cd /opt/airflow/dbt_project && '
                    'dbt run --profiles-dir /opt/airflow/dbt_config '
                    '--select stg_biketrip_raw+ stg_citibike_stations+ int_biketrip_hourly_net_flow_dedup '
                    '--exclude mart_hourly_station_grid'                    
                ),
                doc_md="Run dbt models for staging and intermediate layers. This includes models that transform raw biketrip data and station information into cleaned and aggregated formats. The command selects specific models to run while excluding the final mart model, allowing for testing and validation of the intermediate transformations before the final aggregation step."
            )
            run_staging_intermediate
            
        # Weather
        with TaskGroup("weather") as weather:    
            #Run mart_weather_hourly
            run_weather = BashOperator(
                task_id='run_int_weather_hourly',
                bash_command=(
                    'cd /opt/airflow/dbt_project && '
                    'dbt run --profiles-dir /opt/airflow/dbt_config '
                    '--select stg_weather_hourly+ int_weather_hourly '
                    '--exclude mart_hourly_station_grid' 
                ),
                doc_md="Run dbt models for weather data. This includes staging models that clean and prepare the raw weather data, as well as intermediate models that aggregate the weather data on an hourly basis. The command selects the relevant models to run while excluding the final mart model, allowing for testing and validation of the weather data transformations before they are integrated into the final station grid model."
            )

            #Test mart_weather_hourly
            test_weather = BashOperator(
                task_id='test_int_weather_hourly',
                bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir /opt/airflow/dbt_config --select int_weather_hourly',
                doc_md="Run dbt tests for the intermediate weather model. This ensures that the transformations applied to the weather data are producing expected results and that the data quality is maintained. The command executes dbt tests specifically for the int_weather_hourly model, allowing for validation of the weather data before it is used in the final station grid model."
            )
            run_weather >> test_weather
        
        # Station Grid
        with TaskGroup("station_grid") as station_grid:    
            #Run mart_hourly_station_grid
            run_station_grid = BashOperator(
                task_id='run_mart_hourly_station_grid',
                bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir /opt/airflow/dbt_config --select mart_hourly_station_grid',
                doc_md="Run the dbt model for the final mart_hourly_station_grid. This model aggregates and combines the biketrip data with the weather data and station information to create a comprehensive hourly station grid. The command executes the dbt run for this specific model, which is the final step in the dbt transformation process before testing and exporting the data."
            )

            #Test mart_hourly_station_grid
            test_station_grid = BashOperator(
                task_id='test_mart_hourly_station_grid',
                bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir /opt/airflow/dbt_config --select mart_hourly_station_grid',
                doc_md="Run dbt tests for the final mart_hourly_station_grid model. This ensures that the final aggregated data is accurate and meets the defined data quality standards. The command executes dbt tests specifically for the mart_hourly_station_grid model, allowing for validation of the final output before it is exported and used for analysis."
            )
            
            #test test_net_flow
            test_net_flow = BashOperator(
                task_id='test_net_flow_consistency',
                bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir /opt/airflow/dbt_config --select net_flow_consistency',
                doc_md="Run a custom dbt test to check the consistency of the net flow calculations in the mart_hourly_station_grid. This test ensures that the net flow values are correctly calculated based on the pickups and drop-offs, and that there are no discrepancies in the data. The command executes a specific dbt test defined in the project that focuses on validating the net flow logic in the final station grid model."
            )
            run_station_grid >> test_station_grid >> test_net_flow
            
        # Documentation
        with TaskGroup("documentation") as documentation:
            generate_docs = BashOperator(
                task_id='generate_docs',
                bash_command='cd /opt/airflow/dbt_project && dbt docs generate --profiles-dir /opt/airflow/dbt_config',
                doc_md="Generate dbt documentation for the project. This creates an HTML site that provides detailed information about the dbt models, including their structure, dependencies, and data lineage. The command executes dbt docs generate, which compiles the documentation based on the current state of the dbt project and the models that have been run."
            )
            
            # Generate Generate DAG PNG
            generate_png = BashOperator(
                task_id="generate_png",
                bash_command="cd /opt/airflow/dbt_project && python3 generate_dbt_png.py",
                doc_md="Generate a PNG visualization of the dbt DAG. This custom script creates a visual representation of the dbt model dependencies and structure, which can be useful for understanding the flow of data transformations in the project. The command executes a Python script that generates the PNG file based on the dbt project configuration and model relationships."
            )
            generate_docs >> generate_png
        
        
        export_task= PythonOperator(
                task_id="export_hourly_station_to_s3",
                python_callable=export_data_to_s3_parquet,
                doc_md="Export the final mart_hourly_station_grid data to S3 in Parquet format. This task reads the data from Postgres in chunks, converts it to Parquet format using PyArrow, and uploads it to an S3 bucket. The exported data can then be used for further analysis or monitoring of data drift."
            )
                
        drop_worked_tables = PythonOperator(
            task_id="drop_works_tables",
            python_callable=drop_tables,
            doc_md="Drop intermediate and staging tables that were used during the dbt transformations. This is done to clean up the database and free up space after the final data has been exported. The task runs a Python function that executes SQL commands to drop specific tables in the Postgres database."
        )   
            
        end_dbt = EmptyOperator(task_id="end_dbt")
            
        # Set task dependencies
        # Make dbt_setup run first
        start_dbt >> dbt_setup

        # Staging and Weather depend on DBT setup
        dbt_setup >> [staging, weather]

        # Station Grid depends on Staging and Weather completion
        [staging, weather] >> station_grid >> documentation >> export_task >> drop_worked_tables >> end_dbt
        
    task_clean_local_dir = PythonOperator(
            task_id="clean_local_dir",
            python_callable=clean_local_dir,
            doc_md="Clean the local directory by removing all CSV files. This is done to free up space and ensure that old files do not interfere with future runs of the pipeline. The task uses a Python function to list all CSV files in the specified local directory and deletes them, logging the actions taken."
        ) 
    
    export_result = PythonOperator(
            task_id="export_result",
            python_callable=export_mart_to_parquet,
            doc_md="Export the final mart_hourly_station_grid data to a Parquet file for data drift monitoring. This task reads the data from Postgres in chunks, converts it to Parquet format using PyArrow, and saves it to a specified location. The exported Parquet file can then be used for monitoring changes in the data distribution over time."
        )      
    extract_last_month_task = PythonOperator(
        task_id="extract_last_month_data",
        python_callable=extract_last_month_data_to_parquet,
        doc_md="Extract the last month's data from the historical_data table and save it as a Parquet file. This task creates a new table with the relevant data for the last month, reads it in chunks, converts it to Parquet format using PyArrow, and saves it to a specified location. The extracted Parquet file can be used for reference and analysis of recent data trends."
    )
    
    load_historical_data_task = PythonOperator(
            task_id="load_data_to_historical_data_table",
            python_callable=load_historical_data,
            doc_md="Load the data from the mart_hourly_station_grid into the historical_data table. This task ensures that the historical_data table is updated with the latest aggregated data, while maintaining a unique constraint to prevent duplicate entries. The task runs a Python function that executes SQL commands to insert new data into the historical_data table and truncate the mart_hourly_station_grid for the next cycle."
        )
    
    end_task = EmptyOperator(
        task_id="end_pipeline",
        doc_md="End point of the DAG. This task does not perform any action but serves as an anchor for the end of the workflow, indicating that all previous tasks have been completed successfully."
    )

    # Fan-out
    start_task >> [trip_group, weather_group, station_group, generate_dim_holidays, load_top20_stations_reference]

    # Fan-in
    [trip_group, weather_group, station_group, generate_dim_holidays, load_top20_stations_reference] >> wait_task

    # Fan-out again
    wait_task >> [clean_send_group, dbt_group] >> task_clean_local_dir >> export_result >> extract_last_month_task >> load_historical_data_task >> end_task

    
        

