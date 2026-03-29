import json
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv() 
NEON_CONN_STRING = os.getenv("BACKEND_STORE_URI")

EXPECTED_COLUMNS = {
    # Core identifiers
    "id": "SERIAL PRIMARY KEY",
    "station_id": "TEXT",
    "year" : "INT",
    "month": "INT",
    "day":"INT",
    "hour": "INT",

    # Weather
    "temp": "FLOAT",
    "relative_humidity": "FLOAT",
    "precipitation_total": "FLOAT",
    "average_wind_speed": "FLOAT",
    "coco": "INT",
    "coco_group": "TEXT",

    # Lag / rolling features
    "net_flow_lag_1": "FLOAT",
    "net_flow_lag_2": "FLOAT",
    "net_flow_lag_24": "FLOAT",
    "net_flow_roll_3": "FLOAT",
    "net_flow_roll_24": "FLOAT",
    "num_bikes_taken_lag_1": "FLOAT",
    "num_bikes_dropped_lag_1": "FLOAT",

    # Calendar
    "jour_semaine": "INT",
    "is_holiday": "BOOLEAN",

    # Prediction metadata
    "request_datetime": "TIMESTAMP",
    "prediction": "FLOAT",
    "num_bikes_available": "INT",
    "num_docks_available": "INT",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
}

def get_neon_connection():
    return psycopg2.connect(NEON_CONN_STRING)

def create_table_sql():
    columns_sql = []

    for col, col_type in EXPECTED_COLUMNS.items():
        columns_sql.append(f"{col} {col_type}")

    return f"""
    CREATE TABLE IF NOT EXISTS prediction_logs (
        {", ".join(columns_sql)}
    );
    """

def ensure_prediction_table():
    print(NEON_CONN_STRING)
    try:
        with get_neon_connection() as conn:
            with conn.cursor() as cur:

                # Check existence
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'prediction_logs'
                    );
                """)
                exists = cur.fetchone()[0]

                if not exists:
                    print("📦 Creating table")
                    cur.execute(create_table_sql())
                    return

                # Get existing columns
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'prediction_logs'
                """)
                existing_cols = {row[0] for row in cur.fetchall()}

                expected_cols = set(EXPECTED_COLUMNS.keys())

                # ✅ Add missing columns
                missing_cols = expected_cols - existing_cols

                for col in missing_cols:
                    col_type = EXPECTED_COLUMNS[col]

                    # Skip id if already exists logic conflict
                    if col == "id":
                        continue

                    sql = f"ALTER TABLE prediction_logs ADD COLUMN IF NOT EXISTS {col} {col_type};"
                    print(f"➕ Adding column: {col}")
                    cur.execute(sql)

                # ⚠️ Detect extra columns (no deletion)
                extra_cols = existing_cols - expected_cols
                if extra_cols:
                    print(f"⚠️ Extra columns detected (not removed): {extra_cols}")

                if not missing_cols:
                    print("✅ Schema up to date")

    except Exception as e:
        print(f"❌ Schema migration failed: {e}")

def insert_prediction(station_id, X,request_datetime, prediction, availability_station ):
    
    data = {
        "station_id": station_id,
        "year": int(X["year"].iloc[0]),
        "month": int(X["month"].iloc[0]),
        "day": int(X["day"].iloc[0]),
        "hour": int(X["hour"].iloc[0]),
        "temp": float(X["temp"].iloc[0]),
        "relative_humidity": float(X["relative_humidity"].iloc[0]),
        "precipitation_total": float(X["precipitation_total"].iloc[0]),
        "average_wind_speed": float(X["average_wind_speed"].iloc[0]),
        "coco": int(X["coco"].iloc[0]),
        "coco_group":X["coco_group"].iloc[0],
        "net_flow_lag_1": float(X["net_flow_lag_1"].iloc[0]),
        "net_flow_lag_2": float(X["net_flow_lag_2"].iloc[0]),
        "net_flow_lag_24": float(X["net_flow_lag_24"].iloc[0]),
        "net_flow_roll_3": float(X["net_flow_roll_3"].iloc[0]),
        "net_flow_roll_24": float(X["net_flow_roll_24"].iloc[0]),
        "num_bikes_taken_lag_1": float(X["num_bikes_taken_lag_1"].iloc[0]),
        "num_bikes_dropped_lag_1": float(X["num_bikes_dropped_lag_1"].iloc[0]),
        "jour_semaine": int(X["jour_semaine"].iloc[0]),
        "is_holiday": bool(X["is_holiday"].iloc[0]),

        "request_datetime": request_datetime,
        "prediction": float(prediction),
        "num_bikes_available": availability_station["num_bikes_available"],
        "num_docks_available": availability_station["num_docks_available"]
    } 
    
    try:
        # Open connection
        with psycopg2.connect(NEON_CONN_STRING) as conn:
            with conn.cursor() as cur:
                # Convert numpy types to Python native
                clean_data = {k: (v.item() if hasattr(v, "item") else v) for k, v in data.items()}

                columns = ", ".join(clean_data.keys())
                placeholders = ", ".join(["%s"] * len(clean_data))
                
                sql = f"INSERT INTO prediction_logs ({columns}) VALUES ({placeholders})"
                cur.execute(sql, list(clean_data.values()))

        print("Prediction logged successfully.")
    except Exception as e:
        print("Failed to insert prediction:", e)