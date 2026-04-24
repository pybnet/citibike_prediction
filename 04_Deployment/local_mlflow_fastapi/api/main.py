from fastapi import FastAPI
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError
import requests
import time
import os
import joblib
import pandas as pd
from pathlib import Path
from datetime import datetime
import mlflow
from mlflow.tracking import MlflowClient
import mlflow.xgboost
from mlflow.models.signature import infer_signature
from .citibike import download_station_information, get_station_id_and_short_name, load_station_status_df, get_station_availability
from .predict import predict_from_user_date
from .weather import station_weather_data

description = """
    Permettre à l’utilisateur de connaître la disponibilité d’un vélo Citi Bike dans une station proche de lui dans un temps donné.
"""
tags_metadata = [
    {
        "name": "health check",
        "description": "Renvoi 'status: API is running' si le server fonctionne correctement"
    },

    {
        "name": "List of all station names",
        "description": "Renvoies le noms de toutes les stations"
    },

    {
        "name": "Forecast",
        "description": (
            "Prédit, pour une station donnée, l’évolution du **flux net de vélos** "
            "sur les prochaines heures.\n\n"
            "**Paramètres requis :**\n"
            "- `station_name` : identifiant court de la station (ex. `6140.05`)\n"
            "- `horizon_hours` : nombre d’heures à prédire (ex. `5`)\n"
            "}\n"
            "```"
            )
        }
    ]
app = FastAPI(
    title=" Citibike Prediction API",
    description=description,
    version="0.1",
    contact={
        "name": "Team",
    },
    openapi_tags=tags_metadata
)

# ---- Request schema ----
class FutureFeatures(BaseModel):
    hour_selected: int
    station_selected: str    

def wait_for_mlflow():
    url = os.environ.get("MLFLOW_TRACKING_URI")
    for _ in range(20):
        try:
            requests.get(f"{url}/api/2.0/mlflow/experiments/list")
            print("MLflow is ready!")
            return
        except requests.exceptions.RequestException:
            print("Waiting for MLflow...")
            time.sleep(3)
    raise RuntimeError("MLflow did not become ready in time") 

def ensure_mlflow_bucket():
    bucket_name = "mlflow"
    endpoint_url = os.environ.get("MLFLOW_S3_ENDPOINT_URL")
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    try:
        # Check if bucket exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError:
        # Bucket does not exist, create it
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    
@app.on_event("startup")
def startup_event():
    # Load model
    #print("Loading model...")
    #MODEL_PATH = Path("model/citibike_xgb_pipeline.joblib")
    #app.model = joblib.load(MODEL_PATH)
        
    # Load station list
    print("Loading top20_stations list...")
    STATION_PATH = Path("data/top20_station_list.csv")
    top20_stations = pd.read_csv(STATION_PATH, header=None, names=["name"])

    # Fix station names: cast to string first, then add trailing 0 if needed
    def fix_station_name(s):
        if '.' in s:
            integer, decimal = s.split('.')
            if len(decimal) == 1:
                decimal += '0'
            return f"{integer}.{decimal}"
        return s

    top20_stations['name'] = top20_stations['name'].astype(str).str.strip().apply(fix_station_name)
    app.top20_stations = top20_stations
    print("Top 20 stations loaded")
    
    # Load dataset
    print("Loading preprocessed dataset...")
    DATASET_PATH = Path("data/historical_data.parquet")
    app.dataset_df = pd.read_parquet(DATASET_PATH, engine="pyarrow")
    
    ensure_mlflow_bucket()
    wait_for_mlflow()
    
    # ---- MLflow setup ----
    mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment("citibike_forecast")

    app.mlflow_client = MlflowClient()

    MODEL_NAME = "citibike_forecast_model"
    MODEL_PATH = Path("model/citibike_forecast_model.joblib")
    app.model_version = "local_fallback"

    try:
        # FIX: get_latest_versions() is deprecated in MLflow 2.x.
        #      Use search_model_versions() with ORDER BY instead.
        try:
            versions = app.mlflow_client.search_model_versions(
                filter_string=f"name='{MODEL_NAME}'",
                order_by=["version_number DESC"],
                max_results=1,
            )
        except mlflow.exceptions.MlflowException:
            versions = []

        if versions:
            latest_mv = versions[0]
            app.model_version = latest_mv.version
            print(f"Loading latest MLflow model version {app.model_version}...")
            try:
                # FIX: load by alias "staging" if set, or by version number directly.
                #      Stage-based URIs (Staging/Production) are deprecated in MLflow 2.x.
                try:
                    app.model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@staging")
                except mlflow.exceptions.MlflowException:
                    # fallback: load by version number if alias not set
                    app.model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{app.model_version}")
            except Exception as e_load:
                print(f"Failed to load MLflow model: {e_load}")
                raise

        else:
            # No model in MLflow: load local joblib and register it
            print(f"No MLflow model found. Loading local fallback from {MODEL_PATH}...")
            app.model = joblib.load(MODEL_PATH)

            sample_input = app.dataset_df[
                app.dataset_df["station_id"].isin(app.top20_stations["name"])
            ].iloc[:1]
            # FIX: cast int cols to float64 to avoid schema enforcement warning
            sample_input = sample_input.astype(
                {col: "float64" for col in sample_input.select_dtypes("int").columns}
            )
            signature = infer_signature(sample_input, app.model.predict(sample_input))

            with mlflow.start_run(run_name="register_local_model") as run:
                mlflow.sklearn.log_model(
                    sk_model=app.model,
                    artifact_path="model",   # FIX: use artifact_path, not name= (requires MLflow 3.x server)
                    signature=signature,
                    registered_model_name=MODEL_NAME,  # FIX: register inline instead of separate mlflow.register_model()
                    tags={
                        "author": "team_name",
                        "description": "Citibike bike flow prediction model"
                    }
                )

            # Retrieve the version that was just registered
            versions = app.mlflow_client.search_model_versions(
                filter_string=f"name='{MODEL_NAME}'",
                order_by=["version_number DESC"],
                max_results=1,
            )
            app.model_version = versions[0].version if versions else "local_fallback"

            # Alias it as staging
            if versions:
                app.mlflow_client.set_registered_model_alias(
                    name=MODEL_NAME,
                    alias="staging",
                    version=app.model_version,
                )

            print(f"Local model registered in MLflow as version {app.model_version}")

    except Exception as e_outer:
        print(f"Unexpected error loading model: {e_outer}")
        print("Falling back to local joblib model anyway...")
        app.model = joblib.load(MODEL_PATH)
        app.model_version = "local_fallback"

    print("Setup completed")

# ----- Endpoint health check" -----
@app.get("/", tags=["health check"])
def healthcheck():
    return {"status": "API is running"}


# ---- Endpoint: list all station names ----
@app.get("/stations", tags=["List of all station names"])
async def list_stations():
    """
    Returns a list of all station names available in the dataset.
    """
    return {"station_names": app.top20_stations["name"].tolist()}


# ---- Endpoint: forcast ----

@app.post("/forecast", tags=["Forcast"])
def forecast_station(req: FutureFeatures):
    # --- Station & weather data ---
    df_stations = download_station_information()
    result = get_station_id_and_short_name(df_stations, req.station_selected)
    station_full_id = result['station_id']
    station_short_id = result['short_name']
    print(f"Station selected: {req.station_selected} (full ID: {station_full_id}, short ID: {station_short_id})")
    weather_df = station_weather_data()
    df = load_station_status_df()
    availability = get_station_availability(df, station_full_id)
    availability_clean = {
            "num_bikes_available": int(availability["num_bikes_available"]),
            "num_docks_available": int(availability["num_docks_available"])
        }


    # --- Prepare prediction ---
    request_datetime = datetime.today().replace(hour=req.hour_selected, minute=0, second=0, microsecond=0)
    update_columns = ['temp', 'relative_humidity', 'precipitation_total', 'average_wind_speed', 'coco', 'coco_group', 'is_holiday'] 
    
    dataset = app.dataset_df[app.dataset_df["station_id"] == station_short_id]
    if dataset.empty:
        raise ValueError(f"❌ station_short_id '{station_short_id}' not present in dataset")

    print(f"✅ Dataset for station {station_short_id} loaded with shape {dataset.shape}")
    MODEL_NAME = "citibike_forecast_model"
    
    # --- Run prediction before opening the MLflow run ---
    pred, X = predict_from_user_date(
        dataset=dataset,
        request_datetime=request_datetime,
        model=app.model,
        station_id=station_short_id,
        weather_df=weather_df,
        update_columns=update_columns
    )
 
    # --- MLflow tracking ---
    with mlflow.start_run(
        run_name=f"inference_{station_short_id}_{request_datetime.strftime('%Y%m%d_%H')}",
        experiment_id=mlflow.get_experiment_by_name("citibike_forecast").experiment_id,
        nested=True,
    ):
        mlflow.log_params({
            "model_name": MODEL_NAME,
            "model_version": app.model_version,
            "station_selected": req.station_selected,
            "request_datetime": request_datetime.isoformat(),
            "hour_selected": req.hour_selected,
            "availability_bikes": availability_clean["num_bikes_available"],
            "availability_docks": availability_clean["num_docks_available"],
        })
        mlflow.log_metric("predicted_bike_flow", float(pred))
        mlflow.log_dict(X.iloc[-1].to_dict(), "input_features.json")
        mlflow.set_tag("model_version", app.model_version)
 
        # --- Logged models tab ---
        # log_model() inside an inference run registers the model artifact
        # under the "Logged models" tab of this run in the MLflow UI.
        # We use the registered model URI so no re-upload happens —
        # it references the already-stored artifact via its source run.
        try:
            registered_mv = app.mlflow_client.get_model_version(MODEL_NAME, app.model_version)
            source_run_id = registered_mv.run_id
 
            # This call creates the "Logged models" entry for this inference run
            mlflow.sklearn.log_model(
                sk_model=app.model,
                artifact_path="model",
                registered_model_name=None,  # don't create a new registered version
            )
 
            # --- Registered models tab ---
            # Link this run to the existing registered model version
            # so it appears under "Registered models" in the experiment UI.
            app.mlflow_client.set_model_version_tag(
                name=MODEL_NAME,
                version=app.model_version,
                key="last_inference_run_id",
                value=mlflow.active_run().info.run_id,
            )
            app.mlflow_client.set_model_version_tag(
                name=MODEL_NAME,
                version=app.model_version,
                key="last_inference_station",
                value=req.station_selected,
            )
            app.mlflow_client.set_model_version_tag(
                name=MODEL_NAME,
                version=app.model_version,
                key="last_inference_datetime",
                value=request_datetime.isoformat(),
            )
        except Exception as e:
            print(f"Could not log model details to MLflow UI: {e}")
   
    return  {
        "station": req.station_selected,
        "datetime": request_datetime.isoformat(),
        "predicted_net_flow": round(float(pred), 1),
        "num_bikes_available": availability_clean["num_bikes_available"],
        "num_docks_available": availability_clean["num_docks_available"],
    }