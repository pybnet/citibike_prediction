# 🚲 CitiBike Flow Prediction — Full Stack MLOps Project

![CI](https://github.com//pybnet/citibike_prediction/actions/workflows/test.yml/badge.svg)

> End-to-end MLOps pipeline predicting bike net flow at CitiBike stations in New York City — from data ingestion to automated retraining, monitoring, and real-time API inference.

---

## Table of Contents

- [Project Overview](#-project-overview)
- [Notebooks — Research & Exploration](#-notebooks--research--exploration)
- [Architecture](#-architecture)
- [Stack](#-stack)
- [Repository Structure](#-repository-structure)
- [Infrastructure — Two Docker Stacks](#-infrastructure--two-docker-stacks)
  - [Stack 1 · MLflow + FastAPI + Streamlit](#stack-1--mlflow--fastapi--streamlit-local_mlflow_fastapi)
  - [Stack 2 · Airflow + PostgreSQL](#stack-2--airflow--postgresql-local_airflow_postgres_server)
- [ML Pipeline](#-ml-pipeline)
- [Monitoring](#-monitoring)
- [CI — Automated Tests](#-ci--automated-tests)
- [Getting Started](#-getting-started)
- [Environment Variables](#-environment-variables)

---

## Project Overview

This project builds a **production-grade MLOps system** for predicting the net bike flow (bikes arriving minus bikes leaving) at CitiBike stations, allowing users to know whether a station will have bikes available or docks free at a given hour.

The system covers the full ML lifecycle:

- **Data ingestion** from CitiBike's real-time GBFS API and weather APIs, stored in PostgreSQL
- **Feature engineering** with lag features, rolling averages, weather, and calendar variables
- **Model training** with XGBoost inside a Docker container, orchestrated by Airflow
- **Experiment tracking** and model registry via MLflow backed by MinIO (S3-compatible storage)
- **Real-time inference** via a FastAPI REST API
- **User interface** via Streamlit — users select a station and hour, and choose whether they want to take or drop a bike
- **Automated monitoring** of data drift, concept drift, and model performance via Evidently AI
- **Automated retraining** triggered by Airflow when drift or performance degradation is detected

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        USER INTERFACE                               │
│                    Streamlit  :7860                                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ HTTP
┌───────────────────────────▼─────────────────────────────────────────┐
│                        INFERENCE LAYER                              │
│                     FastAPI  :8000                                   │
│         /forecast  ·  /stations  ·  /health                         │
└───────────┬───────────────────────────────────┬─────────────────────┘
            │ load model                        │ log inference run
┌───────────▼───────────┐           ┌───────────▼─────────────────────┐
│    MODEL REGISTRY     │           │      EXPERIMENT TRACKING        │
│    MLflow  :5001      │           │      MLflow  :5001              │
│    MinIO   :9000      │           │      PostgreSQL  :5432          │
└───────────────────────┘           └─────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION LAYER                            │
│                      Airflow  :8080                                 │
│                                                                     │
│  DAG: monitoring_model                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌─────────────────────────┐ │
│  │ Data drift   │   │ Concept drift│   │  Model performance      │ │
│  │ (Evidently)  │   │ (KS + t-test │   │  (RMSE vs threshold)    │ │
│  │              │   │  + Page-     │   │                         │ │
│  │              │   │  Hinkley)    │   │                         │ │
│  └──────┬───────┘   └──────┬───────┘   └───────────┬─────────────┘ │
│         └──────────────────┴───────────────────────┘               │
│                             │ drift or perf degradation             │
│                    ┌────────▼────────┐                              │
│                    │  Retrain model  │                              │
│                    │  (DockerOperator│                              │
│                    │   XGBoost)      │                              │
│                    └─────────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                  │
│   PostgreSQL :5433 (CitiBike data)  ·  AWS S3 (historical data)    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3 (CeleryExecutor) |
| ML Framework | XGBoost + scikit-learn Pipeline |
| Experiment Tracking | MLflow 2.21 |
| Artifact Storage | MinIO (S3-compatible) |
| Data Storage | PostgreSQL 16 |
| Inference API | FastAPI + Uvicorn |
| User Interface | Streamlit |
| Monitoring | Evidently AI (cloud) |
| Containerisation | Docker + Docker Compose |
| CI | GitHub Actions + pytest |
| Task Queue | Redis + Celery |
| Data Transform | dbt (dbt-postgres) |

---

## Repository Structure

```
.                                       ← repo root
├── 01_Data/
│   └── meteo_holidays_pierre.ipynb     ← Weather & public holidays data collection
│
├── 02_EDA/
│   ├── building_eda_file.ipynb         ← Build EDA dataset: compute net flow per station,
│   │                                      enrich with weather & holiday data
│   └── eda.ipynb                       ← Exploratory Data Analysis
│
├── 03_Model/
│   └── Preprocessing_Training_models.ipynb  ← First model: preprocessing + baseline training
│
└── 04_Deployement/
    ├── local_mlflow_fastapi/           ← Inference + tracking stack
    │   ├── api/
    │   │   └── main.py                 ← FastAPI app
    │   ├── streamlit_app.py            ← Streamlit UI
    │   ├── requirements.txt
    │   ├── Dockerfile
    │   ├── .env.example
    │   └── docker-compose.yaml         ← MLflow + FastAPI + Streamlit stack
    │
    ├── local_airflow_postgres_server/  ← Airflow + data stack
    │   ├── .github/
    │   │   └── workflows/
    │   │       └── test.yml            ← GitHub Actions CI
    │   ├── dags/
    │   │   └── monitoring_model.py     ← Monitoring & retraining DAG
    │   ├── data/
    │   │   └── citibike/
    │   │       ├── reference/          ← Reference dataset for drift
    │   │       └── data_drift/         ← Incoming data for drift detection
    │   ├── tests/
    │   │   └── test_retrain.py         ← 36 unit tests
    │   ├── pytest.ini
    │   ├── .env.example
    │   └── docker-compose.yml          ← Airflow stack
    │
    └── retrain_image/                  ← Docker image for model retraining
        ├── retrain.py                  ← XGBoost training script (refactored into functions)
        ├── requirements.txt
        └── Dockerfile
```

---

## Notebooks — Research & Exploration

The project follows a structured notebook workflow before production deployment.

### `01_Data/` — Data Collection

| Notebook | Description |
|---|---|
| `meteo_holidays_pierre.ipynb` | Fetches weather data (temperature, humidity, precipitation, wind speed, weather code) and public holidays for the NYC area. This data feeds the first version of the model as static enrichment before the automated pipeline was built. |

### `02_EDA/` — Exploratory Data Analysis

| Notebook | Description |
|---|---|
| `building_eda_file.ipynb` | Builds the analysis-ready dataset: computes the net bike flow per station per hour, joins weather data and holiday flags. This file is the input to the EDA notebook. |
| `eda.ipynb` | Full exploratory analysis — distribution of net flows per station, temporal patterns (hour of day, day of week, seasonality), weather correlations, top 20 station selection, and baseline statistics. |

### `03_Model/` — First Model

| Notebook | Description |
|---|---|
| `Preprocessing_Training_models.ipynb` | End-to-end first model: preprocessing pipeline (imputation, encoding), benchmark of multiple regressors (Linear, Ridge, Lasso, ElasticNet, Random Forest, Extra Trees, XGBoost), evaluation vs naive baseline, and hyperparameter tuning. This notebook is the prototype for the production `retrain.py` script. |

### `04_Deployement/` — Production

The production system is split into three components described in detail below.

---

## Infrastructure — Two Docker Stacks

The production deployment (`04_Deployement/`) is split into three components that communicate via `host.docker.internal`.

### Stack 1 · MLflow + FastAPI + Streamlit (`local_mlflow_fastapi`)

Handles **model serving and experiment tracking**.

```
docker-compose.yaml
```

| Service | Role | Port |
|---|---|---|
| `fastapi-app` | REST API — real-time bike flow predictions | `8000` |
| `streamlit` | User interface — station + hour selector | `7860` |
| `mlflow` | Experiment tracking + model registry | `5001` |
| `postgres` | MLflow backend store | `5432` |
| `minio` | Artifact store (S3-compatible) — stores model files | `9000` / `9001` |

**Start:**
```bash
cd local_mlflow_fastapi
docker compose up --build
```

**Access:**

| UI | URL |
|---|---|
| Streamlit app | http://localhost:7860 |
| FastAPI docs | http://localhost:8000/docs |
| MLflow UI | http://localhost:5001 |
| MinIO console | http://localhost:9001 |

**How it works:**

1. On startup, FastAPI loads the latest model version aliased `staging` from the MLflow model registry
2. The model artifact is stored in MinIO and retrieved via the MLflow client
3. Each `/forecast` call logs an inference run to MLflow (params, metrics, model version tag)
4. Streamlit displays the prediction and advises the user whether to take or drop a bike based on predicted net flow and current availability

---

### Stack 2 · Airflow + PostgreSQL (`local_airflow_postgres_server`)

Handles **orchestration, monitoring, and automated retraining**.

```
docker-compose.yml
```

| Service | Role | Port |
|---|---|---|
| `airflow-apiserver` | Airflow UI + REST API | `8080` |
| `airflow-scheduler` | DAG scheduling | — |
| `airflow-worker` | Task execution (Celery) | — |
| `airflow-dag-processor` | DAG parsing | — |
| `airflow-triggerer` | Deferred task execution | — |
| `postgres` | Airflow metadata database | — |
| `postgres_data` | CitiBike operational data | `5433` |
| `redis` | Celery message broker | `6379` |
| `dbt` | Data transformation | — |

**Start:**
```bash
cd local_airflow_postgres_server
echo -e "AIRFLOW_UID=$(id -u)" > .env   # Linux only
docker compose up --build
```

**Access:**

| UI | URL | Default credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |

**Monitoring DAG — `monitoring_model`** (runs monthly on the 10th at 14:00):

```
start
  ├── drift_pipeline (TaskGroup)
  │     detect_file → detect_data_drift → branch
  │                                         ├── data_drift_detected   (→ Evidently Cloud)
  │                                         └── no_data_drift_detected
  ├── monitor_model          (RMSE vs threshold → MLflow)
  └── detect_concept_drift   (KS test + Welch t-test + Page-Hinkley)
          ↓
      branch_retrain
          ├── export_data_to_retrain → train_model (DockerOperator) → clean_file
          └── skip_retrain_task → clean_file
                ↓
              end
```

**Retraining is triggered automatically if any of:**
- Data drift detected (Evidently `DataDriftPreset`)
- Concept drift detected (2/3 statistical tests: KS, Welch t-test, Page-Hinkley)
- Model RMSE exceeds threshold (default: 15)

---

### Component 3 · Retrain Image (`retrain_image/`)

The Docker image used by Airflow's `DockerOperator` to retrain the model.

| File | Role |
|---|---|
| `retrain.py` | Full training script refactored into importable functions |
| `Dockerfile` | Builds the training image (`pybnet/citibikeproject:v1.x`) |
| `requirements.txt` | Pinned dependencies for reproducible training |

**Build & push:**
```bash
cd 04_Deployement/retrain_image
docker build -t pybnet/citibikeproject:v1.4 .
docker push pybnet/citibikeproject:v1.4
```

Airflow pulls this image automatically when retraining is triggered — no manual intervention needed.

---

## ML Pipeline

**Features (20):** station ID, temporal (year/month/day/hour/weekday), weather (temp, humidity, precipitation, wind), lag features (net flow t-1, t-2, t-24), rolling averages (3h, 24h), weather code group, holiday flag.

**Target:** `net_flow` = bikes dropped − bikes taken at a station per hour.

**Training pipeline:**
```
load_data → split_data (80/20 time-based) → build_preprocessor
  → benchmark_models → tune_xgboost (RandomizedSearchCV + TimeSeriesSplit)
    → train_final_pipeline → log_and_register_model → promote_to_staging
```

**Model:** XGBoost regressor inside a scikit-learn `Pipeline` (median imputation + OHE for categoricals).

**Registered in MLflow** with signature, hyperparameters, and aliased as `staging` on every successful training run.

---

## Monitoring

Three complementary monitoring signals run in parallel every month:

| Signal | Method | Tool |
|---|---|---|
| Data drift | Distribution shift on input features | Evidently `DataDriftPreset` |
| Concept drift | KS test + Welch t-test + Page-Hinkley on residuals | scipy + custom |
| Model performance | RMSE vs fixed threshold on last month's actuals | MLflow metrics |

Concept drift uses a **majority vote (≥ 2/3 tests)** to reduce false positives. All results are logged to MLflow under `citibike_netflow_model_monitoring`.

---

## CI — Automated Tests

36 unit tests covering data loading, preprocessing, scoring, pipeline training, MLflow helpers, and model persistence. MLflow calls are fully mocked — no server required to run tests.

```bash
pytest tests/test_retrain.py -v
```

GitHub Actions runs on every push to `main`/`develop` and every pull request.

---

## Getting Started

**Prerequisites:** Docker Desktop (≥ 4GB RAM allocated), Docker Compose v2

**1. Clone the repo**
```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

**2. Start the MLflow + inference stack first**
```bash
cd local_mlflow_fastapi
cp .env.example .env          # fill in credentials
docker compose up --build -d
```

**3. Start the Airflow + data stack**
```bash
cd ../local_airflow_postgres_server
cp .env.example .env          # fill in credentials
docker compose up --build -d
```

**4. Open the app**

Go to http://localhost:7860, select a station and an hour, choose whether you want to take or drop a bike, and click **Predict**.

---

## Environment Variables

### `local_mlflow_fastapi/.env`

| Variable | Description |
|---|---|
| `MLFLOW_TRACKING_URI` | MLflow server URL (default: `http://mlflow:5000`) |
| `AWS_ACCESS_KEY_ID` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | MinIO secret key |
| `MLFLOW_S3_ENDPOINT_URL` | MinIO endpoint |

### `local_airflow_postgres_server/.env`

| Variable | Description |
|---|---|
| `AIRFLOW_UID` | Host user ID (Linux: `$(id -u)`) |
| `RAPIDAPI_KEY` | Weather API key |
| `EVIDENTLY_CLOUD_TOKEN` | Evidently Cloud token |
| `EVIDENTLY_CLOUD_PROJECT_ID` | Evidently Cloud project ID |
| `POSTGRES_AIRFLOW_USER/PASSWORD/DB` | Airflow metadata DB credentials |
| `POSTGRES_DATA_USER/PASSWORD/DB` | CitiBike operational DB credentials |

*Built with Python 3.10 · XGBoost · Apache Airflow 3 · MLflow · FastAPI · Streamlit · Docker*