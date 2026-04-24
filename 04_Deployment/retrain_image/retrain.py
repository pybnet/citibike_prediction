import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
import gc
import logging
import os
import joblib
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient
from scipy.sparse import csr_matrix

# logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
FEATURES = [
    "station_id", "year", "month", "day", "hour",
    "temp", "precipitation_total", "relative_humidity", "average_wind_speed",
    "num_bikes_taken_lag_1", "num_bikes_dropped_lag_1",
    "net_flow_lag_1", "net_flow_lag_2", "net_flow_lag_24",
    "net_flow_roll_3", "net_flow_roll_24",
    "jour_semaine", "coco_group", "is_holiday", "coco",
]
TARGET       = "net_flow"
DATA_PATH    = "/app/data/historical_data.parquet"
EXPERIMENT_NAME  = "Citibike_forecast_training"
MODEL_NAME       = "citibike_forecast_model"
LOCAL_MODEL_PATH = Path("model/citibike_forecast_model.joblib")


# =============================================================================
# 1. Data
# =============================================================================

def load_data(data_path: str = DATA_PATH) -> pd.DataFrame:
    """Load parquet and downcast numeric types to reduce memory."""
    logger.info("Importing data...")
    dataset = pd.read_parquet(data_path, columns=FEATURES + [TARGET])
    for col in dataset.select_dtypes(include=["float64"]).columns:
        dataset[col] = dataset[col].astype("float32")
    for col in dataset.select_dtypes(include=["int64"]).columns:
        dataset[col] = dataset[col].astype("int32")
    return dataset


def split_data(dataset: pd.DataFrame, ratio: float = 0.8):
    """Time-based train/test split. Returns X_train, y_train, X_test, y_test."""
    logger.info("Splitting data...")
    cut = int(len(dataset) * ratio)
    train_df = dataset.iloc[:cut].copy()
    test_df  = dataset.iloc[cut:].copy()
    X_train, y_train = train_df[FEATURES], train_df[TARGET]
    X_test,  y_test  = test_df[FEATURES],  test_df[TARGET]
    logger.info(f"Train shape: {X_train.shape}, Test shape: {X_test.shape}")
    return X_train, y_train, X_test, y_test


# =============================================================================
# 2. Metrics
# =============================================================================

def score(y_true, y_pred) -> dict:
    """Return RMSE, MAE and R² as a dict."""
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae":  float(mean_absolute_error(y_true, y_pred)),
        "r2":   float(r2_score(y_true, y_pred)),
    }


def naive_baseline(y_test: pd.Series) -> dict:
    """Naive baseline: predict the last observed value (net_flow itself)."""
    metrics = score(y_test, y_test.to_numpy())
    logger.info("=== Naive baseline on TEST ===")
    logger.info(f"RMSE: {metrics['rmse']:.4f} | MAE: {metrics['mae']:.4f} | R2: {metrics['r2']:.4f}")
    return metrics


# =============================================================================
# 3. Preprocessing
# =============================================================================

def build_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    """Build (unfitted) ColumnTransformer for numeric + categorical columns."""
    cat_cols = [c for c in X.columns if X[c].dtype == "object" or str(X[c].dtype) == "category"]
    num_cols = [c for c in X.columns if c not in cat_cols]

    numeric_preprocess = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
    ])
    categorical_preprocess = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe",     OneHotEncoder(handle_unknown="ignore", sparse_output=True)),
    ])
    return ColumnTransformer(
        transformers=[
            ("num", numeric_preprocess, num_cols),
            ("cat", categorical_preprocess, cat_cols),
        ],
        remainder="drop"
    )


def preprocess_data(preprocessor: ColumnTransformer, X_train, X_test):
    """Fit on train, transform both splits, return CSR matrices."""
    logger.info("Starting preprocessing...")
    X_train_prep = csr_matrix(preprocessor.fit_transform(X_train))
    X_test_prep  = csr_matrix(preprocessor.transform(X_test))
    return X_train_prep, X_test_prep


# =============================================================================
# 4. Benchmarking
# =============================================================================

def benchmark_models(models: list, preprocessor, X_train, y_train, X_test, y_test) -> pd.DataFrame:
    """Fit each model on preprocessed data and return a results DataFrame."""
    X_train_prep, X_test_prep = preprocess_data(preprocessor, X_train, X_test)
    naive = score(y_test, y_test.to_numpy())
    results = []

    for name, model in models:
        logger.info(f"Training {name}...")
        model.fit(X_train_prep, y_train)
        pred = model.predict(X_test_prep)
        m = score(y_test, pred)
        results.append({
            "model": name,
            **m,
            "rmse_gain_vs_naive": naive["rmse"] - m["rmse"],
        })

    res = pd.DataFrame(results).sort_values("rmse")
    logger.info("=== Benchmark on TEST ===")
    logger.info(res.to_string(index=False))
    return res


# =============================================================================
# 5. Hyperparameter tuning
# =============================================================================

def tune_xgboost(
    preprocessor: ColumnTransformer,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    param_dist: dict,
    n_iter: int = 30,
    n_splits: int = 3,
) -> RandomizedSearchCV:
    """Run RandomizedSearchCV with TimeSeriesSplit and return the fitted search object."""
    logger.info("Starting hyperparameter tuning xgboost...")
    xgb_pipe = Pipeline([
        ("prep",  preprocessor),
        ("model", XGBRegressor(objective="reg:squarederror", random_state=42, n_jobs=1, verbosity=1)),
    ])
    tscv = TimeSeriesSplit(n_splits=n_splits)
    search = RandomizedSearchCV(
        estimator=xgb_pipe,
        param_distributions=param_dist,
        n_iter=n_iter,
        scoring="neg_root_mean_squared_error",
        cv=tscv,
        verbose=2,
        n_jobs=1,
        random_state=42,
    )
    search.fit(X_train, y_train)
    logger.info(f"Best params: {search.best_params_}")
    logger.info(f"Best CV RMSE: {-search.best_score_:.4f}")

    pred_best = search.best_estimator_.predict(X_test)
    m = score(y_test, pred_best)
    logger.info("=== Best XGB (tuned) on TEST ===")
    logger.info(f"RMSE: {m['rmse']:.4f} | MAE: {m['mae']:.4f} | R2: {m['r2']:.4f}")
    return search


# =============================================================================
# 6. Final pipeline
# =============================================================================

def build_final_pipeline(preprocessor: ColumnTransformer, best_params: dict) -> Pipeline:
    """Assemble the final Pipeline from tuned hyperparameters."""
    return Pipeline([
        ("prep", preprocessor),
        ("model", XGBRegressor(
            objective="reg:squarederror",
            n_estimators=best_params["model__n_estimators"],
            learning_rate=best_params["model__learning_rate"],
            max_depth=best_params["model__max_depth"],
            subsample=best_params["model__subsample"],
            colsample_bytree=best_params["model__colsample_bytree"],
            reg_alpha=best_params["model__reg_alpha"],
            min_child_weight=best_params["model__min_child_weight"],
            reg_lambda=best_params["model__reg_lambda"],
            random_state=42,
            n_jobs=1,
        )),
    ])


def train_final_pipeline(
    preprocessor: ColumnTransformer,
    best_params: dict,
    X_train: pd.DataFrame,
    y_train: pd.Series,
) -> Pipeline:
    """Build and fit the final pipeline on the full training set."""
    logger.info("Training final pipeline...")
    pipe = build_final_pipeline(preprocessor, best_params)
    pipe.fit(X_train, y_train)
    return pipe


# =============================================================================
# 7. MLflow logging & registration
# =============================================================================

def setup_mlflow(tracking_uri: str, experiment_name: str) -> str:
    """Set tracking URI, ensure experiment exists, return experiment_id."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        experiment_id = client.create_experiment(experiment_name)
        logger.info(f"✅ Created experiment '{experiment_name}'")
    else:
        experiment_id = experiment.experiment_id
        logger.info(f"✅ Using existing experiment '{experiment_name}'")
    return experiment_id


def ensure_registered_model(client: MlflowClient, model_name: str) -> None:
    """Create the registered model entry if it doesn't exist yet."""
    try:
        client.get_registered_model(model_name)
        logger.info(f"✅ Registered model '{model_name}' already exists.")
    except mlflow.exceptions.MlflowException:
        client.create_registered_model(model_name)
        logger.info(f"✅ Created new registered model '{model_name}'.")


def log_and_register_model(
    final_pipe: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    experiment_id: str,
    model_name: str,
) -> tuple:
    """
    Start an MLflow run, log metrics + params + model artifact,
    register the model, and return (run_id, model_info, metrics).
    """
    client = MlflowClient()

    with mlflow.start_run(run_name="machine_learning_training", experiment_id=experiment_id) as run:
        run_id = run.info.run_id

        pred = final_pipe.predict(X_test)
        m = score(y_test, pred)

        mlflow.log_metric("rmse", m["rmse"])
        mlflow.log_metric("mae",  m["mae"])
        mlflow.log_metric("r2",   m["r2"])
        mlflow.log_params(final_pipe.named_steps["model"].get_params())

        X_sig = X_test.astype({col: "float64" for col in X_test.select_dtypes("int").columns})
        signature  = infer_signature(X_sig, pred)
        model_info = mlflow.sklearn.log_model(
            sk_model=final_pipe,
            artifact_path="model",
            signature=signature,
            registered_model_name=model_name,
        )

        logger.info(f"✅ Run ID: {run_id}")
        logger.info(f"   RMSE={m['rmse']:.4f}  MAE={m['mae']:.4f}  R²={m['r2']:.4f}")

    return run_id, model_info, m


def promote_to_staging(client: MlflowClient, model_name: str) -> str:
    """Retrieve the latest registered version and alias it as 'staging'."""
    versions = client.search_model_versions(
        filter_string=f"name='{model_name}'",
        order_by=["version_number DESC"],
        max_results=1,
    )
    if not versions:
        raise RuntimeError(f"No versions found for registered model '{model_name}'.")

    version = versions[0].version
    client.set_registered_model_alias(name=model_name, alias="staging", version=version)
    logger.info(f"✅ Model version {version} aliased as 'staging'")
    return version


def save_model_locally(final_pipe: Pipeline, path: Path) -> None:
    """Persist the pipeline to disk with joblib."""
    path.parent.mkdir(exist_ok=True)
    joblib.dump(final_pipe, path)
    logger.info(f"✅ Local model saved at {path}")


# =============================================================================
# 8. Entrypoint
# =============================================================================

def main():
    # --- Data ---
    dataset = load_data(DATA_PATH)
    X_train, y_train, X_test, y_test = split_data(dataset)
    naive_baseline(y_test)

    del dataset
    gc.collect()

    # --- Preprocessing ---
    preprocessor = build_preprocessor(X_train)

    # --- Benchmark ---
    models = [
        ("XGBoost", XGBRegressor(
            n_estimators=400, learning_rate=0.05, max_depth=5,
            subsample=0.8, colsample_bytree=0.8,
            reg_alpha=0.0, reg_lambda=1.0,
            random_state=42, n_jobs=1, verbosity=1,
        )),
    ]
    benchmark_models(models, build_preprocessor(X_train), X_train, y_train, X_test, y_test)

    # --- Tuning ---
    param_dist = {
        "model__n_estimators":    [800],
        "model__max_depth":       [6],
        "model__learning_rate":   [0.01],
        "model__subsample":       [1.0],
        "model__colsample_bytree":[0.6],
        "model__min_child_weight":[10],
        "model__reg_alpha":       [0.0],
        "model__reg_lambda":      [5.0],
    }
    search = tune_xgboost(preprocessor, X_train, y_train, X_test, y_test, param_dist)

    # --- Final pipeline ---
    final_pipe = train_final_pipeline(preprocessor, search.best_params_, X_train, y_train)
    save_model_locally(final_pipe, LOCAL_MODEL_PATH)

    # --- MLflow ---
    logger.info("Saving to mlflow...")
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    experiment_id = setup_mlflow(tracking_uri, EXPERIMENT_NAME)

    client = MlflowClient()
    ensure_registered_model(client, MODEL_NAME)

    run_id, model_info, metrics = log_and_register_model(
        final_pipe, X_test, y_test, experiment_id, MODEL_NAME
    )
    version = promote_to_staging(client, MODEL_NAME)

    logger.info(f"Input schema:  {model_info.signature.inputs}")
    logger.info(f"Output schema: {model_info.signature.outputs}")
    logger.info("✅ Training complete.")


if __name__ == "__main__":
    main()