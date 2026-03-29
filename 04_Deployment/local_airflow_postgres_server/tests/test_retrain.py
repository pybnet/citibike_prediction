"""
tests/test_retrain.py

Unit & integration tests for retrain.py.
Run with:  pytest tests/test_retrain.py -v

Dependencies (add to requirements-dev.txt):
    pytest
    pytest-mock
"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from scipy.sparse import issparse

# Import directly from refactored retrain.py
# retrain.py no longer runs on import — safe to import functions
from retrain_image.retrain import (
    FEATURES,
    TARGET,
    load_data,
    split_data,
    score,
    naive_baseline,
    build_preprocessor,
    preprocess_data,
    benchmark_models,
    tune_xgboost,
    build_final_pipeline,
    train_final_pipeline,
    setup_mlflow,
    ensure_registered_model,
    log_and_register_model,
    promote_to_staging,
    save_model_locally,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_dataset():
    """Small synthetic dataset matching retrain.py column schema."""
    np.random.seed(42)
    n = 200
    return pd.DataFrame({
        "station_id":              np.random.choice(["A", "B", "C"], n),
        "year":                    np.random.randint(2022, 2024, n).astype("int32"),
        "month":                   np.random.randint(1, 13, n).astype("int32"),
        "day":                     np.random.randint(1, 29, n).astype("int32"),
        "hour":                    np.random.randint(0, 24, n).astype("int32"),
        "temp":                    np.random.uniform(0, 35, n).astype("float32"),
        "precipitation_total":     np.random.uniform(0, 10, n).astype("float32"),
        "relative_humidity":       np.random.uniform(20, 100, n).astype("float32"),
        "average_wind_speed":      np.random.uniform(0, 20, n).astype("float32"),
        "num_bikes_taken_lag_1":   np.random.randint(0, 20, n).astype("float64"),
        "num_bikes_dropped_lag_1": np.random.randint(0, 20, n).astype("float64"),
        "net_flow_lag_1":          np.random.uniform(-10, 10, n).astype("float64"),
        "net_flow_lag_2":          np.random.uniform(-10, 10, n).astype("float64"),
        "net_flow_lag_24":         np.random.uniform(-10, 10, n).astype("float64"),
        "net_flow_roll_3":         np.random.uniform(-10, 10, n).astype("float64"),
        "net_flow_roll_24":        np.random.uniform(-10, 10, n).astype("float64"),
        "jour_semaine":            np.random.choice(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], n),
        "coco_group":              np.random.choice(["clear","rain","snow"], n),
        "is_holiday":              np.random.choice([True, False], n),
        "coco":                    np.random.randint(1, 10, n).astype("int32"),
        "net_flow":                np.random.randint(-15, 15, n).astype("int32"),
    })


@pytest.fixture
def X_y(sample_dataset):
    X_train, y_train, X_test, y_test = split_data(sample_dataset)
    return X_train, y_train, X_test, y_test


@pytest.fixture
def preprocessor(X_y):
    X_train, _, _, _ = X_y
    return build_preprocessor(X_train)


@pytest.fixture
def fitted_pipeline(X_y, preprocessor):
    X_train, y_train, _, _ = X_y
    from xgboost import XGBRegressor
    best_params = {
        "model__n_estimators":     10,   # small for test speed
        "model__max_depth":        3,
        "model__learning_rate":    0.1,
        "model__subsample":        1.0,
        "model__colsample_bytree": 1.0,
        "model__reg_alpha":        0.0,
        "model__min_child_weight": 1,
        "model__reg_lambda":       1.0,
    }
    return train_final_pipeline(preprocessor, best_params, X_train, y_train)


# =============================================================================
# 1. load_data
# =============================================================================

class TestLoadData:

    def test_loads_correct_columns(self, tmp_path, sample_dataset):
        path = tmp_path / "data.parquet"
        sample_dataset.to_parquet(path)
        df = load_data(str(path))
        assert set(FEATURES + [TARGET]).issubset(df.columns)

    def test_float64_downcast_to_float32(self, tmp_path, sample_dataset):
        path = tmp_path / "data.parquet"
        sample_dataset.to_parquet(path)
        df = load_data(str(path))
        float64_cols = df.select_dtypes(include=["float64"]).columns.tolist()
        assert float64_cols == [], f"float64 columns remain: {float64_cols}"

    def test_int64_downcast_to_int32(self, tmp_path, sample_dataset):
        path = tmp_path / "data.parquet"
        sample_dataset.to_parquet(path)
        df = load_data(str(path))
        int64_cols = df.select_dtypes(include=["int64"]).columns.tolist()
        assert int64_cols == [], f"int64 columns remain: {int64_cols}"


# =============================================================================
# 2. split_data
# =============================================================================

class TestSplitData:

    def test_split_ratio(self, sample_dataset):
        X_train, _, X_test, _ = split_data(sample_dataset)
        assert len(X_train) == 160
        assert len(X_test)  == 40

    def test_no_overlap(self, sample_dataset):
        X_train, _, X_test, _ = split_data(sample_dataset)
        assert max(X_train.index) < min(X_test.index)

    def test_total_rows_preserved(self, sample_dataset):
        X_train, y_train, X_test, y_test = split_data(sample_dataset)
        assert len(X_train) + len(X_test) == len(sample_dataset)

    def test_features_only_in_X(self, sample_dataset):
        X_train, _, _, _ = split_data(sample_dataset)
        assert TARGET not in X_train.columns
        assert set(FEATURES).issubset(X_train.columns)

    def test_custom_ratio(self, sample_dataset):
        X_train, _, X_test, _ = split_data(sample_dataset, ratio=0.9)
        assert len(X_train) == 180
        assert len(X_test)  == 20


# =============================================================================
# 3. score
# =============================================================================

class TestScore:

    def test_perfect_predictions(self):
        y = np.array([1.0, 2.0, 3.0])
        m = score(y, y)
        assert m["rmse"] == 0.0
        assert m["mae"]  == 0.0
        assert m["r2"]   == 1.0

    def test_returns_floats(self):
        m = score(np.array([1, 2, 3]), np.array([1.1, 1.9, 3.2]))
        assert all(isinstance(v, float) for v in m.values())

    def test_returns_all_keys(self):
        m = score(np.array([1, 2, 3]), np.array([1, 2, 3]))
        assert set(m.keys()) == {"rmse", "mae", "r2"}

    def test_rmse_gte_mae(self):
        y_true = np.array([0, 0, 0, 0, 10])
        y_pred = np.array([0, 0, 0, 0,  0])
        m = score(y_true, y_pred)
        assert m["rmse"] >= m["mae"]

    def test_negative_r2_for_bad_model(self):
        m = score(np.array([1,2,3,4,5]), np.array([5,4,3,2,1]))
        assert m["r2"] < 0

    def test_mean_prediction_gives_zero_r2(self):
        y = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        m = score(y, np.full_like(y, y.mean()))
        assert abs(m["r2"]) < 1e-10


# =============================================================================
# 4. naive_baseline
# =============================================================================

class TestNaiveBaseline:

    def test_returns_dict_with_metrics(self, X_y):
        _, _, _, y_test = X_y
        m = naive_baseline(y_test)
        assert set(m.keys()) == {"rmse", "mae", "r2"}

    def test_r2_is_one(self, X_y):
        """Comparing y_test to itself should give R²=1."""
        _, _, _, y_test = X_y
        m = naive_baseline(y_test)
        assert m["r2"] == pytest.approx(1.0)


# =============================================================================
# 5. build_preprocessor / preprocess_data
# =============================================================================

class TestPreprocessor:

    def test_returns_column_transformer(self, X_y):
        X_train, _, _, _ = X_y
        prep = build_preprocessor(X_train)
        assert isinstance(prep, ColumnTransformer)

    def test_output_is_sparse(self, X_y, preprocessor):
        X_train, _, X_test, _ = X_y
        X_tr, X_te = preprocess_data(preprocessor, X_train, X_test)
        assert issparse(X_tr)
        assert issparse(X_te)

    def test_output_format_is_csr(self, X_y, preprocessor):
        X_train, _, X_test, _ = X_y
        X_tr, _ = preprocess_data(preprocessor, X_train, X_test)
        assert X_tr.format == "csr"

    def test_consistent_feature_count(self, X_y, preprocessor):
        X_train, _, X_test, _ = X_y
        X_tr, X_te = preprocess_data(preprocessor, X_train, X_test)
        assert X_tr.shape[1] == X_te.shape[1]

    def test_no_nan_after_imputation(self, X_y):
        X_train, _, X_test, _ = X_y
        X_train_nan = X_train.copy()
        X_train_nan.iloc[0, 4] = np.nan
        prep = build_preprocessor(X_train_nan)
        X_tr, _ = preprocess_data(prep, X_train_nan, X_test)
        assert not np.isnan(X_tr.toarray()).any()

    def test_unknown_category_no_error(self, X_y, preprocessor):
        X_train, _, X_test, _ = X_y
        preprocess_data(preprocessor, X_train, X_test)   # fit
        X_unseen = X_test.copy()
        X_unseen["coco_group"] = "unknown_weather_type"
        try:
            preprocessor.transform(X_unseen)
        except Exception as e:
            pytest.fail(f"Unknown category raised: {e}")


# =============================================================================
# 6. build_final_pipeline / train_final_pipeline
# =============================================================================

class TestFinalPipeline:

    def test_pipeline_is_sklearn_pipeline(self, fitted_pipeline):
        assert isinstance(fitted_pipeline, Pipeline)

    def test_pipeline_has_prep_and_model_steps(self, fitted_pipeline):
        assert "prep"  in fitted_pipeline.named_steps
        assert "model" in fitted_pipeline.named_steps

    def test_predict_shape(self, fitted_pipeline, X_y):
        _, _, X_test, _ = X_y
        preds = fitted_pipeline.predict(X_test)
        assert preds.shape == (len(X_test),)

    def test_no_nan_predictions(self, fitted_pipeline, X_y):
        _, _, X_test, _ = X_y
        preds = fitted_pipeline.predict(X_test)
        assert not np.isnan(preds).any()

    def test_predict_is_numeric(self, fitted_pipeline, X_y):
        _, _, X_test, _ = X_y
        preds = fitted_pipeline.predict(X_test)
        assert np.issubdtype(preds.dtype, np.number)

    def test_best_params_passed_to_model(self, preprocessor, X_y):
        X_train, y_train, _, _ = X_y
        best_params = {
            "model__n_estimators":     50,
            "model__max_depth":        4,
            "model__learning_rate":    0.05,
            "model__subsample":        0.8,
            "model__colsample_bytree": 0.8,
            "model__reg_alpha":        0.1,
            "model__min_child_weight": 3,
            "model__reg_lambda":       2.0,
        }
        pipe = train_final_pipeline(preprocessor, best_params, X_train, y_train)
        xgb = pipe.named_steps["model"]
        assert xgb.n_estimators    == 50
        assert xgb.max_depth       == 4
        assert xgb.learning_rate   == pytest.approx(0.05)


# =============================================================================
# 7. Signature inference
# =============================================================================

class TestSignatureInference:

    def test_int_columns_cast_to_float64(self, X_y):
        _, _, X_test, _ = X_y
        X_sig = X_test.astype({col: "float64" for col in X_test.select_dtypes("int").columns})
        assert len(X_sig.select_dtypes("int").columns) == 0

    def test_signature_columns_match_features(self, fitted_pipeline, X_y):
        from mlflow.models.signature import infer_signature
        _, _, X_test, _ = X_y
        pred = fitted_pipeline.predict(X_test)
        X_sig = X_test.astype({col: "float64" for col in X_test.select_dtypes("int").columns})
        sig = infer_signature(X_sig, pred)
        sig_cols = [inp.name for inp in sig.inputs.inputs]
        assert set(sig_cols) == set(FEATURES)


# =============================================================================
# 8. MLflow helpers (mocked — no real server needed)
# =============================================================================

class TestSetupMlflow:

    @patch("retrain.MlflowClient")
    @patch("retrain.mlflow.set_tracking_uri")
    def test_creates_experiment_if_missing(self, mock_uri, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get_experiment_by_name.return_value = None
        mock_client.create_experiment.return_value = "42"
        mock_client_cls.return_value = mock_client

        eid = setup_mlflow("http://localhost:5001", "new_experiment")
        mock_client.create_experiment.assert_called_once_with("new_experiment")
        assert eid == "42"

    @patch("retrain.MlflowClient")
    @patch("retrain.mlflow.set_tracking_uri")
    def test_uses_existing_experiment(self, mock_uri, mock_client_cls):
        mock_exp = MagicMock()
        mock_exp.experiment_id = "99"
        mock_client = MagicMock()
        mock_client.get_experiment_by_name.return_value = mock_exp
        mock_client_cls.return_value = mock_client

        eid = setup_mlflow("http://localhost:5001", "existing_experiment")
        mock_client.create_experiment.assert_not_called()
        assert eid == "99"


class TestEnsureRegisteredModel:

    def test_creates_model_if_missing(self):
        import mlflow.exceptions
        client = MagicMock()
        client.get_registered_model.side_effect = mlflow.exceptions.MlflowException("not found")
        ensure_registered_model(client, "citibike_forecast_model")
        client.create_registered_model.assert_called_once_with("citibike_forecast_model")

    def test_does_not_create_if_exists(self):
        client = MagicMock()
        client.get_registered_model.return_value = MagicMock()
        ensure_registered_model(client, "citibike_forecast_model")
        client.create_registered_model.assert_not_called()


class TestPromoteToStaging:

    def test_sets_staging_alias(self):
        mock_mv = MagicMock()
        mock_mv.version = "5"
        client = MagicMock()
        client.search_model_versions.return_value = [mock_mv]

        version = promote_to_staging(client, "citibike_forecast_model")
        client.set_registered_model_alias.assert_called_once_with(
            name="citibike_forecast_model", alias="staging", version="5"
        )
        assert version == "5"

    def test_raises_if_no_versions(self):
        client = MagicMock()
        client.search_model_versions.return_value = []
        with pytest.raises(RuntimeError, match="No versions found"):
            promote_to_staging(client, "citibike_forecast_model")


class TestLogAndRegisterModel:

    @patch("retrain.mlflow.sklearn.log_model")
    @patch("retrain.mlflow.log_params")
    @patch("retrain.mlflow.log_metric")
    @patch("retrain.mlflow.start_run")
    def test_metrics_logged(self, mock_run, mock_metric, mock_params, mock_log_model, fitted_pipeline, X_y):
        _, _, X_test, y_test = X_y
        mock_run.return_value.__enter__ = MagicMock(
            return_value=MagicMock(info=MagicMock(run_id="abc123"))
        )
        mock_run.return_value.__exit__ = MagicMock(return_value=False)
        mock_log_model.return_value    = MagicMock(signature=MagicMock(inputs=[], outputs=[]))

        log_and_register_model(fitted_pipeline, X_test, y_test, "exp_1", "citibike_forecast_model")

        logged_metrics = [c[0][0] for c in mock_metric.call_args_list]
        assert "rmse" in logged_metrics
        assert "mae"  in logged_metrics
        assert "r2"   in logged_metrics


# =============================================================================
# 9. save_model_locally
# =============================================================================

class TestSaveModelLocally:

    def test_file_created(self, fitted_pipeline, tmp_path):
        path = tmp_path / "model" / "pipeline.joblib"
        save_model_locally(fitted_pipeline, path)
        assert path.exists()

    def test_loaded_pipeline_is_valid(self, fitted_pipeline, tmp_path):
        import joblib
        path = tmp_path / "model.joblib"
        save_model_locally(fitted_pipeline, path)
        loaded = joblib.load(path)
        assert isinstance(loaded, Pipeline)

    def test_predictions_match_after_reload(self, fitted_pipeline, X_y, tmp_path):
        import joblib
        _, _, X_test, _ = X_y
        path = tmp_path / "model.joblib"
        save_model_locally(fitted_pipeline, path)
        loaded = joblib.load(path)
        np.testing.assert_array_almost_equal(
            fitted_pipeline.predict(X_test),
            loaded.predict(X_test),
        )