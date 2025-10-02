import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.base import BaseEstimator


def _build_pipeline() -> Pipeline:
    """
    Build preprocessing + RandomForest pipeline
    """
    categorical = ["Fuel_Type", "Seller_Type", "Transmission"]
    numeric = [
        "Year", "Present_Price", "Kms_Driven",
        "Owner", "Car_Age", "Kms_Per_Year"
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=15,
        random_state=42
    )

    pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])
    return pipeline


def train_model(train: pd.DataFrame) -> BaseEstimator:
    """
    Train model on training data.
    """
    X = train.drop(columns=["Selling_Price", "Car_Name"])
    y = train["Selling_Price"]

    pipeline = _build_pipeline()
    pipeline.fit(X, y)
    return pipeline


def evaluate_model(model: BaseEstimator, test: pd.DataFrame) -> dict:
    """
    Evaluate model on test data.
    Returns MAE, RMSE, R2.
    """
    X_test = test.drop(columns=["Selling_Price", "Car_Name"])
    y_test = test["Selling_Price"]

    y_pred = model.predict(X_test)

    metrics = {
        "MAE": mean_absolute_error(y_test, y_pred),
        "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
        "R2": r2_score(y_test, y_pred),
    }
    return metrics
