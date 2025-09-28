"""
Скрипт для обучения модели предсказания цены автомобиля
и регистрации в MLflow
"""

import argparse
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from dotenv import load_dotenv
import os

from mlflow.tracking import MlflowClient

# Загружаем .env для MLFLOW_TRACKING_URI
load_dotenv()


def load_and_prepare_data(sample_frac: float = 1.0):
    """Загрузка очищенных данных и подготовка train/test"""
    df = pd.read_parquet("../data/output_data/car_data_cleaned.parquet")

    y = df["Selling_Price"]

    features = [
        "Year",
        "Present_Price",
        "Kms_Driven",
        "Owner",
        "Car_Age",
        "Kms_Per_Year",
        "Fuel_Type",
        "Seller_Type",
        "Transmission",
    ]
    X = df[features]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    if sample_frac < 1.0:
        X_train, _, y_train, _ = train_test_split(
            X_train, y_train, train_size=sample_frac, random_state=42
        )

    return X_train, X_test, y_train, y_test


def create_pipeline(n_estimators: int, max_depth: int):
    """Создаём pipeline с препроцессингом и RandomForest"""
    categorical = ["Fuel_Type", "Seller_Type", "Transmission"]
    numeric = [
        "Year",
        "Present_Price",
        "Kms_Driven",
        "Owner",
        "Car_Age",
        "Kms_Per_Year",
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )

    model = RandomForestRegressor(
        n_estimators=n_estimators, max_depth=max_depth, random_state=42
    )

    pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])
    return pipeline


def evaluate_model(model, X_test, y_test):
    """Оценка качества модели"""
    y_pred = model.predict(X_test)
    metrics = {
        "MAE": mean_absolute_error(y_test, y_pred),
        "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
        "R2": r2_score(y_test, y_pred),
    }
    return metrics, y_pred


def train_model(
    model_name="car_price_model",
    n_estimators=100,
    max_depth=10,
    sample_frac=1.0,
    register_as_prod=True,
):
    """Обучает модель и логирует в MLflow"""
    print("=" * 60)
    print("ОБУЧЕНИЕ МОДЕЛИ ПРОГНОЗА ЦЕНЫ АВТОМОБИЛЯ")
    print("=" * 60)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("car_price_experiment")

    X_train, X_test, y_train, y_test = load_and_prepare_data(sample_frac=sample_frac)

    model = create_pipeline(n_estimators=n_estimators, max_depth=max_depth)

    client = MlflowClient()

    with mlflow.start_run() as run:
        print("Обучение модели...")
        model.fit(X_train, y_train)

        metrics, _ = evaluate_model(model, X_test, y_test)

        print("\nРезультаты:")
        for k, v in metrics.items():
            print(f"{k}: {v:.4f}")
            mlflow.log_metric(k, v)

        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("sample_frac", sample_frac)

        print(f"\nСохраняем модель '{model_name}' в MLflow...")
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=model_name,
        )

        if register_as_prod:
            version = client.get_latest_versions(model_name, stages=[])[-1].version
            client.set_registered_model_alias(
                name=model_name,
                alias="champion",
                version=version,
            )
            print(f"Модель зарегистрирована как v{version} и получила alias 'champion'")

    print("\n" + "=" * 60)
    print("ОБУЧЕНИЕ ЗАВЕРШЕНО УСПЕШНО")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Обучение модели предсказания цены авто")
    parser.add_argument("--model-name", default="car_price_model", help="Имя модели в MLflow")
    parser.add_argument("--n-estimators", type=int, default=100, help="Количество деревьев")
    parser.add_argument("--max-depth", type=int, default=10, help="Максимальная глубина деревьев")
    parser.add_argument("--sample-frac", type=float, default=1.0, help="Доля данных для использования")
    parser.add_argument("--no-prod", action="store_true", help="Не регистрировать alias champion")

    args = parser.parse_args()

    train_model(
        model_name=args.model_name,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        sample_frac=args.sample_frac,
        register_as_prod=not args.no_prod,
    )


if __name__ == "__main__":
    main()
