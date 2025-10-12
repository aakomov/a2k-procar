from prefect import flow, task
import mlflow
from src.preprocessing import load_data, split_data
from src.train import train_model, evaluate_model
from src.inference import save_model
import os


@task
def load_and_split():
    df = load_data()
    train, test = split_data(df)
    return train, test


@task
def train(train):
    model = train_model(train)
    os.makedirs("models", exist_ok=True)
    save_model(model, "models/car_price_model.joblib")
    return model


@task
def evaluate(model, test):
    metrics = evaluate_model(model, test)
    mlflow.log_metrics(metrics)
    return metrics


@flow(name="train-car-price")
def train_pipeline():
    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment("car_price_prediction")

    with mlflow.start_run():
        train_df, test_df = load_and_split()
        model = train(train_df)
        metrics = evaluate(model, test_df)
        print("✅ Training complete. Metrics:", metrics)