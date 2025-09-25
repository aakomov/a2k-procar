from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import mlflow
import mlflow.sklearn

def preprocess_data():
    df = pd.read_csv("/opt/airflow/dags/data/cars.csv")
    df = df.dropna()
    df.to_csv("/opt/airflow/dags/data/cars_clean.csv", index=False)

def train_model():
    df = pd.read_csv("/opt/airflow/dags/data/cars_clean.csv")
    X = df[['Year','Present_Price','Kms_Driven']]
    y = df['Selling_Price']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    mlflow.set_tracking_uri("http://host.docker.internal:5000")
    mlflow.set_experiment("car-price")

    with mlflow.start_run():
        model = LinearRegression()
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        rmse = mean_squared_error(y_test, preds, squared=False)
        mlflow.log_metric("rmse", rmse)
        mlflow.sklearn.log_model(model, "model")

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="procar_pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    preprocess = PythonOperator(
        task_id="preprocess",
        python_callable=preprocess_data,
    )

    train = PythonOperator(
        task_id="train",
        python_callable=train_model,
    )

    preprocess >> train
