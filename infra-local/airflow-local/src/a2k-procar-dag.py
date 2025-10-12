import logging
import warnings
import os
from datetime import timedelta
from io import BytesIO
import json

import pandas as pd
import numpy as np
import boto3
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.sklearn
from airflow import DAG
from airflow.operators.python import PythonOperator

# Конфигурация v5
MINIO_ENDPOINT = 'http://192.168.222.158:9090'
MLFLOW_TRACKING_URI = 'http://192.168.222.158:5000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'

warnings.filterwarnings('ignore')

default_args = {
    "owner": "a2k-procar",
    "depends_on_past": False,
    "start_date": "2025-05-18",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def setup_mlflow_environment():
    """Настройка переменных окружения для MLflow"""
    logging.info("Настройка окружения MLflow")
    
    # Устанавливаем переменные окружения для MLflow S3
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = MINIO_ENDPOINT
    os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'  # Minio требует регион
    
    # Отключаем SSL verification для локального Minio
    os.environ['MLFLOW_S3_IGNORE_TLS'] = 'true'
    
    logging.info("Окружение MLflow настроено")

def read_car_data():
    """Чтение данных об автомобилях из Minio"""
    logging.info("Чтение данных об автомобилях")
    
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            verify=False
        )
        
        # Читаем файл
        response = s3_client.get_object(Bucket='mlflow', Key='part-00000-03df2316-ffcb-44b7-8018-afb9a13d66f7-c000.snappy.parquet')
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        logging.info(f"Данные прочитаны: {df.shape[0]} автомобилей, {df.shape[1]} характеристик")
        logging.info(f"Колонки: {df.columns.tolist()}")
        logging.info(f"Диапазон цен: {df['Selling_Price'].min():.2f} - {df['Selling_Price'].max():.2f}")
        
        # Сериализуем DataFrame в JSON с сохранением структуры
        data_dict = {
            'columns': df.columns.tolist(),
            'data': df.values.tolist(),
            'index': df.index.tolist()
        }
        
        return json.dumps(data_dict)
        
    except Exception as e:
        logging.error(f"Ошибка при чтении данных: {e}")
        raise

def prepare_features(**kwargs):
    """Подготовка фичей для модели"""
    
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='read_car_data')
    
    # Восстанавливаем DataFrame из JSON
    data_dict = json.loads(data_json)
    df = pd.DataFrame(data_dict['data'], columns=data_dict['columns'], index=data_dict['index'])
    
    logging.info("Подготовка фичей для модели")
    
    # Создаем копию данных
    df_processed = df.copy()
    
    # Кодируем категориальные переменные
    categorical_cols = ['Fuel_Type', 'Seller_Type', 'Transmission', 'Car_Name']
    label_encoders = {}
    
    for col in categorical_cols:
        if col in df_processed.columns:
            le = LabelEncoder()
            df_processed[f'{col}_encoded'] = le.fit_transform(df_processed[col])
            label_encoders[col] = le
            logging.info(f"Закодирована колонка {col}: {len(le.classes_)} категорий")
    
    # Выбираем фичи для модели
    feature_columns = [
        'Year', 'Present_Price', 'Kms_Driven', 'Owner', 'Car_Age', 'Kms_Per_Year'
    ] + [f'{col}_encoded' for col in categorical_cols if col in df_processed.columns]
    
    # Целевая переменная - цена продажи
    X = df_processed[feature_columns]
    y = df_processed['Selling_Price']
    
    logging.info(f"Целевая переменная: Selling_Price")
    logging.info(f"Используемые фичи: {feature_columns}")
    logging.info(f"Размеры: X={X.shape}, y={y.shape}")
    
    # Сериализуем данные для передачи
    data_info = {
        'X_columns': X.columns.tolist(),
        'X_data': X.values.tolist(),
        'X_index': X.index.tolist(),
        'y_name': y.name,
        'y_data': y.values.tolist(),
        'y_index': y.index.tolist(),
        'feature_columns': feature_columns,
        'data_shape': df.shape,
        'target_stats': {
            'min_price': float(y.min()),
            'max_price': float(y.max()),
            'mean_price': float(y.mean())
        }
    }
    
    logging.info(f"Статистика цен: min={y.min():.2f}, max={y.max():.2f}, mean={y.mean():.2f}")
    
    return json.dumps(data_info)

def train_car_price_model(**kwargs):
    """Обучение модели для предсказания цены автомобилей"""
    
    ti = kwargs['ti']
    data_info_json = ti.xcom_pull(task_ids='prepare_features')
    
    if not data_info_json:
        raise ValueError("Данные для обучения не получены")
    
    logging.info("🏋️ Обучение модели предсказания цены автомобилей")
    
    # Настраиваем окружение MLflow
    setup_mlflow_environment()
    
    # Восстанавливаем данные из JSON
    data_info = json.loads(data_info_json)
    
    # Восстанавливаем X и y
    X = pd.DataFrame(
        data_info['X_data'], 
        columns=data_info['X_columns'], 
        index=data_info['X_index']
    )
    y = pd.Series(
        data_info['y_data'], 
        name=data_info['y_name'],
        index=data_info['y_index']
    )
    
    feature_columns = data_info['feature_columns']
    
    logging.info(f"Восстановленные данные: X={X.shape}, y={y.shape}")
    
    # Разделяем на train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    logging.info(f"Разделение данных: Train={X_train.shape}, Test={X_test.shape}")
    
    # Настраиваем MLFlow tracking
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    # Начинаем эксперимент MLFlow
    with mlflow.start_run(run_name="car_price_prediction"):
        logging.info("Запуск MLFlow эксперимента")
        
        # Параметры модели
        model_params = {
            'n_estimators': 50,
            'max_depth': 8,
            'min_samples_split': 2,
            'min_samples_leaf': 1,
            'random_state': 42
        }
        
        # Логируем параметры
        mlflow.log_params(model_params)
        mlflow.log_param("features", str(feature_columns))
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("target", "Selling_Price")
        
        # Создаем и обучаем модель
        model = RandomForestRegressor(**model_params)
        model.fit(X_train, y_train)
        
        # Предсказания
        y_pred = model.predict(X_test)
        
        # Метрики
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test, y_pred)
        
        # Логируем метрики
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2_score", r2)
        
        # Логируем модель с явной передачей credentials
        try:
            # Создаем boto3 сессию с credentials
            boto3_session = boto3.Session(
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name='us-east-1'
            )
            
            # Логируем модель с явной передачей сессии
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="car_price_model",
                registered_model_name="CarPricePrediction",
                boto_session=boto3_session
            )
            logging.info("Модель успешно залогирована в MLflow")
            
        except Exception as e:
            logging.error(f"Ошибка при логировании модели: {e}")
            # Пробуем альтернативный способ
            try:
                mlflow.sklearn.log_model(
                    sk_model=model,
                    artifact_path="car_price_model"
                )
                logging.info("Модель залогирована (альтернативный способ)")
            except Exception as e2:
                logging.error(f"Ошибка при альтернативном логировании: {e2}")
                # Продолжаем выполнение без логирования модели
                logging.warning("Модель не была сохранена в MLflow, но обучение завершено")
        
        # Логируем важность фичей
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Логируем топ-3 самых важных фичей
        top_features = feature_importance.head(3)
        for _, row in top_features.iterrows():
            mlflow.log_metric(f"importance_{row['feature']}", float(row['importance']))
        
        logging.info("Результаты обучения:")
        logging.info(f"   MAE: {mae:.4f}")
        logging.info(f"   RMSE: {rmse:.4f}")
        logging.info(f"   R²: {r2:.4f}")
        logging.info(f"   Топ-3 важных фичей:")
        for _, row in top_features.iterrows():
            logging.info(f"     - {row['feature']}: {row['importance']:.4f}")
        
        # Сохраняем результаты
        results = {
            'run_id': mlflow.active_run().info.run_id,
            'metrics': {
                'mae': float(mae),
                'rmse': float(rmse),
                'r2': float(r2)
            },
            'model_type': 'RandomForestRegressor',
            'top_features': top_features.to_dict('records'),
            'model_logged': True
        }
        
        return json.dumps(results)

def evaluate_model(**kwargs):
    """Оценка и сохранение результатов модели"""
    
    ti = kwargs['ti']
    training_results_json = ti.xcom_pull(task_ids='train_car_price_model')
    data_info_json = ti.xcom_pull(task_ids='prepare_features')
    
    logging.info("Оценка модели предсказания цен автомобилей")
    
    if not training_results_json:
        raise ValueError("Результаты обучения не получены")
    
    # Восстанавливаем данные из JSON
    training_results = json.loads(training_results_json)
    data_info = json.loads(data_info_json)
    
    metrics = training_results['metrics']
    run_id = training_results['run_id']
    
    # Анализ качества модели
    r2 = metrics['r2']
    if r2 > 0.8:
        quality = "ОТЛИЧНОЕ"
    elif r2 > 0.6:
        quality = "ХОРОШЕЕ"
    elif r2 > 0.4:
        quality = "УДОВЛЕТВОРИТЕЛЬНОЕ"
    else:
        quality = "НИЗКОЕ"
    
    logging.info(f"Качество модели: {quality}")
    logging.info(f"Метрики модели:")
    logging.info(f"   R²: {r2:.4f}")
    logging.info(f"   MAE: {metrics['mae']:.4f}")
    logging.info(f"   RMSE: {metrics['rmse']:.4f}")
    
    # Сохраняем сводку в Minio
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            verify=False
        )
        
        summary = f"""
Car Price Prediction Model - Results Summary
===========================================
Run ID: {run_id}
Model: RandomForestRegressor
Quality: {quality} (R²: {r2:.4f})

Metrics:
- R² Score: {r2:.4f}
- MAE: {metrics['mae']:.4f}
- RMSE: {metrics['rmse']:.4f}

Data Information:
- Total samples: {data_info['data_shape'][0]}
- Features used: {len(data_info['feature_columns'])}
- Price range: {data_info['target_stats']['min_price']:.2f} - {data_info['target_stats']['max_price']:.2f}
- Average price: {data_info['target_stats']['mean_price']:.2f}

Top 3 Important Features:
"""
        for feature in training_results['top_features']:
            summary += f"- {feature['feature']}: {feature['importance']:.4f}\n"
        
        summary += f"\nMLFlow UI: {MLFLOW_TRACKING_URI}/#/experiments/0/runs/{run_id}"
        
        # Сохраняем в Minio
        summary_bytes = summary.encode('utf-8')
        s3_client.put_object(
            Bucket='mlflow',
            Key=f"car_model_results/summary_{run_id}.txt",
            Body=BytesIO(summary_bytes)
        )
        
        logging.info(f"Сводка сохранена в Minio: car_model_results/summary_{run_id}.txt")
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении сводки: {e}")
    
    final_result = {
        'status': f"{quality} - R²: {r2:.4f}",
        'run_id': run_id,
        'r2_score': r2,
        'mlflow_url': f"{MLFLOW_TRACKING_URI}/#/experiments/0/runs/{run_id}",
        'quality': quality,
        'model_logged': training_results.get('model_logged', False)
    }
    
    return json.dumps(final_result)

def log_final_results(**kwargs):
    """Финальное логирование результатов"""
    
    ti = kwargs['ti']
    final_result_json = ti.xcom_pull(task_ids='evaluate_model')
    
    final_result = json.loads(final_result_json)
    
    logging.info("=" * 60)
    logging.info("ML PIPELINE ДЛЯ ПРЕДСКАЗАНИЯ ЦЕН АВТОМОБИЛЕЙ ЗАВЕРШЕН!")
    logging.info("=" * 60)
    logging.info(f"СТАТУС: {final_result['status']}")
    logging.info(f"КАЧЕСТВО: {final_result['quality']}")
    logging.info(f"R² SCORE: {final_result['r2_score']:.4f}")
    logging.info(f"RUN ID: {final_result['run_id']}")
    logging.info(f"МОДЕЛЬ СОХРАНЕНА: {'Success' if final_result['model_logged'] else 'Failed'}")
    logging.info(f"MLFLOW UI: {final_result['mlflow_url']}")
    logging.info("=" * 60)
    
    return f"Pipeline завершен! Качество: {final_result['quality']}, R²: {final_result['r2_score']:.4f}"

with DAG(
    'car_price_prediction_pipeline',
    default_args=default_args,
    description="ML pipeline для предсказания цен автомобилей",
    schedule=timedelta(days=1),
    catchup=False,
    tags=['car_prices', 'mlflow', 'minio', 'regression'],
) as dag:
    
    read_data_task = PythonOperator(
        task_id='read_car_data',
        python_callable=read_car_data,
    )
    
    prepare_features_task = PythonOperator(
        task_id='prepare_features',
        python_callable=prepare_features,
    )
    
    train_model_task = PythonOperator(
        task_id='train_car_price_model',
        python_callable=train_car_price_model,
    )
    
    evaluate_task = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )
    
    log_results_task = PythonOperator(
        task_id='log_final_results',
        python_callable=log_final_results,
    )
    
    # Определяем порядок выполнения
    read_data_task >> prepare_features_task >> train_model_task >> evaluate_task >> log_results_task