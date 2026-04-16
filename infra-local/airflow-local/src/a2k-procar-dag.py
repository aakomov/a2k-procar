import logging
import warnings
import os
from datetime import timedelta
from io import BytesIO
import json
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
import boto3
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.sklearn
from airflow import DAG
from airflow.operators.python import PythonOperator

# S3
BUCKET = 'mlflow'
DATA_PRQ = 'output_data/car_data_cleaned.parquet'

# Конфигурация v8
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
    
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = MINIO_ENDPOINT
    os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
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
        
        response = s3_client.get_object(Bucket=BUCKET, Key=DATA_PRQ)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        logging.info(f"Данные прочитаны: {df.shape[0]} автомобилей, {df.shape[1]} характеристик")
        
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

def safe_cross_validation(model, X, y, cv=3):
    """Безопасная кросс-валидация для маленьких датасетов"""
    try:
        if len(X) < cv * 2:
            logging.warning(f"Слишком мало данных для {cv}-fold CV. Используем 2-fold.")
            cv = min(2, len(X) // 2)
        
        if cv < 2:
            logging.warning("Недостаточно данных для кросс-валидации")
            return np.nan, np.nan
        
        scores = cross_val_score(model, X, y, cv=cv, scoring='r2')
        return float(scores.mean()), float(scores.std())
    except Exception as e:
        logging.warning(f"Ошибка при кросс-валидации: {e}")
        return np.nan, np.nan

def train_car_price_model(**kwargs):
    """Обучение модели для предсказания цены автомобилей"""
    
    ti = kwargs['ti']
    data_info_json = ti.xcom_pull(task_ids='prepare_features')
    
    if not data_info_json:
        raise ValueError("Данные для обучения не получены")
    
    logging.info("Обучение модели предсказания цены автомобилей")
    
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
    
    # Проверяем размер данных для разделения
    if len(X) < 4:
        logging.error("Недостаточно данных для обучения (нужно минимум 4 samples)")
        raise ValueError("Недостаточно данных для обучения")
    
    # Разделяем на train/test с проверкой размера
    test_size = min(0.2, 2/len(X))  # Гарантируем минимум 2 samples в тесте
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
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
        
        # Метрики с проверкой на достаточность данных
        if len(y_test) >= 2:
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, y_pred)
        else:
            logging.warning("Недостаточно тестовых данных для вычисления метрик")
            mae, mse, rmse, r2 = np.nan, np.nan, np.nan, np.nan
        
        # Безопасная кросс-валидация
        cv_mean, cv_std = safe_cross_validation(model, X, y, cv=3)
        
        # Логируем метрики только если они не NaN
        metrics_to_log = {}
        if not np.isnan(mae): metrics_to_log["mae"] = mae
        if not np.isnan(mse): metrics_to_log["mse"] = mse
        if not np.isnan(rmse): metrics_to_log["rmse"] = rmse
        if not np.isnan(r2): metrics_to_log["r2_score"] = r2
        if not np.isnan(cv_mean): metrics_to_log["cv_r2_mean"] = cv_mean
        if not np.isnan(cv_std): metrics_to_log["cv_r2_std"] = cv_std
        
        if metrics_to_log:
            mlflow.log_metrics(metrics_to_log)
        else:
            logging.warning("Все метрики NaN, ничего не логируем в MLflow")
        
        # Логируем модель
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="car_price_model"
        )
        logging.info("Модель успешно залогирована в MLflow")
        
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
        if not np.isnan(r2):
            logging.info(f"   R²: {r2:.4f}")
        if not np.isnan(cv_mean):
            logging.info(f"   CV R²: {cv_mean:.4f} ± {cv_std:.4f}")
        if not np.isnan(mae):
            logging.info(f"   MAE: {mae:.4f}")
        if not np.isnan(rmse):
            logging.info(f"   RMSE: {rmse:.4f}")
        
        logging.info(f"   Топ-3 важных фичей:")
        for _, row in top_features.iterrows():
            logging.info(f"     - {row['feature']}: {row['importance']:.4f}")
        
        # Сохраняем результаты
        results = {
            'run_id': mlflow.active_run().info.run_id,
            'metrics': {
                'mae': float(mae) if not np.isnan(mae) else None,
                'rmse': float(rmse) if not np.isnan(rmse) else None,
                'r2': float(r2) if not np.isnan(r2) else None,
                'cv_r2_mean': float(cv_mean) if not np.isnan(cv_mean) else None,
                'cv_r2_std': float(cv_std) if not np.isnan(cv_std) else None
            },
            'model_type': 'RandomForestRegressor',
            'top_features': top_features.to_dict('records'),
            'model_logged': True,
            'model_params': model_params,
            'data_size': {
                'total': len(X),
                'train': len(X_train),
                'test': len(X_test)
            }
        }
        
        return json.dumps(results)

def validate_model_ab_test(**kwargs):
    """A/B тестирование и валидация модели"""
    
    ti = kwargs['ti']
    training_results_json = ti.xcom_pull(task_ids='train_car_price_model')
    data_info_json = ti.xcom_pull(task_ids='prepare_features')
    
    logging.info("A/B тестирование и валидация модели")
    
    if not training_results_json:
        raise ValueError("Результаты обучения не получены")
    
    # Восстанавливаем данные из JSON
    training_results = json.loads(training_results_json)
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
    
    run_id = training_results['run_id']
    current_metrics = training_results['metrics']
    
    # Проверяем достаточно ли данных для A/B теста
    if len(X) < 6:
        logging.warning("Недостаточно данных для A/B теста. Пропускаем валидацию.")
        
        validation_results = {
            'run_id': run_id,
            'validation_status': "SKIPPED - Not enough data",
            'improvement_percent': 0,
            'metrics_a': {'r2': current_metrics.get('r2', 0)},
            'metrics_b': {'r2': current_metrics.get('r2', 0)},
            'validation_strategy': 'skipped_insufficient_data'
        }
        
        return json.dumps(validation_results)
    
    # Настраиваем MLflow
    setup_mlflow_environment()
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    # Стратегия валидации: Time-based split для A/B теста
    logging.info("Применяем временную стратегию валидации")
    
    # Разделяем данные по "времени" (индексу как proxy)
    split_point = max(2, int(len(X) * 0.6))  # Минимум 2 samples в каждой части
    
    X_old = X.iloc[:split_point]
    y_old = y.iloc[:split_point]
    X_new = X.iloc[split_point:]
    y_new = y.iloc[split_point:]
    
    logging.info(f"A/B split: Old={X_old.shape}, New={X_new.shape}")
    
    # Проверяем достаточно ли данных для тестирования
    if len(X_new) < 2 or len(X_old) < 2:
        logging.warning("Недостаточно данных для A/B теста после split")
        
        validation_results = {
            'run_id': run_id,
            'validation_status': "SKIPPED - Not enough data after split",
            'improvement_percent': 0,
            'metrics_a': {'r2': current_metrics.get('r2', 0)},
            'metrics_b': {'r2': current_metrics.get('r2', 0)},
            'validation_strategy': 'skipped_insufficient_data'
        }
        
        return json.dumps(validation_results)
    
    # Модель A (baseline) - обучается на "старых" данных
    model_a = RandomForestRegressor(**training_results['model_params'])
    model_a.fit(X_old, y_old)
    
    # Метрики для модели A на новых данных
    y_pred_a = model_a.predict(X_new)
    metrics_a = {
        'r2': r2_score(y_new, y_pred_a) if len(y_new) >= 2 else 0,
        'mae': mean_absolute_error(y_new, y_pred_a) if len(y_new) >= 1 else 0,
        'rmse': np.sqrt(mean_squared_error(y_new, y_pred_a)) if len(y_new) >= 1 else 0
    }
    
    # Метрики для модели B на новых данных
    # Для этого нам нужно переобучить модель B на train+old данных и проверить на новых
    train_split = max(1, int(len(X_new) * 0.7))
    X_train_ab = pd.concat([X_old, X_new.iloc[:train_split]])
    y_train_ab = pd.concat([y_old, y_new.iloc[:train_split]])
    X_test_ab = X_new.iloc[train_split:]
    y_test_ab = y_new.iloc[train_split:]
    
    if len(X_test_ab) < 1:
        logging.warning("Недостаточно данных для тестирования модели B")
        metrics_b = metrics_a  # Используем те же метрики
    else:
        model_b = RandomForestRegressor(**training_results['model_params'])
        model_b.fit(X_train_ab, y_train_ab)
        y_pred_b = model_b.predict(X_test_ab)
        
        metrics_b = {
            'r2': r2_score(y_test_ab, y_pred_b) if len(y_test_ab) >= 2 else 0,
            'mae': mean_absolute_error(y_test_ab, y_pred_b) if len(y_test_ab) >= 1 else 0,
            'rmse': np.sqrt(mean_squared_error(y_test_ab, y_pred_b)) if len(y_test_ab) >= 1 else 0
        }
    
    # Статистическая значимость разницы (упрощенная версия)
    if metrics_a['r2'] != 0:
        improvement = ((metrics_b['r2'] - metrics_a['r2']) / metrics_a['r2']) * 100
    else:
        improvement = 0
    
    # Логируем результаты A/B теста в MLflow
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metrics({
            'ab_test_model_a_r2': metrics_a['r2'],
            'ab_test_model_b_r2': metrics_b['r2'],
            'ab_test_improvement_percent': improvement,
            'ab_test_model_a_mae': metrics_a['mae'],
            'ab_test_model_b_mae': metrics_b['mae'],
            'validation_set_size': len(X_new)
        })
        
        # Логируем стратегию валидации
        mlflow.log_param("validation_strategy", "time_based_ab_test")
        mlflow.log_param("ab_test_split_ratio", "60/40")
        
        # Определяем результат валидации
        if improvement > 5:  # Улучшение на 5% считается значимым
            validation_status = "PASSED - Significant improvement"
            mlflow.log_metric("validation_status", 1)
        elif improvement > -5:  # Ухудшение не более 5%
            validation_status = "PASSED - No significant degradation"
            mlflow.log_metric("validation_status", 1)
        else:
            validation_status = "FAILED - Significant degradation"
            mlflow.log_metric("validation_status", 0)
    
    logging.info("Результаты A/B теста:")
    logging.info(f"   Модель A (baseline) R²: {metrics_a['r2']:.4f}")
    logging.info(f"   Модель B (новая) R²: {metrics_b['r2']:.4f}")
    logging.info(f"   Улучшение: {improvement:.2f}%")
    logging.info(f"   Статус валидации: {validation_status}")
    
    # Сохраняем результаты валидации в Minio
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            verify=False
        )
        
        validation_report = f"""
A/B Test Validation Report
==========================
Run ID: {run_id}
Validation Strategy: Time-based split (60% old / 40% new)

Model A (Baseline) Metrics:
- R²: {metrics_a['r2']:.4f}
- MAE: {metrics_a['mae']:.4f}
- RMSE: {metrics_a['rmse']:.4f}

Model B (New) Metrics:
- R²: {metrics_b['r2']:.4f}
- MAE: {metrics_b['mae']:.4f}
- RMSE: {metrics_b['rmse']:.4f}

Comparison:
- Improvement: {improvement:.2f}%
- Validation Status: {validation_status}

Data Information:
- Total samples: {data_info['data_shape'][0]}
- Validation set size: {len(X_new)}
- Test set size: {len(X_test_ab)}
"""
        
        # Сохраняем в Minio
        report_bytes = validation_report.encode('utf-8')
        s3_client.put_object(
            Bucket='mlflow',
            Key=f"validation_reports/ab_test_{run_id}.txt",
            Body=BytesIO(report_bytes)
        )
        
        logging.info(f"Отчет валидации сохранен в Minio: validation_reports/ab_test_{run_id}.txt")
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении отчета валидации: {e}")
    
    validation_results = {
        'run_id': run_id,
        'validation_status': validation_status,
        'improvement_percent': improvement,
        'metrics_a': metrics_a,
        'metrics_b': metrics_b,
        'validation_strategy': 'time_based_ab_test'
    }
    
    return json.dumps(validation_results)

def evaluate_model(**kwargs):
    """Финальная оценка модели с учетом валидации"""
    
    ti = kwargs['ti']
    training_results_json = ti.xcom_pull(task_ids='train_car_price_model')
    validation_results_json = ti.xcom_pull(task_ids='validate_model_ab_test')
    data_info_json = ti.xcom_pull(task_ids='prepare_features')
    
    logging.info("Финальная оценка модели")
    
    if not training_results_json or not validation_results_json:
        raise ValueError("Данные для оценки не получены")
    
    # Восстанавливаем данные из JSON
    training_results = json.loads(training_results_json)
    validation_results = json.loads(validation_results_json)
    data_info = json.loads(data_info_json)
    
    metrics = training_results['metrics']
    run_id = training_results['run_id']
    validation_status = validation_results['validation_status']
    
    # Анализ качества модели
    r2 = metrics.get('r2', 0)
    cv_r2 = metrics.get('cv_r2_mean', 0)
    
    if r2 and r2 > 0.8 and cv_r2 and cv_r2 > 0.7:
        quality = "ОТЛИЧНОЕ"
    elif r2 and r2 > 0.6 and cv_r2 and cv_r2 > 0.5:
        quality = "ХОРОШЕЕ"
    elif r2 and r2 > 0.4:
        quality = "УДОВЛЕТВОРИТЕЛЬНОЕ"
    else:
        quality = "НИЗКОЕ"
    
    # Учитываем результат валидации
    if "FAILED" in validation_status:
        quality = f"{quality} + ВАЛИДАЦИЯ ПРОВАЛЕНА"
    elif "SKIPPED" in validation_status:
        quality = f"{quality} + ВАЛИДАЦИЯ ПРОПУЩЕНА"
    
    logging.info(f"Качество модели: {quality}")
    logging.info(f"Метрики модели:")
    if r2 is not None:
        logging.info(f"   R²: {r2:.4f}")
    if cv_r2 is not None:
        logging.info(f"   CV R²: {cv_r2:.4f} ± {metrics.get('cv_r2_std', 0):.4f}")
    if metrics.get('mae') is not None:
        logging.info(f"   MAE: {metrics['mae']:.4f}")
    if metrics.get('rmse') is not None:
        logging.info(f"   RMSE: {metrics['rmse']:.4f}")
    logging.info(f"   Статус валидации: {validation_status}")
    
    # Сохраняем финальную сводку в Minio
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            verify=False
        )
        
        summary = f"""
Car Price Prediction Model - Final Evaluation
============================================
Run ID: {run_id}
Model: RandomForestRegressor
Overall Quality: {quality}

Training Metrics:
- R² Score: {r2 if r2 is not None else 'N/A':.4f}
- CV R²: {cv_r2 if cv_r2 is not None else 'N/A':.4f} ± {metrics.get('cv_r2_std', 0) if metrics.get('cv_r2_std') is not None else 'N/A':.4f}
- MAE: {metrics.get('mae', 'N/A') if metrics.get('mae') is not None else 'N/A':.4f}
- RMSE: {metrics.get('rmse', 'N/A') if metrics.get('rmse') is not None else 'N/A':.4f}

Validation Results:
- A/B Test Status: {validation_status}
- Improvement: {validation_results.get('improvement_percent', 0):.2f}%
- Strategy: {validation_results.get('validation_strategy', 'N/A')}

Data Information:
- Total samples: {data_info['data_shape'][0]}
- Features used: {len(data_info['feature_columns'])}

Top 3 Important Features:
"""
        for feature in training_results['top_features']:
            summary += f"- {feature['feature']}: {feature['importance']:.4f}\n"
        
        summary += f"\nMLFlow UI: {MLFLOW_TRACKING_URI}/#/experiments/0/runs/{run_id}"
        
        # Сохраняем в Minio
        summary_bytes = summary.encode('utf-8')
        s3_client.put_object(
            Bucket='mlflow',
            Key=f"final_reports/summary_{run_id}.txt",
            Body=BytesIO(summary_bytes)
        )
        
        logging.info(f"Финальный отчет сохранен в Minio: final_reports/summary_{run_id}.txt")
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении финального отчета: {e}")
    
    final_result = {
        'status': f"{quality} - R²: {r2 if r2 is not None else 'N/A':.4f}",
        'run_id': run_id,
        'r2_score': r2,
        'cv_r2_score': cv_r2,
        'validation_status': validation_status,
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
    logging.info("ML PIPELINE С ВАЛИДАЦИЕЙ ЗАВЕРШЕН!")
    logging.info("=" * 60)
    logging.info(f"СТАТУС: {final_result['status']}")
    logging.info(f"КАЧЕСТВО: {final_result['quality']}")
    if final_result['r2_score'] is not None:
        logging.info(f"R² SCORE: {final_result['r2_score']:.4f}")
    if final_result['cv_r2_score'] is not None:
        logging.info(f"CV R²: {final_result['cv_r2_score']:.4f}")
    logging.info(f"ВАЛИДАЦИЯ: {final_result['validation_status']}")
    logging.info(f"RUN ID: {final_result['run_id']}")
    logging.info(f"МОДЕЛЬ СОХРАНЕНА: {'Success' if final_result['model_logged'] else 'Failed'}")
    logging.info(f"MLFLOW UI: {final_result['mlflow_url']}")
    logging.info("=" * 60)
    
    return f"Pipeline завершен! Качество: {final_result['quality']}, Валидация: {final_result['validation_status']}"

with DAG(
    'car_price_prediction_pipeline_with_validation',
    default_args=default_args,
    description="ML pipeline с A/B тестированием и валидацией моделей",
    schedule=timedelta(days=1),
    catchup=False,
    tags=['car_prices', 'mlflow', 'minio', 'validation', 'ab_testing'],
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
    
    validate_model_task = PythonOperator(
        task_id='validate_model_ab_test',
        python_callable=validate_model_ab_test,
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
    (read_data_task >> prepare_features_task >> train_model_task >> 
     validate_model_task >> evaluate_task >> log_results_task)