#!/usr/bin/env python3
"""
Скрипт для подготовки данных о автомобилях.
Загружает сырые данные, очищает, преобразует и сохраняет в Parquet.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import logging
import sys
import boto3
from io import StringIO, BytesIO
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def setup_s3_client(s3_endpoint, aws_access_key_id, aws_secret_access_key):
    """Настройка S3 клиента"""
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    return s3_client

def load_data_from_s3(s3_client, bucket_name, key):
    """Загрузка данных из S3"""
    try:
        logger.info(f"Загрузка данных из S3: {bucket_name}/{key}")
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        logger.info(f"Данные загружены. Размер: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из S3: {e}")
        raise

def save_data_to_s3(s3_client, df, bucket_name, key, format='parquet'):
    """Сохранение данных в S3"""
    try:
        logger.info(f"Сохранение данных в S3: {bucket_name}/{key}")
        
        if format == 'parquet':
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            s3_client.upload_fileobj(buffer, bucket_name, key)
        elif format == 'csv':
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        
        logger.info(f"Данные успешно сохранены: {bucket_name}/{key}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в S3: {e}")
        raise

def preprocess_data(df):
    """
    Основная функция предобработки данных
    """
    logger.info("Начало предобработки данных")
    
    # Создаем копию для безопасности
    df_clean = df.copy()
    
    # 1. Обработка пропущенных значений в целевой переменной
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['Selling_Price'])
    logger.info(f"Удалено записей с пропущенной ценой: {initial_count - len(df_clean)}")
    
    # 2. Обработка выбросов в цене и пробеге
    # Удаляем явные выбросы (например, цены < 1000 или > 100000000)
    df_clean = df_clean[(df_clean['Selling_Price'] > 1.00) & 
                       (df_clean['Selling_Price'] < 10.00)]
    
    # Обработка пробега
    df_clean = df_clean[(df_clean['Kms_Driven'] > 0) & 
                       (df_clean['Kms_Driven'] < 1000000)]
    
    # 3. Обработка пропусков в признаках
    # Для числовых признаков - заполняем медианой
    numeric_cols = ['Year', 'Kms_Driven']
    for col in numeric_cols:
        if col in df_clean.columns:
            median_val = df_clean[col].median()
            df_clean[col] = df_clean[col].fillna(median_val)
            logger.info(f"Заполнено пропусков в {col}: {df_clean[col].isna().sum()}")
    
    # Для категориальных признаков - заполняем модой или 'unknown'
    categorical_cols = ['Fuel_Type', 'Seller_Type', 'Transmission', 'Owner']
    for col in categorical_cols:
        if col in df_clean.columns:
            mode_val = df_clean[col].mode()[0] if not df_clean[col].mode().empty else 'unknown'
            df_clean[col] = df_clean[col].fillna(mode_val)
            logger.info(f"Заполнено пропусков в {col}: {df_clean[col].isna().sum()}")
    
    # 4. Валидация года выпуска
    current_year = datetime.now().year
    df_clean = df_clean[(df_clean['Year'] > 1950) & (df_clean['Year'] <= current_year)]
    
    # 5. Создание новых признаков (feature engineering)
    df_clean['Car_Age'] = current_year - df_clean['Year']
    df_clean['Kms_Per_Year'] = df_clean['Kms_Driven'] / (df_clean['Car_Age'] + 1)  # +1 чтобы избежать деления на 0
    
    # 6. Приведение типов
    df_clean['Year'] = df_clean['Year'].astype(int)
    df_clean['Kms_Driven'] = df_clean['Kms_Driven'].astype(int)
    
    # 7. Логирование результатов очистки
    logger.info(f"Исходный размер данных: {df.shape}")
    logger.info(f"Очищенный размер данных: {df_clean.shape}")
    logger.info(f"Удалено записей: {len(df) - len(df_clean)}")
    logger.info(f"Осталось записей: {len(df_clean)}")
    
    return df_clean

def main():
    parser = argparse.ArgumentParser(description='Подготовка данных о автомобилях')
    parser.add_argument('--s3-endpoint', default='https://storage.yandexcloud.net', help='S3 endpoint URL')
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket name')
    parser.add_argument('--input-key', required=True, help='S3 key для входных данных')
    parser.add_argument('--output-key', required=True, help='S3 key для выходных данных')
    parser.add_argument('--aws-access-key-id', required=True, help='AWS Access Key ID')
    parser.add_argument('--aws-secret-access-key', required=True, help='AWS Secret Access Key')
    
    args = parser.parse_args()
    
    try:
        # Настройка S3 клиента
        s3_client = setup_s3_client(
            args.s3_endpoint, 
            args.aws_access_key_id, 
            args.aws_secret_access_key
        )
        
        # Загрузка данных
        raw_df = load_data_from_s3(s3_client, args.s3_bucket, args.input_key)
        
        # Предобработка данных
        processed_df = preprocess_data(raw_df)
        
        # Сохранение обработанных данных
        save_data_to_s3(
            s3_client, 
            processed_df, 
            args.s3_bucket, 
            args.output_key, 
            format='parquet'
        )
        
        # Дополнительно: сохранение статистики обработки
        stats = {
            'processing_date': datetime.now().isoformat(),
            'original_records': len(raw_df),
            'processed_records': len(processed_df),
            'removed_records': len(raw_df) - len(processed_df),
            'columns': list(processed_df.columns)
        }
        
        stats_df = pd.DataFrame([stats])
        stats_key = args.output_key.replace('.parquet', '_stats.parquet')
        save_data_to_s3(s3_client, stats_df, args.s3_bucket, stats_key, format='parquet')
        
        logger.info("Обработка данных завершена успешно!")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()