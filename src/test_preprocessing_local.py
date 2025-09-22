# test_preprocessing_local.py
import pandas as pd
import numpy as np
from datetime import datetime
import os
from data_preprocessing_procar import preprocess_data  # Импортируем функцию предобработки

def test_preprocessing_local():
    """Тестирование скрипта предобработки на локальном файле"""
    
    # Создаем папку output если ее нет
    # os.makedirs('output_data', exist_ok=True)
    
    # 1. Загрузка локального файла
    input_file = '../data/input_data/car data.csv'
    output_file = '../data/output_data/car_data_cleaned.parquet'
    stats_file = '../data/output_data/processing_stats.parquet'
    
    print(f"Загрузка данных из файла: {input_file}")
    
    try:
        df = pd.read_csv(input_file)
        print(f"Данные загружены. Размер: {df.shape}")
        print("Первые 5 строк исходных данных:")
        print(df.head())
        print("\nИнформация о данных:")
        print(df.info())
        print("\nПропущенные значения:")
        print(df.isnull().sum())
        
    except FileNotFoundError:
        print(f"❌ Файл {input_file} не найден!")
        print("Убедитесь, что файл находится в той же директории, что и скрипт")
        return
    
    # 2. Обработка данных
    print("\n" + "="*50)
    print("Начало обработки данных...")
    
    try:
        cleaned_df = preprocess_data(df)
        
        print("Обработка завершена успешно!")
        print(f"Размер очищенных данных: {cleaned_df.shape}")
        print(f"Удалено записей: {len(df) - len(cleaned_df)}")
        
        # 3. Сохранение в Parquet
        print("\nСохранение результатов в Parquet...")
        cleaned_df.to_parquet(output_file, index=False, engine='pyarrow')
        print(f"✅ Данные сохранены в: {output_file}")
        
        # 4. Сохранение статистики обработки
        stats_data = {
            'processing_date': [datetime.now().isoformat()],
            'original_records': [len(df)],
            'processed_records': [len(cleaned_df)],
            'removed_records': [len(df) - len(cleaned_df)],
            'input_file': [input_file],
            'output_file': [output_file]
        }
        
        stats_df = pd.DataFrame(stats_data)
        stats_df.to_parquet(stats_file, index=False, engine='pyarrow')
        print(f"✅ Статистика сохранена в: {stats_file}")
        
        # 5. Чтение и проверка сохраненного файла
        print("\n" + "="*50)
        print("Проверка сохраненного Parquet файла...")
        
        # Чтение Parquet файла
        read_df = pd.read_parquet(output_file)
        
        print(f"✅ Файл прочитан успешно. Размер: {read_df.shape}")
        print("\nПервые 5 строк обработанных данных:")
        print(read_df.head())
        
        print("\nИнформация об обработанных данных:")
        print(read_df.info())
        
        print("\nПропущенные значения в обработанных данных:")
        print(read_df.isnull().sum())
        
        print("\nСтатистика числовых признаков:")
        print(read_df.describe())
        
        # Проверка созданных признаков
        new_features = [col for col in read_df.columns if col not in df.columns]
        if new_features:
            print(f"\nСозданные новые признаки: {new_features}")
            print("Статистика новых признаков:")
            for feature in new_features:
                if feature in read_df.columns:
                    print(f"{feature}:")
                    print(read_df[feature].describe())
                    print()
        
        # Проверка, что нет пропусков в ключевых полях
        key_columns = ['Selling_Price', 'Year', 'Kms_Driven', 'Fuel_Type']
        for col in key_columns:
            if col in read_df.columns:
                missing = read_df[col].isnull().sum()
                print(f"Пропуски в '{col}': {missing}")
                if missing > 0:
                    print(f"❌ ВНИМАНИЕ: Найдены пропуски в {col}!")
        
        print("\n" + "="*50)
        print("✅ Тестирование завершено успешно!")
        print(f"Исходные данные: {df.shape}")
        print(f"Очищенные данные: {read_df.shape}")
        print(f"Файл результатов: {output_file}")
        
    except Exception as e:
        print(f"❌ Ошибка при обработке данных: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_preprocessing_local()