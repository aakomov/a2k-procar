
# Запуск
### python upload_fraud_data_cleaning.py --input-path ./input.csv --output-path ./output/
### or
### spark-submit --master yarn upload_fraud_data_cleaning.py --input-path "hdfs://rc1a-dataproc-m-i1n0pn1s7u5716jb.mdb.yandexcloud.net/user/ubuntu/data/*.txt" --output-path "s3a://a2k-otus-bucket-b1g63h8ugecsfua7istl/cleaned_data/"

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
import argparse

def main():
    ## # Парсинг аргументов командной строки
    ## parser = argparse.ArgumentParser(description='Process fraud transactions dataset')
    ## parser.add_argument('--input-path', type=str, required=True, help='Input path for raw data')
    ## parser.add_argument('--output-path', type=str, required=True, help='Output path for cleaned data')
    ## args = parser.parse_args()

    """Main function to execute the PySpark job"""
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set")

    input_path = f"s3a://{bucket_name}/input_data/*.csv"
    output_path = f"s3a://{bucket_name}/output_data/sum_data.parquet"

    # Создание Spark сессии
    spark = SparkSession.builder \
        .appName("FraudTransactionsCleaning") \
        .getOrCreate()

    try:
        # Чтение данных
        df = spark.read.csv(
               input_path,
               header=False,
               sep=',',          # явно указываем разделитель
               comment='#'       # пропускаем строки, начинающиеся с #
            )
        
        # Переименование колонок (так как в исходном файле нет заголовка)
        columns = [
                "tranaction_id", "tx_datetime", "customer_id", "terminal_id",
                "tx_amount", "tx_time_seconds", "tx_time_days", "tx_fraud", "tx_fraud_scenario"
            ]
        
        if len(df.columns) != len(columns):
            raise ValueError(f"Ожидалось {len(columns)} колонок, а получено {len(df.columns)}")
        
        for i, col_name in enumerate(columns):
            df = df.withColumnRenamed(f"_c{i}", col_name)
        
        # Удаление дубликатов
        initial_count = df.count()
        df = df.dropDuplicates()
        deduped_count = df.count()
        print(f"Удалено дубликатов: {initial_count - deduped_count}")

        # Анализ пропущенных значений
        print("\nАнализ пропущенных значений:")
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                print(f"{column}: {null_count} пропусков ({null_count / deduped_count * 100:.2f}%)")
        
        # Анализ нулевых значений в ключевых столбцах
        print("\nАнализ нулевых значений:")
        for column in ['terminal_id', 'customer_id']:
            zero_count = df.filter(col(column) == 0).count()
            print(f"{column}: {zero_count} нулевых значений")

        # Очистка данных
        print("\nОчистка данных...")
        df_clean = df.filter(
            (col('terminal_id') != 0) & 
            (col('customer_id') != 0) 
        )

        # Проверка результатов очистки
        clean_count = df_clean.count()
        print("\nРезультаты очистки:")
        print(f"Исходный размер датасета: {deduped_count} строк")
        print(f"Очищенный размер датасета: {clean_count} строк")
        print(f"Удалено строк: {deduped_count - clean_count}")

        # Создаем новую колонку tx_date, которая содержит только дату без времени
        df_clean = df_clean.withColumn("tx_date", to_date("tx_datetime"))

        # Сохранение в Parquet
        print(f"\nСохранение очищенных данных в {output_path}")
        df_clean.write.partitionBy("tx_date").parquet(output_path, mode='overwrite')
        print("Обработка данных успешно завершена!")
    
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()