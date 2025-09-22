# Запуск:
### spark-submit --master yarn car_data_cleaning.py --bucket your-bucket-name
###
### Скрипт: car_data_cleaning.py
### Обрабатывает датасет "car data.csv" (Kaggle) в Spark и сохраняет в Parquet

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, count, expr
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime

def main():
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    input_path = f"s3a://{bucket_name}/input_data/*.csv"
    output_path = f"s3a://{bucket_name}/output_data/car_data.parquet"

    spark = SparkSession.builder \
        .appName("CarDataCleaning") \
        .getOrCreate()

    try:
        # Чтение с заголовком
        df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True,
            sep=','
        )

        print(f"Загружено строк: {df.count()}, колонок: {len(df.columns)}")

        # === Обработка данных ===

        # 1. Удаляем записи без Selling_Price
        df = df.filter(col("Selling_Price").isNotNull())

        # 2. Фильтрация выбросов по Selling_Price
        df = df.filter((col("Selling_Price") > 1.0) & (col("Selling_Price") < 10.0))

        # 3. Фильтрация выбросов по Kms_Driven
        df = df.filter((col("Kms_Driven") > 0) & (col("Kms_Driven") < 1000000))

        # 4. Заполнение пропусков в числовых колонках медианой
        numeric_cols = ["Year", "Kms_Driven"]
        for col_name in numeric_cols:
            median_val = df.approxQuantile(col_name, [0.5], 0.001)[0]
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull(), lit(median_val)).otherwise(col(col_name))
            )

        # 5. Заполнение пропусков в категориальных колонках модой
        categorical_cols = ["Fuel_Type", "Seller_Type", "Transmission", "Owner"]
        for col_name in categorical_cols:
            mode_val = (
                df.groupBy(col_name)
                  .agg(count("*").alias("cnt"))
                  .orderBy(col("cnt").desc())
                  .limit(1)
                  .collect()[0][0]
            )
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull(), lit(mode_val)).otherwise(col(col_name))
            )

        # 6. Валидация года выпуска
        current_year = datetime.now().year
        df = df.filter((col("Year") > 1950) & (col("Year") <= current_year))

        # 7. Feature engineering
        df = df.withColumn("Car_Age", lit(current_year) - col("Year"))
        df = df.withColumn("Kms_Per_Year", col("Kms_Driven") / (col("Car_Age") + lit(1)))

        # 8. Приведение типов
        df = df.withColumn("Year", col("Year").cast(IntegerType()))
        df = df.withColumn("Kms_Driven", col("Kms_Driven").cast(IntegerType()))
        df = df.withColumn("Selling_Price", col("Selling_Price").cast(DoubleType()))

        print(f"Очищенный датасет: {df.count()} строк, {len(df.columns)} колонок")

        # === Сохранение в Parquet ===
        print(f"Сохранение очищенных данных в {output_path}")
        df.write.parquet(output_path, mode="overwrite")
        print("Обработка данных успешно завершена!")

    except Exception as e:
        print(f"Ошибка: {str(e)}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    main()