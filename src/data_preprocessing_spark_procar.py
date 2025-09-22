from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, count, first
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime

def main():
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket
    
    input_path = f"s3a://{bucket_name}/input_data/car data.csv"
    output_path = f"s3a://{bucket_name}/output_data/car_data_cls.parquet"

    #input_path = "/home/notai/otus/kp_a2k/a2k-procar/data/input_data/car data.csv"
    #output_path = "/home/notai/otus/kp_a2k/a2k-procar/data/output_data/car_data_cls.parquet"

    spark = SparkSession.builder \
        .appName("CarDataCleaning") \
        .getOrCreate()

    try:
        # Чтение с заголовком
        df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True
        )

        print(f"Загружено: {df.count()} строк")

        # 1. Удаляем записи без Selling_Price
        df = df.filter(col("Selling_Price").isNotNull())

        # 2. Фильтрация выбросов
        df = df.filter(
            (col("Selling_Price") > 1.00) & 
            (col("Selling_Price") < 10.00) &
            (col("Kms_Driven") > 0) & 
            (col("Kms_Driven") < 1000000)
        )

        # 3. Заполнение пропусков в числовых колонках (средним)
        numeric_cols = ["Year", "Kms_Driven", "Present_Price"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                mean_val = df.agg(avg(col_name)).first()[0] or 0
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), lit(mean_val)).otherwise(col(col_name))
                )

        # 4. Заполнение пропусков в категориальных колонках
        categorical_cols = ["Fuel_Type", "Seller_Type", "Transmission", "Owner"]
        for col_name in categorical_cols:
            if col_name in df.columns:
                mode_row = (
                    df.groupBy(col_name)
                      .agg(count("*").alias("cnt"))
                      .orderBy(col("cnt").desc())
                      .first()
                )
                mode_val = mode_row[0] if mode_row else "unknown"
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), lit(mode_val)).otherwise(col(col_name))
                )

        # 5. Валидация года выпуска
        current_year = datetime.now().year
        df = df.filter((col("Year") > 1950) & (col("Year") <= current_year))

        # 6. Feature engineering
        df = df.withColumn("Car_Age", lit(current_year) - col("Year"))
        df = df.withColumn("Kms_Per_Year", 
                          when(col("Car_Age") > 0, col("Kms_Driven") / col("Car_Age"))
                          .otherwise(col("Kms_Driven")))

        # 7. Приведение типов
        type_casts = {
            "Year": IntegerType(),
            "Kms_Driven": IntegerType(), 
            "Selling_Price": DoubleType(),
            "Present_Price": DoubleType(),
            "Car_Age": IntegerType()
        }
        
        for col_name, dtype in type_casts.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(dtype))

        print(f"Очищено: {df.count()} строк")

        # 8. Сохранение
        df.write \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .parquet(output_path)

        print("✅ Данные успешно сохранены!")

    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    main()