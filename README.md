

В Airflow DAG add for run data_preprocessing.py:

```
# В вашем DAG файле
preprocess_task = BashOperator(
    task_id='preprocess_data',
    bash_command='''
        python /path/to/data_preprocessing.py \
          --s3-bucket="ml-project-cars-bucket" \
          --input-key="raw/car_data.csv" \
          --output-key="processed/car_data_cleaned_{{ ds }}.parquet" \
          --aws-access-key-id="{{ var.value.AWS_ACCESS_KEY }}" \
          --aws-secret-access-key="{{ var.value.AWS_SECRET_KEY }}"
    ''',
    dag=dag,
)
```

Run local python test_preprocessing_local.py
# В Jupyter клетке
%run test_preprocessing_local.py



Загрузка переменных
![Схема1](./img/otus%20airflow.JPG)

Размещение разработанного скрипта fraud_data_cleaning.py в бакете
![Схема4](./img/otus%20airflow4.JPG)

Использования разработанного скрипта fraud_data_cleaning.py в даге
![Схема2](./img/otus%20airflow2.JPG)

Успешное выполнение пайплайна
![Схема3](./img/otus%20airflow3.JPG)

Результат выполнения пайплайна. Создана структура в бакете
![Схема5](./img/otus%20airflow5.JPG)

Успешное выполнение fraud_data_cleaning.py из дага
![Схема6](./img/otus%20airflow6.JPG)

