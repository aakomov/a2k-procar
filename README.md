

В Airflow DAG add for run data_preprocessing.py:

```
# в DAG файл
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

# В Jupyter
%run test_preprocessing_local.py

# Локальный запуск в spark
spark-submit --master local[2] car_data_cleaning.py


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


# Системные утилиты
sudo apt update && sudo apt install -y docker.io docker-compose openjdk-11-jdk python3 python3-venv python3-pip git curl

# Docker-права
sudo usermod -aG docker $USER
# выйдите и зайдите заново или newgrp docker

# k3d (kubernetes lightweight)
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash

# venv для проекта
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip



# MLOps Car Price Prediction Project

## Запуск проекта локально

Ниже пошаговые команды, которые нужно выполнить для запуска проекта.

### 1. Установка зависимостей
```bash
sudo apt update
sudo apt install docker.io docker-compose python3-pip -y
pip install -r requirements.txt
```

```bash
uv sync
```


### 2. Запуск MinIO (локальное S3)
```bash
cd infra/minio
docker-compose up -d
```

Доступ:
- UI: http://localhost:9001
- Логин: minioadmin
- Пароль: minioadmin

Создайте bucket: `car-price`

### 3. Запуск Airflow
```bash
cd infra/airflow
docker-compose up -d
```
#### Запуск Airflow + API
```bash
docker-compose -f docker-compose.airflow.yml up -d
```

UI: http://localhost:8080  
Логин: airflow / Пароль: airflow

### 4. Запуск MLflow
```bash
cd infra/mlflow
docker-compose up -d
```
#### Запуск MLflow окружения
```bash
docker-compose -f docker-compose.mlflow.yml up -d
```


UI: http://localhost:5000

### 5. Запуск Kafka
```bash
cd infra/kafka
docker-compose up -d
```

### 6. Запуск Prometheus & Grafana
```bash
cd infra/prometheus && docker-compose up -d
cd ../grafana && docker-compose up -d
```

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

### 7. Запуск Feast
```bash
cd infra/feast
feast init feature_repo
```

### 8. Локальное обучение модели
```bash
python scripts/train.py
```

### 9. Запуск REST API модели
```bash
docker build -t car-price-api .
docker run -p 8000:8000 car-price-api
```
#### Проверка API
```bash
curl -X POST "http://localhost:8000/predict" -H "Content-Type: application/json" -d '{"mileage": 30000, "year": 2020}'


API доступно на http://localhost:8000

---

## Структура проекта

```
mlops_car_price_project/
├── data/
│   ├── raw/                 # Сырые данные
│   └── processed/           # Обработанные данные
├── dags/                    # DAGs для Airflow
├── scripts/                 # Python-скрипты (train.py, preprocess.py и т.д.)
├── models/                  # Обученные модели
├── notebooks/               # Jupyter notebooks
├── infra/
│   ├── minio/               # docker-compose для MinIO
│   ├── airflow/             # docker-compose для Airflow
│   ├── mlflow/              # docker-compose для MLflow
│   ├── kafka/               # docker-compose для Kafka
│   ├── prometheus/          # docker-compose для Prometheus
│   ├── grafana/             # docker-compose для Grafana
│   └── feast/               # конфиги Feast
├── k8s/                     # Манифесты Kubernetes
├── .github/workflows/       # CI/CD пайплайны
└── requirements.txt         # Python зависимости
```


