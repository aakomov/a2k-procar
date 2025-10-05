
Очистка данных. Запуск airflow, spark-кластера и s3 в облаке
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra$ make apply
```

Локальный запуск mlflow и minio
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ cd infra-local/mlflow/
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/mlflow$ docker-compose up -d --build

(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/src$ cd ../infra-local/mlflow/
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/mlflow$ docker-compose down
```

Обучение модели
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/mlflow$ cd ../../src/
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/src$ python model_train_procar.py --n-estimators 50 --max-depth 7

python3 model_train_procar.py --model-name url_classifier
python model_train_procar.py --n-estimators 200 --max-depth 15
```

Работающий FastAPI
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ cd infra-local/a2k-docker-rest/
export PYTHONPATH=$(pwd)
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ python3 src/pipeline.py
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ uvicorn src.app:app --port 8000 --reload
```
Docker local
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ make build-prod
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ make run-prod

http://127.0.0.1:8000/
http://localhost:8888/tree
http://localhost:9091/login
http://localhost:5000/

```

Грузим образ на docker hub
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ docker login -u aakomov
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ docker tag a2k-procar:prod aakomov/a2k-procar:prod
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ docker push aakomov/a2k-procar:prod
```

k8s
```bash
notai@notaihost:~$ sudo snap install kubectl --classic


https://helm.sh/docs/intro/install/
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ chmod 700 get_helm.sh
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar$ ./get_helm.sh


notai@notaihost:~$ yc managed-kubernetes cluster get-credentials --id catphenfv8qti7f2fmhi --external
notai@notaihost:~$ export KUBE_CONFIG=~/.kube/config ("важно знать в каком контексте находимся")

(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ make helm-install-ingress
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl get all -n ingress-nginx

(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl apply -f k8s/namespace.yaml
#namespace/a2k-procar created
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl config set-context --current --namespace=a2k-procar
#Context "yc-k8s-cluster" modified.
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl apply -f k8s/deployment.yaml
#deployment.apps/a2k-procar created
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl apply -f k8s/service.yaml
#service/a2k-procar created
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl apply -f k8s/ingress.yaml
#ingress.networking.k8s.io/a2k-procar created
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl get pods
#NAME                          READY   STATUS    RESTARTS   AGE
#a2k-procar-7b5d6557bc-gjxgs   1/1     Running   0          51s


(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl delete -f k8s/ingress.yaml
#ingress.networking.k8s.io "a2k-procar" deleted from a2k-procar namespace
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl delete -f k8s/service.yaml
#service "a2k-procar" deleted from a2k-procar namespace
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl delete -f k8s/deployment.yaml
#deployment.apps "a2k-procar" deleted from a2k-procar namespace
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl delete -f k8s/namespace.yaml
#namespace "a2k-procar" deleted


(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl get all

(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ make helm-deploy

(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl get all -n a2k-procar

```


Prometheus
```bash

Перед этим запустить k8s
minikube start

helm list

(otus-ml-skel) notai@notaihost:~/otus/39/otus-ml-skel/service$ 
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack -n default

(otus-ml-skel) notai@notaihost:~/otus/39/otus-ml-skel/service$ kubectl --namespace default get secrets monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 -d ; echo
(otus-ml-skel) notai@notaihost:~/otus/39/otus-ml-skel/service$ kubectl --namespace default get pods
(otus-ml-skel) notai@notaihost:~/otus/39/otus-ml-skel$ helm uninstall monitoring -n default
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl apply -f k8s/monitoring-a2k-procar.yaml


kubectl apply -f monitoring-a2k-procar.yml
kubectl get servicemonitors -n monitoring

kubectl port-forward svc/monitoring-kube-prometheus-prometheus -n a2k-procar 9090:9090
http://localhost:9090
kubectl port-forward svc/monitoring-grafana -n a2k-procar 3000:80
http://localhost:3000
kubectl port-forward svc/a2k-procar -n a2k-procar 8000:80


(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ kubectl delete -f k8s/monitoring-a2k-procar.yaml
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ helm uninstall monitoring -n a2k-procar
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/a2k-docker-rest$ make helm-destroy


Войти (логин admin, пароль можно узнать:
kubectl get secret monitoring-grafana -n monitoring -o jsonpath="{.data.admin-password}" | base64 --decode)

curl -X POST "http://localhost:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{"Year":2018,"Present_Price":9.85,"Kms_Driven":40000,"Owner":0,"Car_Age":5,"Kms_Per_Year":8000,"Fuel_Type":"Petrol","Seller_Type":"Dealer","Transmission":"Manual"}'


### debug
## kubectl get pods -n a2k-procar
## kubectl logs -n a2k-procar deployment/a2k-procar
## kubectl logs -n a2k-procar <pod_name>
## kubectl get svc -n a2k-procar

## kubectl exec -it -n a2k-procar $(kubectl get pod -n a2k-procar -l app=a2k-procar -o name | head -1) -- sh
## apt update && apt install -y curl
## curl -v 127.0.0.1:8000/metrics

## minikube ssh docker images | grep aakomov
## inikube ssh docker rmi aakomov/a2k-procar:prod



```

http://127.0.0.1:8000/
http://127.0.0.1:8000/docs#/
http://127.0.0.1:8000/metrics
prometheus http://localhost:9090/
grafana http://localhost:3000/


KAFKA
```bash
(a2k-procar) notai@notaihost:~/otus/kp_a2k/a2k-procar/infra-local/kafka$ docker-compose -f docker-compose.yml up -d


python kafka_producer.py --bootstrap localhost:9092 --n 5
http://localhost:9003 → кластер → Topics → input_car_data → Messages.

python kafka_consumer.py --bootstrap localhost:9092 --input_topic input_car_data --output_topic predictions
UI → predictions → Messages.


```


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


