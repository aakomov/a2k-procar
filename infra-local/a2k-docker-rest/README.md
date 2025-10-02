# OTUS Model As a Service

Установка:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Ветка docker:
```bash
git checkout -t origin/docker
```

docker build -t otus-maas:latest .
docker run --name otus-maas otus-maas:latest
docker rm

docker exec -it

(.venv) notai@notaihost:~/otus/36/a2k-model-as-a-service$ export PYTHONPATH=$(pwd)
(.venv) notai@notaihost:~/otus/36/a2k-model-as-a-service$ python3 src/pipeline.py

(.venv) notai@notaihost:~/otus/36/a2k-model-as-a-service$ uvicorn src.app:app --port 8000 --reload


docker login -u aakomov

docker tag otus-maas:prod aakomov/otus-maas:prod
docker push aakomov/otus-maas:prod

осталость только CICD на 1:40, как понимаю применимо для развернутой машины

## План практики

* Установим docker на vm
* Напишем простой Dockerfile
* Запустим простой контейнер
* Подключимся внутрь контейнера
* Запустим jupyter notebook в контейнере и подключимся к нему
* Написать dev/prod Dockerfile и docker-compose для проекта c использованием FastAPI
* Развернуть/протестировать ML модель в контейнере с использованием FastAPI
