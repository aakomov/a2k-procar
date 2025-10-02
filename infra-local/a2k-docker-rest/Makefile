build:
	docker build -t a2k-procar:latest .

run-simple:
	docker run --rm --name a2k-procar a2k-procar:latest

run-it:
	docker run -it --rm --name a2k-procar a2k-procar:latest bash

run-exec:
	docker exec -it a2k-procar bash

build-jupyter:
	docker build -f Dockerfile.jupyter -t a2k-procar:jupyter .

run-jupyter:
	docker run --rm --name a2k-procar-jupyter -p 8888:8888 a2k-procar:jupyter

run-jupyter-with-volume:
	docker run --rm -p 8888:8888 -v ./notebooks:/app/notebooks --name a2k-procar-jupyter a2k-procar:jupyter

build-dev:
	docker build -t a2k-procar:dev -f Dockerfile.dev .

run-dev:
	docker run \
		--rm \
		--name a2k-procar-dev \
		-p 8000:8000 \
		-v ./src:/app/src \
		-v ./models:/app/models \
		a2k-procar:dev

up:
	docker compose down || true
	docker compose up --build

build-prod:
	docker build -t a2k-procar:0.0.1 -f Dockerfile.prod .

run-prod:
	docker run \
		--rm \
		--name a2k-procar-prod \
		-p 8000:8000 \
		a2k-procar:prod

push-prod:
	docker tag a2k-procar:0.0.1 aakomov/a2k-procar:0.0.1
	docker push aakomov/a2k-procar:0.0.1

helm-install-ingress:
	helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
	helm repo update
	helm install ingress-nginx ingress-nginx/ingress-nginx \
		--namespace ingress-nginx \
		--create-namespace \
		--set controller.replicaCount=1 \
		--set controller.nodeSelector."kubernetes\.io/os"=linux \
		--set controller.admissionWebhooks.enabled=false \
		--set controller.service.type=LoadBalancer

helm-uninstall-ingress:
	helm uninstall ingress-nginx -n ingress-nginx

helm-deploy:
	helm upgrade --install a2k-procar helm/a2k-procar \
		--namespace a2k-procar \
		--create-namespace \
		--set image.tag=prod

helm-destroy:
	helm uninstall a2k-procar -n a2k-procar
