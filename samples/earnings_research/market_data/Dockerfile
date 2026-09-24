# Build from the directory that holds the config file (repo root internally, the sample
# root when exported):  docker build -f <pkg>/Dockerfile --build-arg PKG=<pkg> \
#     --build-arg CONFIG=<config file> [--build-arg CODE_VERSION=$(git rev-parse --short HEAD)] .
# ECR Public mirror: Docker Hub anonymous pulls get 429s from CI build IPs
FROM public.ecr.aws/docker/library/python:3.11-slim

ARG PKG=market_data
ARG CONFIG=config.json
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY ${PKG}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ${PKG}/*.py ./
COPY ${CONFIG} /config.json

ENTRYPOINT ["python", "main.py"]
