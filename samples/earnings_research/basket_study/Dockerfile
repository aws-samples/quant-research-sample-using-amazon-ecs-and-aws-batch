# Build from the directory that holds the config file (repo root internally, the sample
# root when exported):  docker build -f <pkg>/Dockerfile --build-arg PKG=<pkg> \
#     --build-arg CONFIG=<config file> [--build-arg CODE_VERSION=$(git rev-parse --short HEAD)] .
# ECR Public mirror: Docker Hub anonymous pulls get 429s from CI build IPs
FROM public.ecr.aws/docker/library/python:3.11-slim

ARG PKG=basket_study
ARG CONFIG=config.json
ARG CODE_VERSION=dev
ENV PYTHONUNBUFFERED=1 MPLBACKEND=Agg

WORKDIR /app

COPY ${PKG}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ${PKG}/*.py ./
COPY ${PKG}/rules/ rules/
COPY ${PKG}/construction/ construction/
COPY ${PKG}/configs/ configs/
COPY ${CONFIG} /config.json
# spec §5 code-version gate: the image carries the SHA it was built from
RUN echo "${CODE_VERSION}" > CODE_VERSION

ENTRYPOINT ["python", "evaluate.py"]