# Build from the directory that holds the config file (repo root internally, the sample
# root when exported):  docker build -f <pkg>/Dockerfile --build-arg PKG=<pkg> \
#     --build-arg CONFIG=<config file> [--build-arg CODE_VERSION=$(git rev-parse --short HEAD)] .
FROM public.ecr.aws/docker/library/python:3.11-slim
ARG PKG=sentiment_analysis
ARG CONFIG=config.json
ARG CODE_VERSION=dev
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY ${PKG}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ${PKG}/ ./
COPY ${CONFIG} /config.json
RUN echo "${CODE_VERSION}" > CODE_VERSION
ENTRYPOINT ["python", "evaluate.py"]
