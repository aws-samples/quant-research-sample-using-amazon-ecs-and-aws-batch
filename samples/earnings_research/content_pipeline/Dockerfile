# Build from the directory that holds the config file (repo root internally, the sample
# root when exported):  docker build -f <pkg>/Dockerfile --build-arg PKG=<pkg> \
#     --build-arg CONFIG=<config file> [--build-arg CODE_VERSION=$(git rev-parse --short HEAD)] .
# ECR Public mirror: Docker Hub anonymous pulls get 429s from CI build IPs
FROM public.ecr.aws/docker/library/python:3.11-slim

ARG PKG=content_pipeline
ARG CONFIG=config.json
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY ${PKG}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Headed Chromium for the backfill's browser strategy: install the browser and
# its OS dependencies, plus xvfb to give it a virtual display on Fargate.
# (playwright install-deps pulls the shared libs Chromium needs.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends xvfb xauth \
    && python -m playwright install-deps chromium \
    && python -m playwright install chromium \
    && rm -rf /var/lib/apt/lists/*

COPY ${PKG}/*.py ./
COPY ${PKG}/config*.json ./
COPY ${PKG}/entrypoint.sh ./
COPY ${CONFIG} /config.json
RUN chmod +x entrypoint.sh

# entrypoint wraps the app in xvfb-run so headless=False works without a
# physical display; primary (non-browser) commands are unaffected.
ENTRYPOINT ["./entrypoint.sh"]
