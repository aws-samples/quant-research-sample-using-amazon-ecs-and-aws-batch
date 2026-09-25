# One image for every Batch package of this sample, built by the infrastructure's
# push-triggered image build (build context = this directory):
#   docker build --build-arg CODE_VERSION=$(git rev-parse HEAD | cut -c 1-12) .
# Packages live under /app/<package>/ with config.json at /app; the dispatcher runs
#   run <package> <command|script.py> [args...]
# ECR Public mirror: Docker Hub anonymous pulls get 429s from CI build IPs
FROM public.ecr.aws/docker/library/python:3.11-slim

ARG CODE_VERSION=dev
ENV PYTHONUNBUFFERED=1 MPLBACKEND=Agg

WORKDIR /app

COPY basket_study/requirements.txt requirements/basket_study.txt
COPY market_data/requirements.txt requirements/market_data.txt
COPY content_pipeline/requirements.txt requirements/content_pipeline.txt
COPY sentiment_analysis/requirements.txt requirements/sentiment_analysis.txt
RUN pip install --no-cache-dir \
        -r requirements/basket_study.txt \
        -r requirements/market_data.txt \
        -r requirements/content_pipeline.txt \
        -r requirements/sentiment_analysis.txt

# Headed Chromium for the content pipeline's browser backfill: the browser, its OS
# dependencies, and xvfb for a virtual display (started by the dispatcher).
RUN apt-get update \
    && apt-get install -y --no-install-recommends xvfb xauth \
    && python -m playwright install-deps chromium \
    && python -m playwright install chromium \
    && rm -rf /var/lib/apt/lists/*

COPY config.json ./
COPY basket_study/ basket_study/
COPY market_data/ market_data/
COPY content_pipeline/ content_pipeline/
COPY sentiment_analysis/ sentiment_analysis/
COPY run.sh /usr/local/bin/run

# spec §5 code-version gate: every package carries the SHA the image was built from
RUN chmod +x /usr/local/bin/run \
    && for pkg in basket_study market_data content_pipeline sentiment_analysis; do \
           echo "${CODE_VERSION}" > "${pkg}/CODE_VERSION"; \
       done

ENTRYPOINT ["run"]
CMD ["help"]
