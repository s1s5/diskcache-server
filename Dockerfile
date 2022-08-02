# ---------- build ----------
FROM python:3.10 AS base

FROM base AS builder

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PATH="$PATH:/root/.poetry/bin:/runtime/bin" \
    PYTHONPATH="$PYTHONPATH:/runtime/lib/python3.10/site-packages" \
    POETRY_VERSION=1.1.13

WORKDIR /opt

COPY pyproject.toml poetry.lock ./
RUN pip install poetry==${POETRY_VERSION}
RUN poetry export --without-hashes --no-interaction --no-ansi -f requirements.txt -o requirements.txt
RUN pip install --prefix=/runtime --force-reinstall -r requirements.txt

# ---------- runtime ----------
FROM python:3.10-slim as runtime

WORKDIR /usr/src/app
RUN groupadd -g 999 app && \
    useradd -d /usr/src/app -s /bin/bash -u 999 -g 999 app

COPY --from=builder /runtime /usr/local
COPY main.py ./

USER app
CMD gunicorn main:app -b 0.0.0.0:8000 -w 1 -k uvicorn.workers.UvicornWorker --max-requests 10000 --graceful-timeout 180
