FROM python:3.10-slim

RUN mkdir /data
RUN chown app:app /data
VOLUME /data

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV POETRY_VIRTUALENVS_CREATE=false

RUN groupadd -g 999 app
RUN adduser --no-create-home --uid 999 --ingroup app app

USER root
WORKDIR /opt/app

RUN python -c "import urllib.request; urllib.request.urlretrieve('https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py', '/tmp/get-poetry.py')" && \
    python /tmp/get-poetry.py -y && \
            rm /tmp/get-poetry.py

COPY pyproject.toml poetry.lock /opt/app/
RUN /root/.poetry/bin/poetry install --no-dev --no-ansi --no-interaction

WORKDIR /app

copy main.py ./

USER app

CMD gunicorn main:app -w 1 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
