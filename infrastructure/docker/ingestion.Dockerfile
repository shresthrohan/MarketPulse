FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONPATH=/srv
WORKDIR /srv
COPY ingestion/requirements.txt ingestion/requirements.lock /tmp/
RUN pip install -r /tmp/requirements.txt -c /tmp/requirements.lock && useradd --system --uid 10001 app
COPY config /srv/config
COPY ingestion/__init__.py ingestion/main.py ingestion/logging_setup.py /srv/ingestion/
COPY ingestion/providers /srv/ingestion/providers
COPY ingestion/producers /srv/ingestion/producers
COPY ingestion/simulator /srv/ingestion/simulator
USER app
CMD ["python", "-m", "ingestion.main"]

FROM runtime AS test
USER root
COPY ingestion/requirements-dev.txt /tmp/
RUN pip install -r /tmp/requirements-dev.txt -c /tmp/requirements.lock
COPY ingestion/tests /srv/ingestion/tests
USER app
