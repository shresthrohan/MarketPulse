FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONPATH=/srv
WORKDIR /srv
COPY processing/requirements.txt processing/requirements.lock /tmp/
RUN pip install -r /tmp/requirements.txt -c /tmp/requirements.lock && useradd --system --uid 10001 app
COPY config /srv/config
COPY processing/__init__.py processing/main.py processing/logging_setup.py processing/repository.py processing/cache.py /srv/processing/
COPY processing/consumers /srv/processing/consumers
COPY processing/validators /srv/processing/validators
COPY processing/transformers /srv/processing/transformers
COPY processing/aggregators /srv/processing/aggregators
USER app
CMD ["python", "-m", "processing.main"]

FROM runtime AS test
USER root
COPY processing/requirements-dev.txt /tmp/
RUN pip install -r /tmp/requirements-dev.txt -c /tmp/requirements.lock
# The simulator model is stdlib-only; tests check simulator output against the validator.
COPY ingestion/__init__.py /srv/ingestion/__init__.py
COPY ingestion/simulator /srv/ingestion/simulator
COPY processing/tests /srv/processing/tests
COPY scripts /srv/scripts
USER app
