# Base image pinned by digest: rebuilding the same commit gives the same OS layer.
FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/srv:/srv/backend
WORKDIR /srv/backend
COPY backend/requirements.txt backend/requirements.lock /tmp/
# requirements.lock pins every transitive dependency to the versions the test suite ran against.
RUN pip install -r /tmp/requirements.txt -c /tmp/requirements.lock && useradd --system --uid 10001 app
COPY config /srv/config
COPY ingestion/__init__.py /srv/ingestion/__init__.py
COPY ingestion/simulator /srv/ingestion/simulator
COPY scripts/seed_data.py /srv/scripts/seed_data.py
COPY backend/app /srv/backend/app
COPY backend/alembic /srv/backend/alembic
COPY backend/alembic.ini /srv/backend/alembic.ini
USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--ws-ping-interval", "20", "--timeout-graceful-shutdown", "10"]

# Test image = runtime + test tooling and tests; what CI and `docker compose exec backend pytest` use.
FROM runtime AS test
USER root
COPY backend/requirements-dev.txt /tmp/
RUN pip install -r /tmp/requirements-dev.txt -c /tmp/requirements.lock
COPY backend/tests /srv/backend/tests
COPY scripts /srv/scripts
USER app
