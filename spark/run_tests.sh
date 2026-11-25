#!/bin/sh
# Runs spark/tests inside apache/spark:3.5.3-python3, from the repo root:
#   docker run --rm -v "$PWD/spark:/opt/marketpulse" -w /opt/marketpulse apache/spark:3.5.3-python3 sh run_tests.sh
set -eu

# The image has no pytest and its `spark` user has no home directory, so install into a throwaway target.
python3 -m pip install --quiet --no-cache-dir --disable-pip-version-check --target /tmp/pylib pytest==8.3.4

PY4J=$(ls /opt/spark/python/lib/py4j-*-src.zip)
export PYTHONPATH="/tmp/pylib:/opt/spark/python:$PY4J:$PWD"
export PYTHONDONTWRITEBYTECODE=1  # keep the bind-mounted source tree clean

exec python3 -m pytest -p no:cacheprovider -q tests "$@"
