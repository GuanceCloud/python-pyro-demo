#!/usr/bin/env bash
# gunicorn -w 1 --bind 0.0.0.0:8080 app:app
OTEL_SERVICE_NAME="python-pyro-demo" OTEL_RESOURCE_ATTRIBUTES="service.version=v3.5.7,service.env=dev,host=$(hostname)" OTEL_TRACES_EXPORTER=otlp OTEL_EXPORTER_OTLP_PROTOCOL=grpc OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4317" PYROSCOPE_APPLICATION_NAME="python-pyro-demo" python app.py