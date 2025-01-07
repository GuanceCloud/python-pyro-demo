import datetime
import functools
import gzip
import json
import logging
import os
import re
import socket
import uuid
from typing import List, Optional, TypeVar, Callable

import pyroscope
from flask import Flask, request, jsonify
from opentelemetry import trace
from opentelemetry.sdk.resources import (Resource)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, ConsoleSpanExporter
from pyroscope.otel import PyroscopeSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

exporter = OTLPSpanExporter(endpoint='http://127.0.0.1:4317', insecure=True, timeout=30)

UUID = uuid.uuid1()

provider = TracerProvider(resource=Resource(attributes={
    "service.name": "python-pyro-demo",
    "service.version": "v3.5.7",
    "service.env": "dev",
    "host": socket.gethostname(),
    "process_id": os.getpid(),
    "runtime_id": str(UUID),
}))

provider.add_span_processor(PyroscopeSpanProcessor())
provider.add_span_processor(SimpleSpanProcessor(span_exporter=ConsoleSpanExporter()))
provider.add_span_processor(BatchSpanProcessor(span_exporter=exporter, max_queue_size=100, max_export_batch_size=30))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("python-pyro-demo")

I = TypeVar('I')
O = TypeVar('O')

pyroscope.configure(
    app_name="python-pyro-demo",
    application_name="python-pyro-demo",
    server_address="http://localhost:9529",
    detect_subprocesses=True,
    oncpu=True,
    enable_logging=True,
    report_pid=True,
    report_thread_id=True,
    report_thread_name=True,
    tags={
        "host": socket.gethostname(),
        "service": 'python-pyro-demo',
        "version": 'v0.2.3',
        "env": "testing",
        "process_id": os.getpid(),
        "runtime_id": str(UUID),
    }
)

FORMAT = '%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] - %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger(__name__)
log.level = logging.DEBUG


def convert_or_none(v: Optional[I], converter: Callable[[I], O]) -> Optional[O]:
    if v is None:
        return v

    return converter(v)


class Movie:
    def __init__(self, d: dict):
        self.__d = d

    @property
    def title(self) -> Optional[str]:
        return convert_or_none(self.__d.get("title"), str)

    @property
    def rating(self) -> Optional[float]:
        return convert_or_none(self.__d.get("vote_average"), float)

    @property
    def release_date(self) -> Optional[str]:
        return convert_or_none(self.__d.get("release_date"), str)

    def to_dict(self):
        return {
            "title": self.title,
            "rating": self.rating,
            "release_date": self.release_date,
        }


SERVER_DIR = os.path.dirname(os.path.realpath(__file__))
CACHED_MOVIES: Optional[List[Movie]] = None

app = Flask(__name__)


def main():
    app.run(host="0.0.0.0", port=8080, debug=False, threaded=True)


@app.route('/movies')
@tracer.start_as_current_span("movies")
def movies():
    log.info("/movies receive request")

    query: str = request.args.get("q", request.args.get("query"))

    movies_list = get_movies()

    num = 37
    fib = fibonacci(num)
    log.info("fibonacci(%d) = %d", num, fib)

    num = 36
    fib = fibonacci(num)
    log.info("fibonacci(%d) = %d", num, fib)

    # Problem: We are sorting over the entire list but might be filtering most of it out later.
    # Solution: Sort after filtering
    movies_list = sort_desc_release_date(movies_list)

    if query:
        movies_list = [m for m in movies_list if re.search(query.upper(), m.title.upper())]

    return jsonify([m.to_dict() for m in movies_list])


def fibonacci(num: int):
    if num <= 2:
        return 1
    if num % 29 == 0:
        return fibonacci_with_tracing(num-1) + fibonacci(num-2)
    return fibonacci(num - 1) + fibonacci(num - 2)


@tracer.start_as_current_span("fibonacci")
def fibonacci_with_tracing(num: int):
    if num <= 2:
        return 1
    return fibonacci(num-1) + fibonacci(num - 2)


@tracer.start_as_current_span("sort_desc_release_date")
def sort_desc_release_date(movies_list: List[Movie]) -> List[Movie]:
    # Problem: We are parsing a datetime for each comparison during sort
    # Example Solution:
    #   Since date is in isoformat (yyyy-mm-dd) already, that one sorts nicely with normal string sorting
    #   `return sorted(movies, key=lambda m: m.release_date, reverse=True)`
    def sorting_cmp(m1: Movie, m2: Movie) -> int:
        try:
            m1_dt = datetime.date.fromisoformat(m1.release_date)
        except Exception:
            m1_dt = datetime.date.min
        try:
            m2_dt = datetime.date.fromisoformat(m2.release_date)
        except Exception:
            m2_dt = datetime.date.min
        return int((m1_dt - m2_dt).total_seconds())

    return sorted(movies_list, key=functools.cmp_to_key(sorting_cmp), reverse=True)


def get_movies() -> List[Movie]:
    return load_movies()


@tracer.start_as_current_span("load_movies")
def load_movies():
    with gzip.open(os.path.join(SERVER_DIR, "./movies5000.json.gz")) as f:
        movies_list = [Movie(d) for d in json.load(f)]
        return movies_list


if __name__ == '__main__':
    main()
