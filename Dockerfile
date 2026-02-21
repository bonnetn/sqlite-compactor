# Base builder:
FROM debian:13-slim@sha256:f6e2cfac5cf956ea044b4bd75e6397b4372ad88fe00908045e9a0d21712ae3ba AS build
RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes python3-venv gcc libpython3-dev && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel

# Build the requirements.txt file:
FROM build AS requirements-builder
RUN /venv/bin/pip install poetry~=1.7.1
WORKDIR /app
COPY pyproject.toml poetry.lock /app/
RUN /venv/bin/python -m poetry export -f requirements.txt -o /requirements.txt

# Create the virtual environment:
FROM build AS build-venv
COPY --from=requirements-builder /requirements.txt /requirements.txt
RUN /venv/bin/pip install --disable-pip-version-check -r /requirements.txt

# Run python code:
FROM gcr.io/distroless/python3-debian13:latest
COPY --from=build-venv /venv /venv
WORKDIR /app
COPY sqlite-compactor/main.py sqlite-compactor/compactor.py sqlite-compactor/query_builder.py /app
RUN ["/venv/bin/python", "-c", "import duckdb; duckdb.sql('INSTALL sqlite;')"]
ENTRYPOINT ["/venv/bin/python", "-m", "main"]
