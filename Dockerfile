# Base builder:
FROM debian:13-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd AS build
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
FROM gcr.io/distroless/python3-debian13:latest@sha256:393cdf69ec7a5e217f837f2ff9b2123e06545d89c6e718c14ad020451fcb1900
COPY --from=build-venv /venv /venv
WORKDIR /app
COPY sqlite-compactor/main.py sqlite-compactor/compactor.py sqlite-compactor/query_builder.py /app
RUN ["/venv/bin/python", "-c", "import duckdb; duckdb.sql('INSTALL sqlite;')"]
ENTRYPOINT ["/venv/bin/python", "-m", "main"]
