# Base builder:
FROM debian:12-slim@sha256:d02c76d82364cedca16ba3ed6f9102406fa9fa8833076a609cabf14270f43dfc AS build
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
FROM gcr.io/distroless/python3-debian12:latest@sha256:d1427d962660c43d476b11f9bb7d6df66001296bba9577e39b33d2e8897614cd
COPY --from=build-venv /venv /venv
WORKDIR /app
COPY sqlite-compactor/main.py sqlite-compactor/compactor.py /app
ENTRYPOINT ["/venv/bin/python", "-m", "main"]
