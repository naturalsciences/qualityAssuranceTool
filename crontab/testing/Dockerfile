FROM python:3.12-slim AS builder
ENV TZ="Europe/Brussels"


# install minimal extras
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY main.py /app/main.py

ENTRYPOINT ["python", "/app/main.py"]
