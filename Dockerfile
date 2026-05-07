FROM mcr.microsoft.com/playwright/python:v1.53.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    fluxbox \
    novnc \
    websockify \
    x11vnc \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
COPY web/requirements_web.txt /tmp/requirements_web.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements_web.txt

COPY . /app

RUN chmod +x /app/docker/entrypoint-web.sh \
    /app/docker/entrypoint-scheduler.sh \
    /app/docker/entrypoint-runner.sh
