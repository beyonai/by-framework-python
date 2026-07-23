# syntax=docker/dockerfile:1
#
# Template Dockerfile for a by-framework Worker. This installs the published
# `by-framework` package from PyPI and expects YOUR worker code (the module
# containing your GatewayWorker subclass) to sit next to this Dockerfile in
# the build context - copy this file into your own project and adjust the
# COPY line and the default --worker-class below.
#
# Build:  docker build -f deploy/Dockerfile -t my-agent-worker .
# Run:    docker run --rm -e REDIS_HOST=redis my-agent-worker \
#           --worker-class my_agent.MyAgent

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /app

# Pin this in production (e.g. --build-arg BY_FRAMEWORK_VERSION=0.2.2).
ARG BY_FRAMEWORK_VERSION=""
RUN uv venv /opt/venv \
    && uv pip install --python /opt/venv/bin/python \
        "by-framework${BY_FRAMEWORK_VERSION:+==${BY_FRAMEWORK_VERSION}}"

# Replace this with your own worker package/module.
COPY . /app/worker
ENV PYTHONPATH=/app/worker

COPY deploy/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--worker-class", "my_agent.MyAgent"]
