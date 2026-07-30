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
# "pypi" (default - for downstream projects) or "local" (used only by this
# repo's own .github/workflows/deploy-smoke-test.yml, to validate this
# Dockerfile against a PR's code before it's ever released). Leave this
# alone unless you're working on by-framework-python itself.
ARG BY_FRAMEWORK_SOURCE="pypi"

# Replace this with your own worker package/module.
COPY . /app/worker
ENV PYTHONPATH=/app/worker

# In "local" mode, /app/worker above IS the by-framework-python checkout, so
# install it editable from there instead of from PyPI.
RUN uv venv /opt/venv \
    && if [ "$BY_FRAMEWORK_SOURCE" = "local" ]; then \
        uv pip install --python /opt/venv/bin/python -e /app/worker; \
    else \
        uv pip install --python /opt/venv/bin/python \
            "by-framework${BY_FRAMEWORK_VERSION:+==${BY_FRAMEWORK_VERSION}}"; \
    fi

COPY deploy/entrypoint.sh /entrypoint.sh

# Opt-in readiness endpoint (see
# docs/architecture/worker-readiness-endpoint.md) - BYAI_WORKER_HEALTH_PORT
# is run_worker()'s own env-var fallback for --health-port, so this alone
# is enough to turn it on; no CMD change needed even if you override
# --worker-class below.
ENV BYAI_WORKER_HEALTH_PORT=8080
EXPOSE 8080

# Readiness only - see the hard rule in
# docs/architecture/worker-readiness-endpoint.md: never turn this into a
# liveness/restart-on-unhealthy check. urlopen() raises (non-zero exit) on
# the endpoint's real 503, so this actually fails on "not ready" - unlike
# a healthcheck that only checks "did the server respond at all".
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/readyz', timeout=3)"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["--worker-class", "my_agent.MyAgent"]
