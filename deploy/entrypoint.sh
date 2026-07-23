#!/bin/sh
# Uses `exec` so the Python process replaces this shell as PID 1, receiving
# SIGTERM directly - a shell left as PID 1 does not forward signals to its
# child by default, which would silently defeat the graceful-shutdown fix in
# by_framework.worker.app (see docs/architecture/production-deployment.md).
#
# `$(hostname)` gives every replica a distinct worker id with no extra
# wiring: Docker Compose assigns each scaled replica a unique container
# hostname, and Kubernetes sets a Pod's hostname to its Pod name by default -
# so the same entrypoint works unchanged under `docker compose up --scale`
# and a Kubernetes Deployment.
set -e
exec python -m by_framework --worker-id "worker-$(hostname)" "$@"
