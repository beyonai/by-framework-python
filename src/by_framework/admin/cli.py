"""by-admin — by-framework cluster admin CLI."""

# pylint: disable=global-statement

from __future__ import annotations

import asyncio
import json
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from by_framework.admin.worker_manager import WorkerManager
from by_framework.common.config import RedisConfig
from by_framework.common.redis_client import init_redis, init_redis_from_url
from by_framework.core.registry import WorkerRegistry
from by_framework.metrics.snapshot import (
    build_observability_snapshot,
    load_history_from_redis,
)

# --------------------------------------------------------------------------- #
#  App hierarchy
# --------------------------------------------------------------------------- #

app = typer.Typer(
    name="by-admin",
    no_args_is_help=True,
    help=(
        "by-framework cluster admin CLI. For Redis Cluster, omit --redis-url "
        "and set REDIS_CLUSTER_HOST (comma-separated host:port list; implies "
        "both cluster mode and REDIS_KEY_SCHEMA_VERSION=v2 unless set "
        "explicitly). REDIS_MODE=cluster + REDIS_CLUSTER_NODES + "
        "REDIS_KEY_SCHEMA_VERSION=v2 also still works."
    ),
)
worker_app = typer.Typer(no_args_is_help=True)
type_app = typer.Typer(no_args_is_help=True)
metrics_app = typer.Typer(no_args_is_help=True)

app.add_typer(worker_app, name="worker", help="Worker lifecycle management")
app.add_typer(type_app, name="type", help="Agent-type admission control")
app.add_typer(metrics_app, name="metrics", help="Metrics and observability")

# --------------------------------------------------------------------------- #
#  Global state + helpers
# --------------------------------------------------------------------------- #

console = Console()
err_console = Console(stderr=True)
_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_redis_url: Optional[str] = None


@app.callback()
def _global(
    redis_url: Optional[str] = typer.Option(
        None,
        "--redis-url",
        envvar=["BYAI_REDIS_URL", "REDIS_URL"],
        help=(
            "Standalone Redis connection URL [env: BYAI_REDIS_URL]. "
            "For Redis Cluster, omit this and set REDIS_CLUSTER_HOST "
            "(comma-separated host:port list)."
        ),
    ),
):
    """by-framework cluster admin CLI."""
    global _redis_url
    _redis_url = redis_url


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def _die(msg: str, code: int = 1) -> None:
    err_console.print(f"[red]Error:[/red] {msg}")
    raise typer.Exit(code)


def _get_redis(redis_url: Optional[str] = None):
    configured_url = redis_url if redis_url is not None else _redis_url
    if configured_url:
        return init_redis_from_url(configured_url)
    config = RedisConfig.from_env()
    if config.mode == "cluster":
        return init_redis(config=config)
    return init_redis_from_url(_DEFAULT_REDIS_URL)


# --------------------------------------------------------------------------- #
#  worker subcommands
# --------------------------------------------------------------------------- #

_LIFECYCLE_COLORS = {"active": "green", "suspended": "yellow", "evicted": "red"}


@worker_app.command("list")
def worker_list(
    agent_type: Optional[str] = typer.Option(
        None, "--type", "-t", help="Filter by agent type"
    ),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """List all online workers."""

    async def _inner():
        registry = WorkerRegistry(_get_redis())
        workers = await registry.get_all_workers()

        rows = [{"worker_id": wid, **data} for wid, data in workers.items()]
        if agent_type:
            rows = [r for r in rows if agent_type in r.get("agent_types", [])]

        if as_json:
            typer.echo(json.dumps(rows, default=str))
            return

        if not rows:
            console.print("[dim]No workers found.[/dim]")
            return

        table = Table(show_header=True, header_style="bold")
        table.add_column("Worker ID")
        table.add_column("Lifecycle")
        table.add_column("Agent Types")
        table.add_column("IP")
        table.add_column("Last Seen (ms)")

        for w in rows:
            lc = w.get("lifecycle", "active")
            color = _LIFECYCLE_COLORS.get(lc, "white")
            table.add_row(
                w["worker_id"],
                f"[{color}]{lc}[/{color}]",
                ", ".join(w.get("agent_types", [])),
                w.get("ip_address", ""),
                str(w.get("last_seen", "")),
            )

        console.print(table)
        console.print(f"\n[dim]{len(rows)} worker(s)[/dim]")

    _run(_inner())


@worker_app.command("info")
def worker_info(
    worker_id: str = typer.Argument(..., help="Worker ID"),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show detailed info for a single worker."""

    async def _inner():
        registry = WorkerRegistry(_get_redis())
        workers = await registry.get_all_workers()
        worker = workers.get(worker_id)

        if worker is None:
            _die(f"worker '{worker_id}' not found")
            return

        result = {"worker_id": worker_id, **worker}
        if as_json:
            typer.echo(json.dumps(result, default=str))
            return

        lc = result.get("lifecycle", "active")
        color = _LIFECYCLE_COLORS.get(lc, "white")
        lc_reason = result.get("lifecycle_reason", "")
        agent_types_str = ", ".join(result.get("agent_types", []))
        ip_address = result.get("ip_address", "")
        last_seen = result.get("last_seen", "")
        console.print(f"[bold]Worker:[/bold] {worker_id}")
        console.print(f"  lifecycle:        [{color}]{lc}[/{color}]")
        console.print(f"  lifecycle_reason: {lc_reason}")
        console.print(f"  agent_types:      {agent_types_str}")
        console.print(f"  ip_address:       {ip_address}")
        console.print(f"  last_seen (ms):   {last_seen}")

    _run(_inner())


@worker_app.command("suspend")
def worker_suspend(
    worker_id: str = typer.Argument(..., help="Worker ID"),
    reason: str = typer.Option("", "--reason", "-r", help="Reason for suspension"),
):
    """Suspend a worker (stops consuming, stays online)."""

    async def _inner():
        mgr = WorkerManager(_get_redis())
        await mgr.suspend_worker(worker_id, reason=reason)
        console.print(f"[green]✓[/green] Suspended {worker_id}")

    _run(_inner())


@worker_app.command("resume")
def worker_resume(
    worker_id: str = typer.Argument(..., help="Worker ID"),
):
    """Resume a suspended worker."""

    async def _inner():
        mgr = WorkerManager(_get_redis())
        await mgr.resume_worker(worker_id)
        console.print(f"[green]✓[/green] Resumed {worker_id}")

    _run(_inner())


@worker_app.command("evict")
def worker_evict(
    worker_id: str = typer.Argument(..., help="Worker ID"),
    force: bool = typer.Option(
        False, "--force", help="Cancel in-flight tasks immediately"
    ),
):
    """Evict a worker (clears lease, removes from routing)."""

    async def _inner():
        mgr = WorkerManager(_get_redis())
        await mgr.evict_worker(worker_id, force=force)
        console.print(f"[green]✓[/green] Evicted {worker_id}")

    _run(_inner())


# --------------------------------------------------------------------------- #
#  type subcommands
# --------------------------------------------------------------------------- #


@type_app.command("denylist")
def type_denylist(
    agent_type: str = typer.Argument(..., help="Agent type name"),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show all workers denied for an agent type."""

    async def _inner():
        mgr = WorkerManager(_get_redis())
        denied = await mgr.get_type_denylist(agent_type)
        if as_json:
            typer.echo(json.dumps(denied))
            return
        if not denied:
            console.print(f"[dim]No denied workers for type '{agent_type}'[/dim]")
            return
        for wid in denied:
            console.print(f"  {wid}")

    _run(_inner())


@type_app.command("deny")
def type_deny(
    agent_type: str = typer.Argument(..., help="Agent type name"),
    worker_id: str = typer.Argument(..., help="Worker ID to deny"),
):
    """Deny a worker from consuming an agent type."""

    async def _inner():
        mgr = WorkerManager(_get_redis())
        await mgr.deny_worker_for_type(agent_type, worker_id)
        console.print(
            f"[green]✓[/green] Denied {worker_id} from consuming '{agent_type}'"
        )

    _run(_inner())


@type_app.command("allow")
def type_allow(
    agent_type: str = typer.Argument(..., help="Agent type name"),
    worker_id: str = typer.Argument(..., help="Worker ID to allow"),
):
    """Remove denial for a worker on an agent type."""

    async def _inner():
        mgr = WorkerManager(_get_redis())
        await mgr.allow_worker_for_type(agent_type, worker_id)
        console.print(f"[green]✓[/green] Allowed {worker_id} on '{agent_type}'")

    _run(_inner())


# --------------------------------------------------------------------------- #
#  metrics subcommands
# --------------------------------------------------------------------------- #


@metrics_app.command("snapshot")
def metrics_snapshot(
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Print current cluster observability snapshot."""

    async def _inner():
        snapshot = await build_observability_snapshot(_get_redis())
        if as_json:
            typer.echo(json.dumps(snapshot, default=str))
            return
        totals = snapshot.get("totals", {})
        workers_online = totals.get("workers_online", 0)
        agent_types_n = totals.get("agent_types", 0)
        active_execs = totals.get("active_executions", 0)
        queue_depth = snapshot.get("queue_depth_total", 0)
        console.print("[bold]Cluster Snapshot[/bold]")
        console.print(f"  Workers online:     {workers_online}")
        console.print(f"  Agent types:        {agent_types_n}")
        console.print(f"  Active executions:  {active_execs}")
        console.print(f"  Queue depth total:  {queue_depth}")

    _run(_inner())


@metrics_app.command("history")
def metrics_history(
    limit: int = typer.Option(
        20, "--limit", "-n", help="Number of history points to show"
    ),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Print recent metrics history points."""

    async def _inner():
        points = await load_history_from_redis(_get_redis(), limit=limit)
        if as_json:
            typer.echo(json.dumps(points, default=str))
            return
        if not points:
            console.print("[dim]No history points found.[/dim]")
            return
        table = Table(show_header=True, header_style="bold")
        table.add_column("Timestamp (ms)")
        table.add_column("Workers")
        table.add_column("Active")
        table.add_column("Queue Depth")
        for p in points:
            table.add_row(
                str(p.get("generated_at", "")),
                str(p.get("workers_online", "")),
                str(p.get("active_executions", "")),
                str(p.get("queue_depth_total", "")),
            )
        console.print(table)

    _run(_inner())


# --------------------------------------------------------------------------- #
#  Entry point
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    app()
