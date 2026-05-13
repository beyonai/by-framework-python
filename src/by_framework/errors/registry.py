"""Registry-related exception definitions."""

from by_framework.errors.base import FrameworkError


class WorkerNotFoundError(FrameworkError):
    """No available Worker found."""

    def __init__(self, agent_type: str):
        super().__init__(f"No worker found for agent type: {agent_type}")
        self.agent_type = agent_type


class WorkerLockError(FrameworkError):
    """Worker lock failed (may already be occupied by another instance)."""

    def __init__(self, worker_id: str):
        super().__init__(f"Worker ID already in use: {worker_id}")
        self.worker_id = worker_id


class WorkerRegistryNotSetError(FrameworkError):
    """WorkerRegistry is not set."""

    def __init__(self, operation: str):
        super().__init__(f"GatewayClient requires a WorkerRegistry to {operation}")
        self.operation = operation
