"""
Gateway SDK 异常定义。

所有业务异常均在此模块中集中定义，禁止在业务代码中直接 raise Exception。
"""


class GatewaySDKError(Exception):
    """Gateway SDK 基础异常类"""

    def __init__(self, message: str, cause: Exception | None = None):
        super().__init__(message)
        self._cause = cause

    @property
    def cause(self) -> Exception | None:
        return self._cause


# === Redis 相关异常 ===


class RedisConnectionError(GatewaySDKError):
    """Redis 连接失败"""

    def __init__(
        self,
        message: str = "Failed to connect to Redis",
        cause: Exception | None = None,
    ):
        super().__init__(message, cause)


class StreamGroupExistsError(GatewaySDKError):
    """Redis Stream 消费者组已存在"""

    def __init__(self, group_name: str, stream_name: str):
        super().__init__(
            f"Consumer group '{group_name}' already exists in stream '{stream_name}'"
        )
        self.group_name = group_name
        self.stream_name = stream_name


# === 执行相关异常 ===


class ExecutionNotFoundError(GatewaySDKError):
    """执行记录不存在"""

    def __init__(self, execution_id: str, session_id: str = ""):
        msg = f"Execution not found: {execution_id}"
        if session_id:
            msg += f" (session: {session_id})"
        super().__init__(msg)
        self.execution_id = execution_id
        self.session_id = session_id


class ExecutionDataError(GatewaySDKError):
    """执行数据解析失败"""

    def __init__(self, execution_id: str, cause: Exception | None = None):
        super().__init__(f"Failed to parse execution data for {execution_id}", cause)
        self.execution_id = execution_id


class SessionMismatchError(GatewaySDKError):
    """Session 不匹配"""

    def __init__(self, message_id: str, expected_session: str, actual_session: str):
        super().__init__(
            f"Session mismatch for message {message_id}: "
            f"expected {expected_session}, got {actual_session}"
        )
        self.message_id = message_id
        self.expected_session = expected_session
        self.actual_session = actual_session


class TerminalStateError(GatewaySDKError):
    """尝试对已终止的执行进行操作"""

    def __init__(self, execution_id: str, current_status: str):
        super().__init__(
            f"Execution {execution_id} is already in terminal state: {current_status}"
        )
        self.execution_id = execution_id
        self.current_status = current_status


# === 消息处理异常 ===


class UnsupportedCommandError(GatewaySDKError):
    """不支持的命令类型"""

    def __init__(self, command_type: str):
        super().__init__(f"Unsupported command type: {command_type}")
        self.command_type = command_type


class MessageParseError(GatewaySDKError):
    """消息解析失败"""

    def __init__(self, message_id: str = "", cause: Exception | None = None):
        msg = "Failed to parse message"
        if message_id:
            msg += f": {message_id}"
        super().__init__(msg, cause)
        self.message_id = message_id


class MessageDataNotFoundError(GatewaySDKError):
    """消息数据不存在"""

    def __init__(self, message_id: str = ""):
        msg = "Message data not found"
        if message_id:
            msg += f": {message_id}"
        super().__init__(msg)
        self.message_id = message_id


# === Worker 相关异常 ===


class WorkerNotFoundError(GatewaySDKError):
    """未找到可用的 Worker"""

    def __init__(self, agent_type: str):
        super().__init__(f"No worker found for agent type: {agent_type}")
        self.agent_type = agent_type


class WorkerLockError(GatewaySDKError):
    """Worker 锁定失败（可能已被其他实例占用）"""

    def __init__(self, worker_id: str):
        super().__init__(f"Worker ID already in use: {worker_id}")
        self.worker_id = worker_id


class WorkerRegistryNotSetError(GatewaySDKError):
    """WorkerRegistry 未设置"""

    def __init__(self, operation: str):
        super().__init__(f"GatewayClient requires a WorkerRegistry to {operation}")
        self.operation = operation


# === 命令验证异常 ===


class CommandValidationError(GatewaySDKError):
    """命令参数校验失败"""

    def __init__(self, command_type: str, reason: str):
        super().__init__(f"Validation failed for {command_type}: {reason}")
        self.command_type = command_type
        self.reason = reason
