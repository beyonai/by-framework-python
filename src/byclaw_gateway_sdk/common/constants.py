"""
Gateway SDK Redis Key 常量定义。

所有 Redis Stream 名称、Hash Key、Set Key 等配置项均在此文件中集中管理，
禁止在业务代码中直接硬编码字面量字符串。
"""


class RedisKeys:
    """Gateway SDK 全局 Redis Key 命名规范及常量。"""

    # --- 队列与流 (Streams / Queues) ---
    @staticmethod
    def ctrl_stream(capability: str) -> str:
        """控制流队列，用于向指定能力的 Worker 下发任务。"""
        return f"byai_gateway:ctrl:capability:{capability}"

    @staticmethod
    def worker_ctrl_stream(worker_id: str) -> str:
        """Worker 专属控制队列，用于向指定 worker 定向下发控制命令。"""
        return f"byai_gateway:ctrl:worker:{worker_id}"

    @staticmethod
    def session_data_stream(session_id: str) -> str:
        """会话级数据流。Worker 将流式内容推送至此。"""
        return f"byai_gateway:session:{session_id}:data_stream"

    @staticmethod
    def task_group(group_id: str) -> str:
        """任务组的进度追踪 Hash Key。"""
        return f"byai_gateway:task_group:{group_id}"

    # --- 注册中心 (Registry) ---
    # 活跃 Worker 的有序集合 (按心跳时间戳排序)
    ACTIVE_WORKERS = "byai_gateway:registry:active_workers"

    # 默认生存时间 (7天)，用于清理会话相关的聚合 Key
    DEFAULT_SESSION_TTL = 7 * 24 * 3600

    @staticmethod
    def worker_capabilities(worker_id: str) -> str:
        """存储某个 Worker 支持的所有能力标识符的 Set Key。"""
        return f"byai_gateway:registry:worker:capabilities:{worker_id}"

    @staticmethod
    def capability_workers(capability: str) -> str:
        """存储具备某种能力的所有 Worker ID 的 Set Key。"""
        return f"byai_gateway:registry:capability:workers:{capability}"

    @staticmethod
    def worker_lock(worker_id: str) -> str:
        """Worker 启动互斥锁，防止重复 worker_id 并发启动。"""
        return f"byai_gateway:registry:worker:lock:{worker_id}"

    @staticmethod
    def session_registry(session_id: str) -> str:
        """会话级聚合注册表 (Hash)。

        内部分为以下 Field 类别：
        - exec:{execution_id} -> 存储具体的执行明细 JSON
        - msg_map:{message_id} -> 存储消息 ID 到执行 ID 的映射关系
        """
        return f"byai_gateway:session:{session_id}:registry"

    # --- 消费者组 (Consumer Groups) ---
    # Gateway Worker 消费控制流使用的 Consumer Group
    CG_AGENT_ENGINES = "byai_gateway:consumer_group:agent_engines"


# --- ID 前缀常量 (ID Prefixes) ---
# 用于生成唯一 ID，避免在业务代码中硬编码
MESSAGE_ID_PREFIX = "msg-"
EXECUTION_ID_PREFIX = "exec-"
TASK_GROUP_ID_PREFIX = "tg-"
CANCEL_MESSAGE_ID_PREFIX = "msg-cancel-"

# --- Redis Hash Field 前缀 ---
# Session Registry Hash 中的字段前缀
EXEC_FIELD_PREFIX = "exec:"
MSG_MAP_PREFIX = "msg_map:"


# --- 任务组 Hash 字段 (Task Group Hash Fields) ---
TASK_GROUP_FIELD_TOTAL = "total"
TASK_GROUP_FIELD_COMPLETED = "completed"
TASK_GROUP_FIELD_SOURCE_AGENT = "source_agent_id"


# --- 时间与睡眠常量 (Timing Constants) ---
# 控制循环睡眠间隔（秒）
CONTROL_LOOP_SLEEP_SECONDS = 0.01
# 等待任务完成超时（秒）
WAIT_FOR_TASKS_TIMEOUT_SECONDS = 5.0
# 任务组 Key TTL（秒），默认 1 天
TASK_GROUP_TTL_SECONDS = 86400
# 首次重试等待时间（秒）
FIRST_RETRY_WAIT_SECONDS = 1.0
# 最大重试次数
MAX_RETRY_COUNT = 3


# --- 流读取标记 (Stream Read Markers) ---
# Redis XREAD/XREADGROUP 使用 ">" 表示仅读取新消息
STREAM_READ_LAST_ID = ">"
