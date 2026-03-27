# 🚀 by-framework-python

[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](pyproject.toml)
[![Python](https://img.shields.io/badge/python-3.12+-yellow.svg)](pyproject.toml)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](pyproject.toml)

**by-framework** 是一个基于 Redis Streams 构建的分布式、高性能 Agent 调度引擎。它为【超级助手】、【数字员工】等具备自驱编排、沙箱隔离能力的智能体服务提供了标准化的开发框架和运行环境。

---

## 📋 目录

- [✨ 核心特性](#-核心特性)
- [🏗️ 核心架构](#️-核心架构)
- [📦 安装](#-安装)
- [🚀 快速上手](#-快速上手)
- [💡 深入理解](#-深入理解)
- [🔌 插件系统](#-插件系统)
- [📡 发送任务](#-发送任务)
- [🧪 示例](#-示例)
- [🛠️ 配置参考](#️-配置参考)
- [📚 API 参考](#-api-参考)
- [🚀 部署指南](#-部署指南)
- [🔗 更多资源](#-更多资源)

---

## ✨ 核心特性

- ⚡ **原生异步**：基于 Python `asyncio` 构建，完美契合 I/O 密集型 Agent 任务。
- 🧩 **高度插件化**：内置强大的插件系统，支持动态扩展工具（Tools）、提示词（Prompts），并支持**热重载**。
- 📊 **状态管控**：完善的 `AgentContext` 支持，轻松实现流式输出、状态流转和附件处理。
- 🔄 **解耦架构**：采用"控制流-数据流分离"设计，支持大规模 Worker 集群水平扩展。
- 📝 **历史持久化**：支持内存和 Postgres 等多种历史存储方式，便于任务追踪和审计。
- 🎯 **能力匹配**：Worker 通过声明 `capabilities` 实现任务的智能路由。

---

## 🏗️ 核心架构

系统采用事件驱动设计，高度解耦：

```
┌─────────────┐       ┌──────────────┐       ┌──────────────┐
│   Client    │──────▶│  Redis Input │──────▶│   Gateway    │
│ (Gateway)   │       │     MQ       │       │   Worker     │
└─────────────┘       └──────────────┘       └──────┬───────┘
        ▲                                              │
        │                                              │
        │                                              ▼
┌─────────────┐       ┌──────────────┐       ┌──────────────┐
│   Backend   │◀─────│  Redis Data   │◀─────│   Business   │
│  (WebSocket)│       │     MQ       │       │   Logic      │
└─────────────┘       └──────────────┘       └──────────────┘
```

### 核心组件说明

- **接入层**: `GatewayClient` 向 Redis Input MQ 投递控制指令。
- **调度层**: 利用 Redis Stream 实现 Worker 集群的竞争消费与路由。
- **执行层**: `GatewayWorker` 主动拉取任务，并在独立的隔离工作空间中执行业务逻辑。
- **输出层**: 数据通过异步推入 Data MQ，支持 WebSocket 推送及多路并行消费。

### 数据流向

```
User Request
    ↓
Gateway (写入 Redis queue:ctrl)
    ↓
Worker (消费 queue:ctrl，处理任务)
    ↓
Redis Stream (写入 queue:data:stream)
    ↓
Backend (消费 queue:data:stream，通过 WebSocket 推送)
    ↓
Frontend (渲染实时 AI 响应)
```

---

## 📦 安装

### 前置要求

- Python 3.12+
- Redis 7.0+ (用于消息队列)
- (可选) PostgreSQL 14+ (用于历史持久化)

### 使用 pip 安装

```bash
# 基础安装
pip install by-framework

# 包含 Postgres 支持
pip install "by-framework[postgres]"

# 开发模式安装
pip install -e ".[dev]"
```

### 使用 uv 安装 (推荐)

```bash
# 克隆项目后安装所有依赖
cd by-framework-python
uv sync
```

---

## 🚀 快速上手

### 1. 创建一个简单的 Agent Worker

创建 `my_agent.py`：

```python
import asyncio
from by_framework import GatewayWorker, AgentContext, run_worker

class MyAssistant(GatewayWorker):
    def get_capabilities(self):
        # 声明此 Worker 能够处理的 Agent 类型
        return ["weather_agent", "chat_agent"]

    async def process_command(self, command, context: AgentContext):
        # 发送流式文本片段
        await context.emit_chunk("正在处理您的请求...\n")

        # 模拟耗时操作
        await asyncio.sleep(0.5)

        # 更新任务状态
        await context.emit_state("thinking")

        # 从 command 中获取用户输入
        user_input = command.data.get("input", "")

        # 发送思考过程
        await context.emit_chunk(f"我收到了: {user_input}\n")
        await asyncio.sleep(0.3)

        # 发送最终结果
        await context.emit_chunk("这是我的回复！")

        return {
            "status": "success",
            "message": "任务完成",
            "data": {"answer": "今天天气晴朗"}
        }

if __name__ == "__main__":
    run_worker(
        worker_class=MyAssistant,
        worker_id="worker-01",
        redis_host="localhost",
        redis_port=6379,
    )
```

### 2. 启动 Redis

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

### 3. 启动 Worker

```bash
cd by-framework-python
uv run python my_agent.py
```

### 4. 发送测试任务

创建 `send_task.py`：

```python
import asyncio
from by_framework import ByaiGatewayClient, AskAgentCommand

async def send_task():
    # 使用 ByaiGatewayClient，它集成了默认的消息拦截器
    client = ByaiGatewayClient(redis_host="localhost")

    # 创建命令 (AskAgentCommand 是最常用的任务指令)
    command = AskAgentCommand(
        # 必须提供消息头，或使用 client.send_message 快捷方法
        target_agent_type="weather_agent",
        content="今天北京天气怎么样？",
        session_id="session-001"
    )

    # 方式一：直接发送命令对象
    # response = await client.send_command(command)

    # 方式二：使用便捷方法 (推荐)
    response = await client.send_message(
        target_agent_type="weather_agent",
        session_id="session-001",
        content="今天北京天气怎么样？"
    )
    
    if response.success:
        print(f"任务已发送，消息 ID: {response.message_id}")
    else:
        print(f"发送失败")

asyncio.run(send_task())
```

运行：

```bash
uv run python send_task.py
```

---

## 💡 深入理解

### GatewayWorker 基类

`GatewayWorker` 是所有自定义 Worker 的基类，你需要实现以下方法：

| 方法 | 是否必须 | 描述 |
|------|---------|------|
| `get_capabilities()` | 是 | 返回此 Worker 能处理的 Agent 类型列表 |
| `process_command(command, context)` | 是 | 处理具体的业务逻辑 |

### AgentContext 上下文

`AgentContext` 提供了与运行环境交互的能力：

```python
async def process_command(self, command, context: AgentContext):
    # 1. 发送流式输出
    await context.emit_chunk("正在处理...")

    # 2. 发送产物/结构化数据
    await context.emit_artifact({"key": "value"})

    # 3. 获取消息 ID 和会话 ID
    msg_id = context.current_message_id
    session_id = context.session_id

    # 4. 调用其他 Agent (支持挂起当前任务等待返回)
    result = await context.call_agent(
        target_agent_type="translator_agent",
        content="Hello",
        wait_for_reply=True
    )

```

### 命令与消息协议

#### AskAgentCommand (任务指令)

```python
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader

command = AskAgentCommand(
    header=MessageHeader(
        message_id="msg_123",
        session_id="sess_456",
        target_agent_type="weather_agent"
    ),
    content="查询北京天气",
    extra_payload={
        "location": "北京"
    }
)
```

#### 事件类型

| 事件类型 | 描述 |
|---------|------|
| `chunk` | 文本片段 (用于流式输出) |
| `data` | 结构化数据 |
| `state` | 状态更新 |
| `error` | 错误 |
| `done` | 完成 |

---

## 🔌 插件系统

插件是 Gateway SDK 扩展能力的基石。你可以通过插件注册工具、提示词模板等。

### 插件目录结构

```
my_plugins/
├── weather_plugin/
│   ├── __init__.py
│   ├── plugin.py
│   ├── tools.py
│   └── prompts/
│       └── weather_prompt.txt
└── calculator_plugin/
    └── ...
```

### 编写插件

创建 `my_cool_plugin.py`：

```python
from by_framework import Plugin, PluginManifest, AgentConfig, PluginBuildContext
from typing import Any

class WeatherPlugin(Plugin):
    def __init__(self):
        super().__init__(PluginManifest(
            plugin_id="weather_plugin",
            version="1.0.0",
            name="天气查询插件",
            description="提供天气查询能力"
        ))

    async def register_agent_configs(self, build_context: PluginBuildContext) -> list[AgentConfig]:
        # 插件通过返回 AgentConfig 列表来注册能力
        config = AgentConfig(
            agent_id="weather_assistant",
            tools={
                "get_current_weather": self._get_weather,
                "get_forecast": self._get_forecast
            },
            prompts={
                "system_prompt": "你是一个天气助手..."
            }
        )
        return [config]

    async def _get_weather(self, city: str) -> dict[str, Any]:
        """获取当前天气"""
        # 实际项目中这里会调用真实的天气 API
        return {
            "city": city,
            "temperature": 25,
            "condition": "晴",
            "humidity": 60
        }

    async def _get_forecast(self, city: str, days: int = 3) -> list[dict]:
        """获取天气预报"""
        return [
            {"day": 1, "high": 28, "low": 18, "condition": "晴"},
            {"day": 2, "high": 26, "low": 16, "condition": "多云"},
            {"day": 3, "high": 24, "low": 14, "condition": "阴"}
        ][:days]

    # 插件生命周期钩子
    async def on_task_start(self, context: AgentContext):
        """任务开始时调用"""
        print(f"任务 {context.task_id} 开始")

    async def on_task_complete(self, context: AgentContext, result: Any):
        """任务成功完成时调用"""
        print(f"任务 {context.task_id} 完成")

    async def on_task_error(self, context: AgentContext, error: Exception):
        """任务出错时调用"""
        print(f"任务 {context.task_id} 出错: {error}")
```

### 使用插件

方式一：通过 plugin_list 参数传入

```python
from by_framework import run_worker
from my_cool_plugin import WeatherPlugin

run_worker(
    worker_class=MyAssistant,
    worker_id="worker-01",
    plugin_list=[WeatherPlugin()]
)
```

方式二：通过 plugin_configurator 回调配置

```python
from by_framework import run_worker
from my_cool_plugin import WeatherPlugin

def configure_plugins(registry):
    registry.register_bundle(WeatherPlugin())

run_worker(
    worker_class=MyAssistant,
    worker_id="worker-01",
    plugin_configurator=configure_plugins
)
```

方式三：通过插件目录自动加载 (支持热重载)

```python
run_worker(
    worker_class=MyAssistant,
    plugin_dir="./my_plugins"  # 插件目录
)
```

---

## 📡 发送任务

### 使用 ByaiGatewayClient

`ByaiGatewayClient` 是对 `GatewayClient` 的封装，默认集成了 `ByaiMessageInterceptor`，支持更高级的消息协议。

```python
from by_framework import ByaiGatewayClient

async def main():
    # 初始化客户端
    client = ByaiGatewayClient(
        redis_host="localhost",
        redis_port=6379
    )

    # 发送消息
    response = await client.send_message(
        target_agent_type="weather_agent",
        session_id="session_123",
        tenant_id="tenant_123",
        content="查询北京今天的天气"
    )
    
    if response.success:
        print(f"任务已发送，消息 ID: {response.message_id}")

    # 关闭客户端
    await client.close()

import asyncio
asyncio.run(main())
```

---

## 🧪 示例

### 示例 1: 基础流式输出

```python
class StreamingAgent(GatewayWorker):
    def get_capabilities(self):
        return ["streaming_demo"]

    async def process_command(self, command, context: AgentContext):
        text = "这是一段流式输出的示例文本。"

        for char in text:
            await context.emit_chunk(char)
            await asyncio.sleep(0.05)

        return {"status": "done"}
```

### 示例 2: 使用工具调用

工具通过插件机制提供，使用 `context.call_tool(name, **kwargs)` 调用。

```python
class ToolAgent(GatewayWorker):
    def get_capabilities(self):
        return ["tool_demo"]

    def get_tools(self):
        return {
            "calculate": self.calculate,
            "search": self.search
        }

    async def process_command(self, command, context: AgentContext):
        # 通过 call_tool 调用工具
        result = await context.call_tool("calculate", a=2, b=3, op="+")
        await context.emit_chunk(f"计算结果: {result}")

        return {"status": "success"}

    async def calculate(self, a: float, b: float, op: str) -> float:
        """计算工具"""
        if op == "+":
            return a + b
        elif op == "-":
            return a - b
        elif op == "*":
            return a * b
        elif op == "/":
            return a / b if b != 0 else 0
        return 0
```

### 示例 3: 历史记录持久化

历史消息通过 `HistoryProvider` 管理，自动保存流式响应。

```python
from by_framework.core.history import HistoryProvider

class HistoryAgent(GatewayWorker):
    def get_capabilities(self):
        return ["history_demo"]

    async def process_command(self, command, context: AgentContext):
        # 获取历史记录
        history = await HistoryProvider.get_session_history(
            session_id=context.session_id,
            limit=10
        )
        await context.emit_chunk(f"历史记录: {history}")

        return {"status": "success"}
```

历史记录会在 `emit_chunk` 时自动保存到 `HistoryProvider` 指定的存储后端。

---

## 🛠️ 配置参考

### run_worker 函数参数

`run_worker` 函数支持丰富的配置项：

| 参数 | 类型 | 描述 | 默认值 |
| :--- | :--- | :--- | :--- |
| `worker_class` | `Type[GatewayWorker]` | **必填**。业务 Worker 类。 | - |
| `worker_id` | `str` | Worker 实例的唯一标识名。 | `"worker-1"` |
| `redis_host` | `str` | Redis 服务器地址。 | `"localhost"` |
| `redis_port` | `int` | Redis 端口。 | `6379` |
| `redis_db` | `int` | Redis 数据库号。 | `0` |
| `redis_password` | `str` | Redis 密码 (可选)。 | `None` |
| `redis_username` | `str` | Redis 用户名 (可选)。 | `None` |
| `workspace_dir` | `str` | 任务执行的本地工作目录。 | `"/tmp/gateway-workspace"` |
| `consumer_group` | `str` | Redis 消费者组名称。 | `"agent_engines"` |
| `max_concurrency` | `int` | 单个 Worker 的最大并发处理数。 | `50` |
| `fetch_count` | `int` | 每次从 Redis 批量获取的消息数量。 | `10` |
| `redis_max_connections` | `int` | Redis 最大连接数。 | `max_concurrency + 10` |
| `plugin_list` | `List[Plugin]` | 显式传入的插件列表。 | `None` |
| `plugin_configurator` | `Callable` | 插件配置回调函数。 | `None` |
| `plugin_dir` | `str` | 插件自动扫描目录 (支持热重载)。 | `None` |
| `history` | `BaseHistoryStorage` | 历史记录存储后端。 | `None` (默认 in-memory) |

### 环境变量

| 环境变量 | 描述 | 默认值 |
|---------|------|-------|
| `REDIS_HOST` | Redis 主机 | `localhost` |
| `REDIS_PORT` | Redis 端口 | `6379` |
| `REDIS_DB` | Redis 数据库 | `0` |
| `REDIS_PASSWORD` | Redis 密码 | - |
| `REDIS_USERNAME` | Redis 用户名 | - |
| `BYAI_WORKER_CONCURRENCY` | 最大并发数 | `50` |
| `BYAI_WORKER_FETCH_COUNT` | 批量获取消息数 | `10` |
| `BYAI_REDIS_MAX_CONNECTIONS` | Redis 最大连接数 | `max_concurrency + 10` |

---

## 📚 API 参考

### GatewayWorker

```python
class GatewayWorker:
    def get_capabilities(self) -> List[str]:
        """返回此 Worker 能处理的 Agent 类型列表"""
        pass

    async def process_command(self, command, context: AgentContext) -> Any:
        """处理命令并返回结果"""
        pass
```

### AgentContext

```python
class AgentContext:
    session_id: str
    trace_id: str
    current_agent_id: str
    current_message_id: str

    async def emit_chunk(self, event: Union[StreamChunkEvent, str], event_type: Optional[str] = None):
        """发送文本片段或流式事件"""

    async def emit_state(self, event: Union[StateChangeEvent, str], event_type: Optional[str] = None):
        """发送状态更新"""

    async def emit_artifact(self, event: Union[ArtifactEvent, str], event_type: Optional[str] = None):
        """发送产物/附件"""

    async def ask_user(self, event: Union[AskUserEvent, str]) -> dict:
        """向用户发送等待输入请求"""

    async def call_agent(self, target_agent_type: str, content: str, **kwargs) -> dict:
        """调用其他 Agent"""

    async def dispatch_group(self, tasks: list[dict], **kwargs) -> dict:
        """分发任务组"""

    async def call_tool(self, name: str, **kwargs):
        """调用已注册的工具"""

    async def get_active_workers(self) -> Dict[str, Any]:
        """获取集群中所有活跃的 worker"""
```

### GatewayClient / ByaiGatewayClient

```python
class GatewayClient:
    async def send_message(
        self,
        target_agent_type: str,
        session_id: str,
        content: Any,
        tenant_id: str = "",
        action_type: str = "ASK_AGENT",
        metadata: Optional[dict] = None
    ) -> SendMessageResponse:
        """发送消息，返回响应对象"""

    async def cancel_task(self, message_id: str, session_id: str, reason: str = "") -> CancelTaskResponse:
        """取消指定的任务"""

## 🚀 部署指南

### 单机部署

1. **准备环境**

```bash
# 安装依赖
cd by_framework
uv sync
```

2. **启动 Redis**

```bash
docker run -d --name gateway-redis \
  -p 6379:6379 \
  --restart unless-stopped \
  redis:7-alpine
```

3. **启动 Worker**

```bash
uv run python -m by_framework \
  --worker-class my_agent.MyAgent \
  --worker-id worker-01 \
  --redis-host localhost
```

### 集群部署

使用 Docker Compose 部署多个 Worker：

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

  worker-1:
    build: .
    command: >
      python -m by_framework
      --worker-class my_agent.MyAgent
      --worker-id worker-01
      --redis-host redis
    depends_on:
      - redis

  worker-2:
    build: .
    command: >
      python -m by_framework
      --worker-class my_agent.MyAgent
      --worker-id worker-02
      --redis-host redis
    depends_on:
      - redis

volumes:
  redis-data:
```

### 生产环境建议

1. **使用连接池**

```python
run_worker(
    worker_class=MyAgent,
    redis_max_connections=50
)
```

2. **配置监控**

```python
from by_framework.common.logger import setup_logging

setup_logging(level="INFO", json_format=True)
```

3. **启用历史持久化**

```python
from by_framework.core.history.storage.postgres import PostgresHistoryStorage

storage = PostgresHistoryStorage(
    dsn="postgresql://user:pass@localhost/gateway"
)

run_worker(
    worker_class=MyAgent,
    history=storage
)
```

---

### 常见问题

**Q: 如何保证任务不丢失？**

A: Redis Streams 提供持久化机制。Worker 使用 `XACK` 确认消息处理完成，未确认的消息会被重新投递。

**Q: 如何实现 Worker 负载均衡？**

A: 多个 Worker 连接同一个 Redis Stream，Redis 会自动在消费者组内进行负载分配。

**Q: 支持哪些语言的 SDK？**

A: 目前支持 Python、Java、TypeScript。可以参考现有 SDK 实现其他语言版本。

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 开发流程

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'feat: add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启 Pull Request

---

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

由 **byai 团队** 维护。

有问题或建议？欢迎联系我们！
