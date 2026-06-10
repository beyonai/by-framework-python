# Trace 与 Metrics 时间关联方案

## 背景

by-framework 的可观测性现在分成两条主线：

- `trace`：记录单个任务内部的执行链路，例如 `client.dispatch`、
  `worker.execute`、agent task、LangGraph node、LLM call。
- `metrics`：记录系统级状态，例如 worker 在线数、队列深度、pending、
  执行延迟、失败数和告警数。

排查单个任务时，只看 trace 能定位“慢在哪个节点”；但要解释“为什么慢”，
还需要知道该节点发生时系统整体处于什么状态。因此第一阶段通过时间窗口把
trace span 和 metrics history 关联起来。

## 设计

trace 和 metrics 不合并成一套模型，而是在读取层建立关联：

1. `TraceReadClient.get_trace(trace_id)` 继续返回任务链路树。
2. `TraceReadClient.explain_trace(trace_id)` 从 trace record 和 span 推导
   `start_ts/end_ts`。
3. `MetricsReadClient.explain_window(start_ts, end_ts, buffer_ms)` 读取该窗口
   前后的系统 metrics history。
4. `explain_trace()` 返回 `related_metrics`，用于 dashboard 或排查工具展示
   “该任务发生时系统状态”。

核心关联字段：

- 时间：`start_ts`、`end_ts`
- 任务：`trace_id`、`session_id`、`message_id`、`execution_id`
- 资源：`worker_id`、`source_agent_type`、`target_agent_type`

## 第一阶段 API

```python
from by_framework.metrics import MetricsReadClient
from by_framework_trace_query import TraceReadClient

metrics = await MetricsReadClient(redis).explain_window(
    start_ts=trace_start,
    end_ts=trace_end,
    buffer_ms=5000,
)

explanation = await TraceReadClient(redis_client=redis).explain_trace(
    trace_id,
    session_id=session_id,
    include_metrics=True,
)
```

`related_metrics` 返回：

- `window`：实际查询窗口。
- `samples`：命中的 metrics history points。
- `summary`：各关键指标的 `min/max/last`。
- `diagnostics`：例如 `metrics_queue_backlog`、
  `metrics_consumer_pending`、`metrics_history_missing`。

## 写入约束

为了让时间关联可靠，trace 写入必须满足：

- trace meta 的 `start_ts` 表示整个 trace 的最早开始时间。
- trace meta 的 `updated_at` 表示已知最晚结束/更新时间。
- span 写入不能把 trace-level `start_ts` 覆盖成更晚的子节点时间。
- metrics history point 必须有毫秒级 `generated_at`。

本次改造已修复 Redis span 写入覆盖 trace meta `start_ts` 的问题。

## 后续演进

1. Dashboard 点击 span 时，用 span 自身时间窗口查询 metrics。
2. `MetricsReadClient` 增加 agent/worker 过滤后的细粒度 history。
3. `TraceReadClient.explain_trace()` 输出 span-level metrics correlation。
4. 接入 Prometheus/Langfuse/Phoenix read source，形成多源 explain。
