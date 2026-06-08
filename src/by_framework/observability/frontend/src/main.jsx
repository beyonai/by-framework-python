import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Clock3,
  GitBranch,
  ListTree,
  Pause,
  Play,
  RefreshCw,
  Server,
  Workflow,
} from "lucide-react";
import "./styles.css";

const TABS = [
  { id: "overview", label: "概览", icon: Activity },
  { id: "workers", label: "Workers", icon: Server },
  { id: "queues", label: "队列", icon: Workflow },
  { id: "executions", label: "执行记录", icon: BarChart3 },
  { id: "sessions", label: "会话", icon: ListTree },
];

const STATUS_ORDER = [
  "RUNNING",
  "QUEUED",
  "CANCELLING",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
];

const REFRESH_OPTIONS = [
  { label: "5 秒", value: 5000 },
  { label: "15 秒", value: 15000 },
  { label: "60 秒", value: 60000 },
  { label: "手动", value: 0 },
];

function isDemoMode() {
  const demo = new URLSearchParams(window.location.search).get("demo");
  return demo === "1" || demo === "true";
}

async function fetchJson(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${path} 请求失败: ${response.status}`);
  }
  return response.json();
}

function endpoint(path) {
  return isDemoMode() ? `${path}?demo=1` : path;
}

function App() {
  const [activeTab, setActiveTab] = useState("overview");
  const [workerData, setWorkerData] = useState(null);
  const [executionData, setExecutionData] = useState(null);
  const [queueData, setQueueData] = useState(null);
  const [flowData, setFlowData] = useState(null);
  const [historyData, setHistoryData] = useState({ points: [] });
  const [apiHealth, setApiHealth] = useState(null);
  const [sessionData, setSessionData] = useState(null);
  const [traceData, setTraceData] = useState(null);
  const [sessionId, setSessionId] = useState(isDemoMode() ? "sess-demo" : "");
  const [traceId, setTraceId] = useState("");
  const [error, setError] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(0);
  const [paused, setPaused] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(5000);

  // Execution filters can be set by clicking worker cards.
  const [filterWorker, setFilterWorker] = useState("");
  const [filterStatus, setFilterStatus] = useState("");
  const [filterAgent, setFilterAgent] = useState("");

  const refresh = async () => {
    if (refreshing) return;
    setRefreshing(true);
    setError("");
    try {
      const workerPromise = fetchJson(endpoint("/api/workers"));
      const executionPromise = fetchJson(endpoint("/api/executions"));
      const flowPromise = fetchJson(endpoint("/api/flow"));
      const historyPromise = fetchJson(endpoint("/api/history"));
      const healthPromise = fetchJson("/api/health");
      const workers = await workerPromise;
      setWorkerData(workers);
      const [executions, flow, history, runtimeHealth] = await Promise.all([
        executionPromise,
        flowPromise,
        historyPromise,
        healthPromise,
      ]);
      setExecutionData(executions);
      setFlowData(flow);
      setHistoryData(history);
      setApiHealth(runtimeHealth);
      const params = new URLSearchParams();
      if (isDemoMode()) params.set("demo", "1");
      (workers.agent_types || []).forEach((agentType) =>
        params.append("agent_type", agentType),
      );
      const query = params.toString();
      const queues = await fetchJson(`/api/queues${query ? `?${query}` : ""}`);
      setQueueData(queues);
      setLastUpdated(Date.now());
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setRefreshing(false);
    }
  };

  useEffect(() => {
    let cancelled = false;
    let timer = null;
    const loop = async () => {
      if (!paused) await refresh();
      if (!cancelled && refreshInterval > 0) {
        timer = window.setTimeout(loop, refreshInterval);
      }
    };
    loop();
    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, [paused, refreshInterval]);

  const alerts = useMemo(
    () =>
      dedupeAlerts([
        ...(workerData?.alerts || []),
        ...(executionData?.alerts || []),
        ...(queueData?.alerts || []),
      ]),
    [workerData, executionData, queueData],
  );

  const agentHealth = useMemo(
    () =>
      mergeAgentHealthQueues(
        executionData?.agent_health || [],
        queueData?.queues || {},
      ),
    [executionData, queueData],
  );

  const navigateToExecutions = (workerId) => {
    setFilterWorker(workerId);
    setFilterStatus("");
    setFilterAgent("");
    setActiveTab("executions");
  };

  const loadSession = async (event) => {
    event.preventDefault();
    if (!sessionId.trim()) {
      setError("请输入 Session ID");
      return;
    }
    const params = new URLSearchParams({ session_id: sessionId.trim() });
    if (traceId.trim()) params.set("trace_id", traceId.trim());
    if (isDemoMode()) params.set("demo", "1");
    try {
      setError("");
      const session = await fetchJson(`/api/session?${params.toString()}`);
      setSessionData(session);
      const selectedTraceId = traceId.trim() || deriveTraceId(session);
      if (selectedTraceId) {
        const traceParams = new URLSearchParams();
        if (sessionId.trim()) traceParams.set("session_id", sessionId.trim());
        if (isDemoMode()) traceParams.set("demo", "1");
        const traceQuery = traceParams.toString();
        setTraceData(
          await fetchJson(
            `/api/trace/${encodeURIComponent(selectedTraceId)}${
              traceQuery ? `?${traceQuery}` : ""
            }`,
          ),
        );
      } else {
        setTraceData(null);
      }
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <p className="eyebrow">by-framework 可观测性</p>
          <h1>Redis Streams 工作集群监控</h1>
        </div>
        <div className="toolbar">
          {apiHealth ? <ApiHealthPill health={apiHealth} /> : null}
          <span className="timestamp">
            {lastUpdated ? `更新于 ${formatTime(lastUpdated)}` : "等待数据…"}
          </span>
          <select
            className="interval-select"
            value={refreshInterval}
            onChange={(e) => {
              const val = Number(e.target.value);
              setRefreshInterval(val);
              if (val === 0) setPaused(false);
            }}
          >
            {REFRESH_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setPaused((p) => !p)}
            disabled={refreshInterval === 0}
            title={paused ? "继续自动刷新" : "暂停自动刷新"}
          >
            {paused ? <Play size={17} /> : <Pause size={17} />}
            {paused ? "继续" : "暂停"}
          </button>
          <a href={isDemoMode() ? "/metrics?demo=1" : "/metrics"}>指标</a>
          <button type="button" onClick={refresh} disabled={refreshing}>
            <RefreshCw size={17} />
            刷新
          </button>
        </div>
      </header>

      <nav className="tabs" aria-label="可观测性视图">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              key={tab.id}
              type="button"
              className={activeTab === tab.id ? "tab active" : "tab"}
              onClick={() => setActiveTab(tab.id)}
            >
              <Icon size={17} />
              {tab.label}
            </button>
          );
        })}
      </nav>

      {error ? <div className="toast">{error}</div> : null}

      <main className="shell">
        {activeTab === "overview" ? (
          <Overview
            workers={workerData}
            executions={executionData}
            queues={queueData}
            history={historyData}
            flow={flowData}
            alerts={alerts}
            agentHealth={agentHealth}
            apiHealth={apiHealth}
          />
        ) : null}
        {activeTab === "workers" ? (
          <Workers data={workerData} onWorkerClick={navigateToExecutions} />
        ) : null}
        {activeTab === "queues" ? <Queues data={queueData} /> : null}
        {activeTab === "executions" ? (
          <Executions
            data={executionData}
            agentHealth={agentHealth}
            filterWorker={filterWorker}
            filterStatus={filterStatus}
            filterAgent={filterAgent}
            setFilterWorker={setFilterWorker}
            setFilterStatus={setFilterStatus}
            setFilterAgent={setFilterAgent}
          />
        ) : null}
        {activeTab === "sessions" ? (
          <Sessions
            sessionData={sessionData}
            sessionId={sessionId}
            setSessionId={setSessionId}
            traceId={traceId}
            setTraceId={setTraceId}
            traceData={traceData}
            loadSession={loadSession}
          />
        ) : null}
      </main>
    </div>
  );
}

function Overview({
  workers,
  executions,
  queues,
  history,
  flow,
  alerts,
  agentHealth,
  apiHealth,
}) {
  const totals = workers?.totals || {};
  const health = healthFromAlerts(alerts);
  return (
    <>
      <section className={`health-banner ${health.status}`}>
        <div>
          <span>集群健康</span>
          <strong>{healthLabel(health.status)}</strong>
          <p>{health.summary}</p>
        </div>
        <div className="health-score">
          <span>Score</span>
          <strong>{health.score}</strong>
        </div>
      </section>
      <section className="metric-grid">
        <Metric label="在线 Workers" value={totals.workers_online ?? 0} />
        <Metric label="Agent 类型" value={totals.agent_types ?? 0} />
        <Metric label="活跃任务" value={totals.active_executions ?? 0} />
        <Metric label="追踪任务" value={totals.tracked_executions ?? 0} />
      </section>
      <Panel title="数据流拓扑" icon={GitBranch}>
        <DataFlow flow={flow?.data_flow} />
      </Panel>
      <section className="layout-two">
        <Panel title="执行状态分布">
          <StatusBars counts={workers?.status_counts || {}} />
        </Panel>
        <Panel title="健康告警" icon={AlertTriangle}>
          <AlertList alerts={alerts} />
        </Panel>
      </section>
      <section className="layout-two">
        <Panel title="延迟" icon={Clock3}>
          <Latency latency={executions?.latency || {}} />
        </Panel>
        <Panel title="队列压力">
          <QueueSummary queues={queues?.queues || {}} />
        </Panel>
      </section>
      <Panel title="Dashboard runtime">
        <DashboardRuntime health={apiHealth} />
      </Panel>
      <Panel title="实时趋势">
        <Trends points={history?.points || []} />
      </Panel>
      <Panel title="Agent 健康状态">
        <AgentHealth agents={agentHealth} />
      </Panel>
    </>
  );
}

function DataFlow({ flow }) {
  const nodes = flow?.nodes || [];
  const edges = flow?.edges || [];
  const summary = flow?.summary || {};
  if (!nodes.length) return <Empty text="等待后端数据流模型" />;
  return (
    <div className="data-flow">
      <div className="flow-summary">
        <Metric label="总队列深度" value={summary.queue_depth_total ?? 0} compact />
        <Metric label="消费者 Pending" value={summary.consumer_pending_total ?? 0} compact />
        <Metric label="在线 Workers" value={summary.workers_online ?? 0} compact />
        <Metric
          label="端到端 P95"
          value={formatDuration(summary.total_latency_p95_ms)}
          compact
        />
      </div>
      <div className="flow-map" role="list" aria-label="系统数据流拓扑">
        {nodes.map((node, index) => (
          <React.Fragment key={node.id}>
            <article className={`flow-node ${node.status || "unknown"}`} role="listitem">
              <div className="flow-node-top">
                <span>{node.kind}</span>
                <strong>{node.label}</strong>
              </div>
              <span className={`flow-status ${node.status || "unknown"}`}>
                {flowStatusLabel(node.status)}
              </span>
              <div className="flow-node-metrics">
                {Object.entries(node.metrics || {}).map(([key, value]) => (
                  <span key={key}>
                    {metricLabel(key)} <b>{formatMetricValue(key, value)}</b>
                  </span>
                ))}
              </div>
            </article>
            {index < nodes.length - 1 ? (
              <span className="flow-arrow" aria-hidden="true">
                →
              </span>
            ) : null}
          </React.Fragment>
        ))}
      </div>
      <div className="flow-edges">
        {edges.map((edge) => (
          <article key={edge.id} className={`flow-edge ${edge.status || "unknown"}`}>
            <strong>{edge.label}</strong>
            <span>
              {edge.source} → {edge.target}
            </span>
            <code>
              {edge.metric_label}={formatInteger(edge.metric_value)}
            </code>
          </article>
        ))}
      </div>
    </div>
  );
}

function Workers({ data, onWorkerClick }) {
  const workers = data?.workers || [];
  return (
    <Panel
      title="Workers"
      subtitle={data?.worker_scan ? scanSummary(data.worker_scan) : ""}
    >
      <div className="worker-grid">
        {workers.length ? (
          workers.map((worker) => (
            <article
              key={worker.worker_id}
              className="worker-card clickable"
              onClick={() => onWorkerClick(worker.worker_id)}
              title="点击查看该 Worker 的执行记录"
            >
              <div className="worker-top">
                <div>
                  <h3>{worker.worker_id}</h3>
                  <p>{(worker.agent_types || []).join(", ") || "无 Agent 类型"}</p>
                </div>
                <span className="status-pill">在线</span>
              </div>
              <span className="pill">最后活跃 {formatTime(worker.last_seen)}</span>
              <div className="mini-grid">
                <Metric label="活跃" value={worker.active_count ?? 0} compact />
                <Metric label="总计" value={worker.total_tracked ?? 0} compact />
                <Metric label="失败" value={worker.counts?.failed ?? 0} compact />
              </div>
            </article>
          ))
        ) : (
          <Empty text="未发现在线 Workers" />
        )}
      </div>
    </Panel>
  );
}

function Queues({ data }) {
  return (
    <Panel title="队列">
      <QueueRows queues={data?.queues || {}} />
    </Panel>
  );
}

function Executions({
  data,
  agentHealth,
  filterWorker,
  filterStatus,
  filterAgent,
  setFilterWorker,
  setFilterStatus,
  setFilterAgent,
}) {
  const filteredExecutions = useMemo(() => {
    let execs = data?.recent_executions || [];
    if (filterWorker) execs = execs.filter((e) => e.worker_id === filterWorker);
    if (filterStatus) execs = execs.filter((e) => e.status === filterStatus);
    if (filterAgent)
      execs = execs.filter((e) =>
        (e.target_agent_type || "").toLowerCase().includes(filterAgent.toLowerCase()),
      );
    return execs;
  }, [data, filterWorker, filterStatus, filterAgent]);

  const hasFilter = filterWorker || filterStatus || filterAgent;

  return (
    <>
      <section className="layout-two">
        <Panel title="失败详情">
          <Failures failures={data?.failures || {}} />
        </Panel>
        <Panel title="延迟" icon={Clock3}>
          <Latency latency={data?.latency || {}} />
        </Panel>
      </section>
      <Panel title="Agent 执行健康状态">
        <AgentHealth agents={agentHealth} />
      </Panel>
      <Panel title="最近执行记录">
        <div className="filter-row">
          <label>
            <span>Worker</span>
            <input
              value={filterWorker}
              onChange={(e) => setFilterWorker(e.target.value)}
              placeholder="按 Worker ID 过滤"
            />
          </label>
          <label>
            <span>状态</span>
            <select value={filterStatus} onChange={(e) => setFilterStatus(e.target.value)}>
              <option value="">全部状态</option>
              {STATUS_ORDER.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Agent 类型</span>
            <input
              value={filterAgent}
              onChange={(e) => setFilterAgent(e.target.value)}
              placeholder="按 Agent 类型过滤"
            />
          </label>
          {hasFilter ? (
            <button
              type="button"
              className="btn-clear"
              onClick={() => {
                setFilterWorker("");
                setFilterStatus("");
                setFilterAgent("");
              }}
            >
              清除过滤
            </button>
          ) : null}
        </div>
        <ExecutionTable executions={filteredExecutions} totalCount={data?.recent_executions?.length ?? 0} />
      </Panel>
    </>
  );
}

function Sessions({
  sessionData,
  sessionId,
  setSessionId,
  traceId,
  setTraceId,
  traceData,
  loadSession,
}) {
  const executions = sessionData?.executions || [];
  const totals = sessionData?.totals || {};
  return (
    <Panel title="会话详情">
      <form className="session-form" onSubmit={loadSession}>
        <label>
          <span>Session ID</span>
          <input
            value={sessionId}
            onChange={(event) => setSessionId(event.target.value)}
            placeholder="sess-..."
          />
        </label>
        <label>
          <span>Trace ID</span>
          <input
            value={traceId}
            onChange={(event) => setTraceId(event.target.value)}
            placeholder="可选"
          />
        </label>
        <button type="submit">查询</button>
      </form>
      {sessionData ? (
        <section className="session-summary">
          <Metric label="执行节点" value={totals.executions ?? executions.length} compact />
          <Metric label="数据事件" value={totals.events ?? 0} compact />
          <Metric label="Trace" value={sessionData.trace_id || traceId || "全部"} compact />
          <Metric label="更新时间" value={formatTime(sessionData.generated_at)} compact />
        </section>
      ) : null}
      <section className="session-section">
        <h3>调用瀑布</h3>
        <TraceWaterfall
          executions={executions}
          generatedAt={sessionData?.generated_at || Date.now()}
        />
      </section>
      <section className="session-section">
        <h3>Trace Span 瀑布</h3>
        <TraceSpanWaterfall trace={traceData} />
      </section>
      <div className="layout-two">
        <section>
          <h3>执行树</h3>
          <Tree nodes={sessionData?.execution_tree || []} />
        </section>
        <section>
          <h3>时间线</h3>
          <Timeline items={sessionData?.timeline || []} />
        </section>
      </div>
    </Panel>
  );
}

function TraceSpanWaterfall({ trace }) {
  const timeline = trace?.timeline || [];
  if (!timeline.length) return <Empty text="输入 Trace ID 后将显示 Span 瀑布" />;
  const duration = Math.max(1, Number(trace.duration_ms || 0));
  return (
    <div className="span-waterfall">
      <div className="trace-topline">
        <strong>{trace.trace_id}</strong>
        <span className={`pill status-badge-${trace.status?.toLowerCase()}`}>
          {trace.status}
        </span>
        <span>{formatDuration(duration)}</span>
        <span>{trace.span_count ?? timeline.length} spans</span>
      </div>
      <div className="span-scale" aria-hidden="true">
        <span>0 ms</span>
        <span>{formatDuration(Math.round(duration / 2))}</span>
        <span>{formatDuration(duration)}</span>
      </div>
      <div className="span-rows" role="list" aria-label="Trace Span 瀑布">
        {timeline.map((span) => {
          const left = percent(span.offset_ms, duration);
          const width = Math.max(1.5, percent(span.duration_ms || 1, duration));
          return (
            <article key={span.span_id} className="span-row" role="listitem">
              <div className="span-label">
                <strong>{span.operation}</strong>
                <span>{span.component}</span>
                <code>{span.worker_id || span.execution_id || span.message_id}</code>
              </div>
              <div className="span-track">
                <div
                  className={`span-bar component-${componentClass(span.component)}`}
                  style={{ left: `${left}%`, width: `${width}%` }}
                  title={`${span.operation} · ${formatDuration(span.duration_ms)}`}
                >
                  {formatDuration(span.duration_ms)}
                </div>
              </div>
              <div className="span-meta">
                <span className={`pill status-badge-${span.status?.toLowerCase()}`}>
                  {span.status}
                </span>
                <small>{span.event_type || span.target_agent_type || "span"}</small>
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

function TraceWaterfall({ executions, generatedAt }) {
  const rows = useMemo(
    () => buildTraceRows(executions, generatedAt),
    [executions, generatedAt],
  );
  if (!rows.length) return <Empty text="查询会话后将显示调用瀑布" />;

  const minStart = Math.min(...rows.map((row) => row.start));
  const maxEnd = Math.max(...rows.map((row) => row.end));
  const range = Math.max(1, maxEnd - minStart);

  return (
    <div className="waterfall" role="list" aria-label="Trace 调用瀑布">
      <div className="waterfall-scale" aria-hidden="true">
        <span>0 ms</span>
        <span>{formatDuration(Math.round(range / 2))}</span>
        <span>{formatDuration(range)}</span>
      </div>
      {rows.map((row) => {
        const left = percent(row.start - minStart, range);
        const width = Math.max(1.5, percent(row.end - row.start, range));
        const queueLeft = percent(row.queueStart - row.start, row.duration);
        const queueWidth = Math.max(0, percent(row.queueEnd - row.queueStart, row.duration));
        const runLeft = percent(row.runStart - row.start, row.duration);
        const runWidth = Math.max(1.5, percent(row.runEnd - row.runStart, row.duration));
        return (
          <article key={row.key} className="waterfall-row" role="listitem">
            <div className="waterfall-meta">
              <strong>{row.agent}</strong>
              <span>{row.worker}</span>
              <code>{row.executionId}</code>
            </div>
            <div className="waterfall-track">
              <div
                className={`waterfall-bar status-badge-${row.status.toLowerCase()}`}
                style={{ left: `${left}%`, width: `${width}%` }}
                title={`${row.status} · 总耗时 ${formatDuration(row.duration)}`}
              >
                {queueWidth ? (
                  <span
                    className="waterfall-segment queue"
                    style={{ left: `${queueLeft}%`, width: `${queueWidth}%` }}
                    title={`队列等待 ${formatDuration(row.queueDuration)}`}
                  />
                ) : null}
                <span
                  className="waterfall-segment run"
                  style={{ left: `${runLeft}%`, width: `${runWidth}%` }}
                  title={`运行耗时 ${formatDuration(row.runDuration)}`}
                />
              </div>
            </div>
            <div className="waterfall-numbers">
              <span className={`pill status-badge-${row.status.toLowerCase()}`}>
                {row.status}
              </span>
              <strong>{formatDuration(row.duration)}</strong>
              <small>
                queue {formatDuration(row.queueDuration)} · run{" "}
                {formatDuration(row.runDuration)}
              </small>
            </div>
          </article>
        );
      })}
      <div className="waterfall-legend">
        <span>
          <i className="queue" /> 队列等待
        </span>
        <span>
          <i className="run" /> Worker 执行
        </span>
      </div>
    </div>
  );
}

function Metric({ label, value, compact = false }) {
  return (
    <article className={compact ? "metric compact" : "metric"}>
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function ApiHealthPill({ health }) {
  const degraded = health.status !== "ok";
  return (
    <span
      className={`api-health-pill ${degraded ? "degraded" : "ok"}`}
      title={degraded ? health.last_error_message : "Dashboard API 正常"}
    >
      API {degraded ? `异常 ${health.api_error_count}` : "正常"}
      {degraded && health.last_error_type ? ` · ${health.last_error_type}` : ""}
    </span>
  );
}

function DashboardRuntime({ health }) {
  if (!health) return <Empty text="等待 dashboard runtime 数据" />;
  const slowestRoute = [...(health.routes || [])].sort(
    (left, right) => (right.max_duration_ms || 0) - (left.max_duration_ms || 0),
  )[0];
  return (
    <div className="runtime-grid">
      <Metric label="API 状态" value={health.status === "ok" ? "正常" : "异常"} compact />
      <Metric label="成功请求" value={health.api_success_count ?? 0} compact />
      <Metric label="错误请求" value={health.api_error_count ?? 0} compact />
      <Metric label="运行时长" value={formatDuration(health.uptime_ms)} compact />
      <article className="runtime-detail">
        <span>最近错误</span>
        <strong>{health.last_error_type || "无"}</strong>
        <code>{health.last_error_route || "no route"}</code>
      </article>
      <article className="runtime-detail">
        <span>最慢路由</span>
        <strong>
          {slowestRoute
            ? `${slowestRoute.route} · ${formatDuration(slowestRoute.max_duration_ms)}`
            : "无数据"}
        </strong>
        <code>{slowestRoute ? `errors=${slowestRoute.error_count}` : "no route stats"}</code>
      </article>
    </div>
  );
}

function Panel({ title, subtitle = "", icon: Icon, children }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>
            {Icon ? <Icon size={18} /> : null}
            {title}
          </h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </div>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function StatusBars({ counts }) {
  const max = Math.max(1, ...Object.values(counts));
  const rows = STATUS_ORDER.filter((status) => counts[status]);
  return rows.length ? (
    <div className="bars">
      {rows.map((status) => (
        <div key={status} className="bar-row">
          <span>{status}</span>
          <div className="bar-track">
            <div
              className={`bar-fill status-${status.toLowerCase()}`}
              style={{ width: `${Math.max(4, (counts[status] / max) * 100)}%` }}
            />
          </div>
          <strong>{counts[status]}</strong>
        </div>
      ))}
    </div>
  ) : (
    <Empty text="暂无执行状态数据" />
  );
}

function AlertList({ alerts }) {
  return alerts.length ? (
    <div className="stack">
      {alerts.map((alert, index) => (
        <article key={`${alert.code}-${index}`} className="alert-row">
          <span className={`severity ${alert.severity || "info"}`}>
            {alert.severity === "critical" ? "严重" : alert.severity === "warning" ? "警告" : "信息"}
          </span>
          <div>
            <strong>{alert.message}</strong>
            <code>{alert.code}</code>
            {alert.value !== undefined && alert.threshold !== undefined ? (
              <span className="alert-meta">
                当前 {formatInteger(alert.value)} / 阈值 {formatInteger(alert.threshold)}
              </span>
            ) : null}
          </div>
        </article>
      ))}
    </div>
  ) : (
    <Empty text="无活跃健康告警" />
  );
}

function Latency({ latency }) {
  const queue = latency.queue || {};
  const run = latency.run || latency;
  const total = latency.total || {};
  return (
    <div className="mini-grid">
      <Metric label="队列等待 P95" value={formatDuration(queue.p95_ms)} compact />
      <Metric label="运行耗时 P95" value={formatDuration(run.p95_ms)} compact />
      <Metric label="端到端 P95" value={formatDuration(total.p95_ms)} compact />
      <Metric label="运行均值" value={formatDuration(run.avg_ms)} compact />
    </div>
  );
}

function QueueSummary({ queues }) {
  const all = allQueues(queues);
  const depth = all.reduce((sum, queue) => sum + Number(queue.length || 0), 0);
  const pending = all.reduce(
    (sum, queue) =>
      sum +
      (queue.consumer_groups || []).reduce(
        (groupSum, group) => groupSum + Number(group.pending || 0),
        0,
      ),
    0,
  );
  return (
    <div className="mini-grid">
      <Metric label="Stream 数" value={all.length} compact />
      <Metric label="队列深度" value={depth} compact />
      <Metric label="待处理" value={pending} compact />
    </div>
  );
}

function QueueRows({ queues }) {
  const rows = allQueues(queues);
  return rows.length ? (
    <div className="queue-list">
      {rows.map((queue) => {
        const pending = (queue.consumer_groups || []).reduce(
          (sum, group) => sum + Number(group.pending || 0),
          0,
        );
        return (
          <article key={`${queue.name}-${queue.stream}`} className="queue-row">
            <div>
              <strong>{queue.name}</strong>
              <code>{queue.stream}</code>
              <div className="chips">
                {(queue.consumer_groups || []).map((group) => (
                  <span key={group.name}>
                    {group.name} 待处理={group.pending ?? 0}
                    {group.lag === null || group.lag === undefined
                      ? ""
                      : ` 滞后=${group.lag}`}
                  </span>
                ))}
              </div>
            </div>
            <div className="queue-numbers">
              <span>{queue.length ?? "n/a"}</span>
              <span className={pending ? "pending hot" : "pending"}>{pending}</span>
            </div>
          </article>
        );
      })}
    </div>
  ) : (
    <Empty text="未发现队列 Stream" />
  );
}

function Failures({ failures }) {
  const recent = failures.recent || [];
  return recent.length ? (
    <div className="stack">
      {recent.map((failure) => (
        <article key={failure.execution_id} className="failure-row">
          <div>
            <strong>{failure.error_type || "未知错误"}</strong>
            <span>{failure.failed_stage || "失败"}</span>
          </div>
          <p>{failure.error_message}</p>
          <code>
            {failure.execution_id} {failure.target_agent_type}
          </code>
        </article>
      ))}
    </div>
  ) : (
    <Empty text="无最近失败记录" />
  );
}

function AgentHealth({ agents }) {
  return agents.length ? (
    <div className="agent-list">
      {agents.map((agent) => (
        <article key={agent.agent_type} className="agent-row">
          <div>
            <strong>{agent.agent_type}</strong>
            <span>{agent.worker_count ?? 0} Workers</span>
          </div>
          <div className="mini-grid">
            <Metric label="队列深度" value={agent.queue_depth ?? 0} compact />
            <Metric label="活跃" value={agent.recent_active_executions ?? 0} compact />
            <Metric label="失败" value={agent.recent_failed_executions ?? 0} compact />
            <Metric label="近期" value={agent.recent_executions ?? 0} compact />
          </div>
        </article>
      ))}
    </div>
  ) : (
    <Empty text="暂无 Agent 健康数据" />
  );
}

function ExecutionTable({ executions, totalCount }) {
  const filtered = executions.length;
  const showCount = totalCount > 0 && filtered !== totalCount;
  return executions.length ? (
    <>
      {showCount ? (
        <p className="filter-count">
          显示 {filtered} / {totalCount} 条记录
        </p>
      ) : null}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>执行 ID</th>
              <th>Worker</th>
              <th>Agent</th>
              <th>状态</th>
              <th>延迟</th>
              <th>路由</th>
              <th>更新时间</th>
            </tr>
          </thead>
          <tbody>
            {executions.map((execution) => (
              <tr key={execution.execution_id}>
                <td>
                  <code>{execution.execution_id}</code>
                  <br />
                  <span>{execution.message_id}</span>
                </td>
                <td>{execution.worker_id}</td>
                <td>{execution.target_agent_type}</td>
                <td>
                  <span className={`pill status-badge-${execution.status?.toLowerCase()}`}>
                    {execution.status}
                  </span>
                </td>
                <td>
                  队列 {formatDuration(execution.queue_latency_ms)}
                  <br />
                  运行 {formatDuration(execution.run_latency_ms)}
                </td>
                <td>
                  {execution.route_status}
                  <br />
                  <span>{execution.route_policy}</span>
                </td>
                <td>{formatTime(execution.updated_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  ) : (
    <Empty text="暂无执行记录" />
  );
}

function Trends({ points }) {
  const series = [
    ["队列深度", "queue_depth_total", "#4058d6", formatInteger],
    ["待处理", "consumer_pending_total", "#b7791f", formatInteger],
    ["活跃", "active_executions", "#16875d", formatInteger],
    ["失败", "failed_executions", "#c53030", formatInteger],
    ["运行 P95", "latency_p95_ms", "#805ad5", formatDuration],
    ["总 P95", "total_latency_p95_ms", "#dd6b20", formatDuration],
  ];
  return points.length ? (
    <div className="trend-grid">
      {series.map(([label, key, color, formatter]) => (
        <article key={key} className="trend-card">
          <div>
            <span>{label}</span>
            <strong>{formatter(points[points.length - 1]?.[key] ?? 0)}</strong>
          </div>
          <svg viewBox="0 0 120 36" role="img" aria-label={`${label} 趋势`}>
            <path
              d={sparklinePath(points, key)}
              fill="none"
              stroke={color}
              strokeWidth="2.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </article>
      ))}
    </div>
  ) : (
    <Empty text="收集快照后将显示趋势历史" />
  );
}

function Tree({ nodes }) {
  return nodes.length ? (
    <div className="tree">
      {nodes.map((node) => (
        <article key={node.execution_id} className="tree-node">
          <div>
            <strong>{node.target_agent_type || "未知 Agent"}</strong>
            <span className="pill">{node.status || "未知"}</span>
          </div>
          <code>
            {node.execution_id}
            <br />
            message={node.message_id}
          </code>
          {node.children?.length ? <Tree nodes={node.children} /> : null}
        </article>
      ))}
    </div>
  ) : (
    <Empty text="该会话无执行记录" />
  );
}

function Timeline({ items }) {
  return items.length ? (
    <div className="timeline">
      {items.map((item, index) => (
        <article key={`${item.timestamp}-${index}`} className="timeline-row">
          <span>{formatTime(item.timestamp)}</span>
          <div>
            <strong>{item.status || item.event_type || item.kind}</strong>
            <code>{item.execution_id || item.stream_id || item.message_id}</code>
          </div>
        </article>
      ))}
    </div>
  ) : (
    <Empty text="无时间线条目" />
  );
}

function Empty({ text }) {
  return <p className="empty-state">{text}</p>;
}

function allQueues(queues) {
  const agentRows = (queues.agent_type_streams || []).map((queue) => ({
    ...queue,
    name: queue.agent_type,
  }));
  const controlRows = Object.entries(queues.control_plane || {}).map(
    ([name, queue]) => ({ ...queue, name }),
  );
  return [...agentRows, ...controlRows];
}

function mergeAgentHealthQueues(agentHealth, queues) {
  const depthByAgent = Object.fromEntries(
    (queues.agent_type_streams || []).map((queue) => [
      queue.agent_type,
      queue.length ?? 0,
    ]),
  );
  return agentHealth.map((agent) => ({
    ...agent,
    queue_depth: depthByAgent[agent.agent_type] ?? agent.queue_depth ?? 0,
  }));
}

function buildTraceRows(executions, generatedAt) {
  return executions
    .map((execution) => {
      const created = Number(execution.created_at || 0);
      const started = Number(execution.started_at || 0);
      const finished = Number(execution.finished_at || 0);
      const updated = Number(execution.updated_at || 0);
      const fallbackEnd = Number(generatedAt || Date.now());
      const start = firstPositive(created, started, updated, fallbackEnd);
      const runStart = firstPositive(started, created, updated, fallbackEnd);
      const end = Math.max(
        start,
        firstPositive(finished, updated, generatedAt, runStart),
      );
      const queueStart = created || start;
      const queueEnd = started && started >= queueStart ? started : queueStart;
      const runEnd = Math.max(runStart, finished || updated || generatedAt || runStart);
      const rowStart = Math.min(start, queueStart, runStart);
      const rowEnd = Math.max(end, queueEnd, runEnd);
      const status = String(execution.status || "UNKNOWN");
      return {
        key:
          execution.execution_id ||
          execution.message_id ||
          `${execution.worker_id}-${start}`,
        executionId: execution.execution_id || execution.message_id || "unknown",
        agent: execution.target_agent_type || "unknown agent",
        worker: execution.worker_id || "unknown worker",
        status,
        start: rowStart,
        end: rowEnd,
        duration: Math.max(1, rowEnd - rowStart),
        queueStart,
        queueEnd,
        runStart,
        runEnd,
        queueDuration: Math.max(0, queueEnd - queueStart),
        runDuration: Math.max(0, runEnd - runStart),
      };
    })
    .sort((left, right) => left.start - right.start);
}

function firstPositive(...values) {
  return values.find((value) => Number(value) > 0) || 0;
}

function percent(value, total) {
  return Math.min(100, Math.max(0, (Number(value || 0) / Math.max(1, total)) * 100));
}

function deriveTraceId(session) {
  if (session?.trace_id) return session.trace_id;
  const executionTrace = (session?.executions || []).find((execution) => execution.trace_id);
  if (executionTrace?.trace_id) return executionTrace.trace_id;
  const eventTrace = (session?.recent_events || []).find((event) => event.trace_id);
  return eventTrace?.trace_id || "";
}

function componentClass(component) {
  return String(component || "unknown").replace(/[^a-z0-9_-]/gi, "-").toLowerCase();
}

function scanSummary(scan) {
  return `${scan.source || "worker_scan"}：已扫描 ${scan.scanned_workers ?? 0} / ${scan.known_workers ?? 0} 个 Workers${scan.truncated ? "（已截断）" : ""}`;
}

function formatTime(value) {
  const numeric = Number(value);
  if (!numeric) return "未知";
  const date = new Date(numeric);
  const now = new Date();
  const isToday = date.toDateString() === now.toDateString();
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  const isYesterday = date.toDateString() === yesterday.toDateString();
  const timePart = date.toLocaleTimeString("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  if (isToday) return timePart;
  if (isYesterday) return `昨天 ${timePart}`;
  const datePart = date.toLocaleDateString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
  });
  return `${datePart} ${timePart}`;
}

function formatDuration(value) {
  const numeric = Number(value || 0);
  if (numeric <= 0) return "0 ms";
  if (numeric < 1000) return `${Math.round(numeric)} ms`;
  return `${(numeric / 1000).toFixed(1)} s`;
}

function formatInteger(value) {
  return String(Math.round(Number(value || 0)));
}

function healthFromAlerts(alerts) {
  const criticalAlerts = alerts.filter((alert) => alert.severity === "critical").length;
  const warningAlerts = alerts.filter((alert) => alert.severity === "warning").length;
  const score = Math.max(0, 100 - criticalAlerts * 40 - warningAlerts * 10);
  if (criticalAlerts) {
    return {
      status: "critical",
      score,
      summary: `${criticalAlerts} 个严重告警，${warningAlerts} 个警告告警`,
    };
  }
  if (warningAlerts) {
    return {
      status: "warning",
      score,
      summary: `${warningAlerts} 个警告告警`,
    };
  }
  return {
    status: "healthy",
    score,
    summary: "无活跃健康告警",
  };
}

function dedupeAlerts(alerts) {
  const seen = new Set();
  return alerts.filter((alert) => {
    const key = [
      alert.code || "",
      alert.severity || "",
      alert.message || "",
      alert.value ?? "",
      alert.threshold ?? "",
    ].join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function healthLabel(status) {
  if (status === "critical") return "严重";
  if (status === "warning") return "需要关注";
  return "健康";
}

function flowStatusLabel(status) {
  if (status === "critical") return "严重";
  if (status === "warning") return "关注";
  if (status === "healthy") return "健康";
  return "未观测";
}

function metricLabel(key) {
  const labels = {
    active_executions: "活跃",
    consumer_pending: "Pending",
    control_queue_depth: "控制队列",
    deadletters: "死信",
    failed_executions: "失败",
    observable_from_framework: "框架观测",
    pending_deliveries: "待投递",
    queue_depth: "队列深度",
    queue_latency_p95_ms: "队列 P95",
    recent_events: "近期事件",
    run_latency_p95_ms: "运行 P95",
    total_latency_p95_ms: "总 P95",
    tracked_executions: "追踪任务",
    workers_online: "在线",
  };
  return labels[key] || key;
}

function formatMetricValue(key, value) {
  if (key.endsWith("_ms")) return formatDuration(value);
  return formatInteger(value);
}

function sparklinePath(points, key) {
  const values = points.map((point) => Number(point[key] || 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(1, max - min);
  const xStep = points.length > 1 ? 118 / (points.length - 1) : 0;
  return values
    .map((value, index) => {
      const x = 1 + index * xStep;
      const y = 34 - ((value - min) / range) * 32;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

createRoot(document.getElementById("root")).render(<App />);
