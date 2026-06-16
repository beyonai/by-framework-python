import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  Bell,
  Boxes,
  CheckCircle2,
  ChevronRight,
  Clock3,
  Download,
  Eye,
  Gauge,
  GitBranch,
  Home,
  Layers3,
  ListFilter,
  Menu,
  MoreVertical,
  Pause,
  Play,
  RefreshCw,
  Search,
  Server,
  Settings,
  Users,
  Workflow,
} from "lucide-react";
import "./styles.css";

const TABS = [
  { id: "overview", label: "总览", icon: Home },
  { id: "workers", label: "Workers", icon: Users },
  { id: "queues", label: "队列与 Streams", icon: Boxes },
  { id: "executions", label: "告警与分析", icon: Bell },
  { id: "sessions", label: "会话", icon: GitBranch },
  { id: "settings", label: "配置", icon: Settings },
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

const NODE_ICONS = {
  client: Activity,
  control_queues: Layers3,
  workers: Users,
  data_stream: Workflow,
  websocket_backend: Gauge,
  control_plane: Settings,
};

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

async function postJson(path, body = {}) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${path} 请求失败: ${response.status}`);
  }
  return response.json();
}

function endpoint(path) {
  if (!isDemoMode()) return path;
  return `${path}${path.includes("?") ? "&" : "?"}demo=1`;
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
  const [configData, setConfigData] = useState(null);
  const [metricCatalog, setMetricCatalog] = useState(null);
  const [actionData, setActionData] = useState(null);
  const [sessionId, setSessionId] = useState(isDemoMode() ? "sess-demo" : "");
  const [traceId, setTraceId] = useState("");
  const [error, setError] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(0);
  const [paused, setPaused] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(5000);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [modal, setModal] = useState(null);

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
      const actionPromise = fetchJson(endpoint("/api/actions"));
      const healthPromise = fetchJson("/api/health");
      const workers = await workerPromise;
      setWorkerData(workers);
      const [executions, flow, history, actions, runtimeHealth] = await Promise.all([
        executionPromise,
        flowPromise,
        historyPromise,
        actionPromise,
        healthPromise,
      ]);
      setExecutionData(executions);
      setFlowData(flow);
      setHistoryData(history);
      setActionData(actions);
      setApiHealth(runtimeHealth);

      const params = new URLSearchParams();
      if (isDemoMode()) params.set("demo", "1");
      params.set("consumer_details", "1");
      (workers.agent_types || []).forEach((agentType) =>
        params.append("agent_type", agentType),
      );
      const query = params.toString();
      setQueueData(await fetchJson(`/api/queues${query ? `?${query}` : ""}`));
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

  const pageTitle =
    activeTab === "executions"
      ? "Redis Streams 工作集群监控 / 告警与分析"
      : "Redis Streams 工作集群监控";

  const navigateToExecutions = (workerId) => {
    setFilterWorker(workerId);
    setFilterStatus("");
    setFilterAgent("");
    setActiveTab("executions");
  };

  const handleWorkerAction = async (action, workerId) => {
    if (isDemoMode()) return;
    try {
      await postJson(`/api/admin/worker/${encodeURIComponent(workerId)}/${action}`);
      setWorkerData(await fetchJson("/api/workers"));
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const loadConfig = async () => {
    try {
      setConfigData(await fetchJson(endpoint("/api/config")));
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const loadMetricCatalog = async () => {
    try {
      setMetricCatalog(await fetchJson(endpoint("/api/metrics/catalog")));
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const exportCurrentView = async () => {
    const scopeByTab = {
      overview: "snapshot",
      workers: "workers",
      queues: "queues",
      executions: "alerts",
      sessions: "snapshot",
      settings: "snapshot",
    };
    const scope = scopeByTab[activeTab] || "snapshot";
    try {
      const data = await fetchJson(endpoint(`/api/export?scope=${scope}`));
      downloadJson(data, `by-framework-${scope}-${new Date().toISOString()}.json`);
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const openQueueDetail = async (queueName) => {
    try {
      const detail = await fetchJson(endpoint(`/api/queues/${encodeURIComponent(queueName)}`));
      setModal({ kind: "queue", title: `队列详情：${detail.queue.name}`, payload: detail });
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const openWorkerDetail = async (workerId) => {
    try {
      const detail = await fetchJson(endpoint(`/api/workers/${encodeURIComponent(workerId)}`));
      setModal({ kind: "worker", title: `Worker 详情：${detail.worker.worker_id}`, payload: detail });
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const openExecutionDetail = async (executionId) => {
    try {
      const detail = await fetchJson(endpoint(`/api/executions/${encodeURIComponent(executionId)}`));
      setModal({ kind: "execution", title: `执行详情：${detail.execution.execution_id}`, payload: detail });
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const openAlertCenter = async () => {
    try {
      const detail = await fetchJson(endpoint("/api/alerts"));
      setModal({ kind: "alerts", title: "告警中心详情", payload: detail });
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const openActionDetail = async (actionId) => {
    try {
      const detail = await fetchJson(endpoint(`/api/actions/${encodeURIComponent(actionId)}`));
      setModal({ kind: "action", title: `处理事项：${detail.action.title}`, payload: detail });
    } catch (err) {
      setError(err.message || String(err));
    }
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
      if (!selectedTraceId) {
        setTraceData(null);
        return;
      }
      const traceParams = new URLSearchParams();
      if (sessionId.trim()) traceParams.set("session_id", sessionId.trim());
      if (isDemoMode()) traceParams.set("demo", "1");
      const query = traceParams.toString();
      setTraceData(
        await fetchJson(
          `/api/trace/${encodeURIComponent(selectedTraceId)}${query ? `?${query}` : ""}`,
        ),
      );
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  return (
    <div className="app-shell">
      <Sidebar
        activeTab={activeTab}
        setActiveTab={(tab) => {
          setActiveTab(tab);
          setSidebarOpen(false);
        }}
        open={sidebarOpen}
      />
      <div className="workspace">
        <Topbar
          title={pageTitle}
          apiHealth={apiHealth}
          lastUpdated={lastUpdated}
          refreshInterval={refreshInterval}
          setRefreshInterval={setRefreshInterval}
          paused={paused}
          setPaused={setPaused}
          refresh={refresh}
          refreshing={refreshing}
          setSidebarOpen={setSidebarOpen}
          onExport={exportCurrentView}
        />
        {error ? <div className="toast" role="alert">{error}</div> : null}
        <main className="content">
          {activeTab === "overview" ? (
            <Overview
              workers={workerData}
              executions={executionData}
              queues={queueData}
              history={historyData}
              flow={flowData}
              alerts={alerts}
              actions={actionData?.actions || []}
              apiHealth={apiHealth}
              onOpenAlerts={openAlertCenter}
              onOpenAction={openActionDetail}
              onNavigate={setActiveTab}
            />
          ) : null}
          {activeTab === "workers" ? (
            <Workers
              data={workerData}
              history={historyData}
              alerts={alerts}
              onWorkerClick={navigateToExecutions}
              onWorkerOpen={openWorkerDetail}
              onWorkerAction={handleWorkerAction}
            />
          ) : null}
          {activeTab === "queues" ? (
            <Queues
              data={queueData}
              flow={flowData}
              history={historyData}
              onNavigate={setActiveTab}
              onQueueOpen={openQueueDetail}
            />
          ) : null}
          {activeTab === "executions" ? (
            <Executions
              data={executionData}
              agentHealth={agentHealth}
              alerts={alerts}
              actions={actionData?.actions || []}
              history={historyData}
              filterWorker={filterWorker}
              filterStatus={filterStatus}
              filterAgent={filterAgent}
              setFilterWorker={setFilterWorker}
              setFilterStatus={setFilterStatus}
              setFilterAgent={setFilterAgent}
              onOpenAlerts={openAlertCenter}
              onOpenAction={openActionDetail}
              onExecutionOpen={openExecutionDetail}
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
          {activeTab === "settings" ? (
            <SettingsView
              config={configData}
              metricCatalog={metricCatalog}
              onLoad={loadConfig}
              onLoadCatalog={loadMetricCatalog}
            />
          ) : null}
        </main>
        {modal ? <DetailModal modal={modal} onClose={() => setModal(null)} /> : null}
      </div>
    </div>
  );
}

function Sidebar({ activeTab, setActiveTab, open }) {
  return (
    <aside className={`sidebar ${open ? "open" : ""}`}>
      <div className="brand">
        <span className="brand-mark"><Workflow size={21} /></span>
        <strong>BY-FRAMEWORK</strong>
      </div>
      <nav className="side-nav" aria-label="Dashboard views">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              key={tab.id}
              type="button"
              className={activeTab === tab.id ? "side-item active" : "side-item"}
              onClick={() => !tab.disabled && setActiveTab(tab.id)}
              disabled={tab.disabled}
            >
              <Icon size={18} />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </nav>
      <div className="sidebar-footer">
        <button type="button" className="ghost-button" disabled title="当前版本仅提供浅色模式">
          <Activity size={16} />
          浅色模式
        </button>
        <button type="button" className="ghost-button" disabled title="移动端可通过菜单按钮收起">
          <ChevronRight size={16} />
          折叠菜单
        </button>
      </div>
    </aside>
  );
}

function Topbar({
  title,
  apiHealth,
  lastUpdated,
  refreshInterval,
  setRefreshInterval,
  paused,
  setPaused,
  refresh,
  refreshing,
  setSidebarOpen,
  onExport,
}) {
  return (
    <header className="topbar">
      <button
        type="button"
        className="icon-button mobile-menu"
        onClick={() => setSidebarOpen((value) => !value)}
        aria-label="打开导航"
      >
        <Menu size={18} />
      </button>
      <h1>{title}</h1>
      <div className="topbar-actions">
        {apiHealth ? <ApiHealthPill health={apiHealth} /> : null}
        <span className="toolbar-pill">
          <RefreshCw size={15} />
          更新于 {lastUpdated ? formatTime(lastUpdated) : "等待数据"}
        </span>
        <label className="select-shell" aria-label="刷新频率">
          <select
            value={refreshInterval}
            onChange={(event) => {
              const value = Number(event.target.value);
              setRefreshInterval(value);
              if (value === 0) setPaused(false);
            }}
          >
            {REFRESH_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className="secondary-button"
          onClick={() => setPaused((value) => !value)}
          disabled={refreshInterval === 0}
        >
          {paused ? <Play size={16} /> : <Pause size={16} />}
          {paused ? "继续" : "暂停"}
        </button>
        <button type="button" className="secondary-button" onClick={onExport}>
          <Download size={16} />
          导出
        </button>
        <button type="button" className="primary-button" onClick={refresh} disabled={refreshing}>
          <RefreshCw size={16} className={refreshing ? "spinning" : ""} />
          刷新
        </button>
      </div>
    </header>
  );
}

function Overview({
  workers,
  executions,
  queues,
  history,
  flow,
  alerts,
  actions,
  apiHealth,
  onOpenAlerts,
  onOpenAction,
  onNavigate,
}) {
  const totals = workers?.totals || {};
  const health = healthFromAlerts(alerts);
  const queueRows = allQueues(queues?.queues || {});
  const pending = totalPending(queueRows);
  return (
    <>
      <section className="hero-card">
        <div className="health-score-block">
          <span>集群健康评分</span>
          <strong className={`score-${health.status}`}>{health.score}</strong>
          <small>/100</small>
          <p>{health.summary}</p>
        </div>
        <div className="hero-alert">
          <span className="red-dot" />
          <div>
            <strong>{alerts[0]?.message || "未发现关键告警"}</strong>
            <p>{alerts[0] ? `${alerts[0].code} · ${severityLabel(alerts[0].severity)}` : "所有核心链路运行稳定"}</p>
          </div>
          <ChevronRight size={20} />
        </div>
        <div className="overview-facts">
          <Fact icon={Server} label="集群名称" value="production-cluster" />
          <Fact icon={Clock3} label="运行时长" value={formatDuration(apiHealth?.uptime_ms)} />
          <Fact icon={Workflow} label="版本" value="v1.0.0" />
        </div>
      </section>

      <section className="metric-grid six">
        <MetricCard icon={Users} tone="blue" label="在线 Workers" value={totals.workers_online ?? 0} meta={`预期 ${Math.max(totals.agent_types ?? 0, totals.workers_online ?? 0)}`} delta={totals.workers_online ? "正常" : "需处理"} />
        <MetricCard icon={CheckCircle2} tone="green" label="活跃任务" value={totals.active_executions ?? 0} meta="较 1 小时前" delta="0%" />
        <MetricCard icon={Clock3} tone="amber" label="待处理任务" value={pending} meta="Pending Delivery" delta={pending ? "关注" : "0%"} />
        <MetricCard icon={Activity} tone="violet" label="消费延迟 P95" value={formatDuration(executions?.latency?.total?.p95_ms)} meta="端到端" delta="实时" />
        <MetricCard icon={AlertTriangle} tone="red" label="错误率" value={`${errorRate(workers?.status_counts || {})}%`} meta="失败 / 总任务" delta={alerts.length ? "上升" : "稳定"} />
        <MetricCard icon={Gauge} tone="blue" label="吞吐量" value={`${Math.max(0, totals.active_executions ?? 0)} msg/s`} meta="当前窗口" delta="观测中" />
      </section>

      <section className="main-grid">
        <Panel title="数据流转管道">
          <DataFlow flow={flow?.data_flow} />
        </Panel>
        <Panel title="核心风险 / 重点告警" badge={alerts.length || 0}>
          <AlertList alerts={alerts.slice(0, 4)} onOpenAll={onOpenAlerts} />
        </Panel>
      </section>

      <section className="main-grid lower">
        <Panel title="近 1 小时趋势">
          <TrendDuo points={history?.points || []} />
        </Panel>
        <Panel title="待处理事项">
          <ActionList
            actions={actions}
            onOpenAction={onOpenAction}
            onNavigate={onNavigate}
          />
        </Panel>
      </section>
    </>
  );
}

function Workers({
  data,
  history,
  alerts,
  onWorkerClick,
  onWorkerOpen,
  onWorkerAction,
}) {
  const workers = data?.workers || [];
  const totals = data?.totals || {};
  const pools = buildWorkerPools(workers);
  return (
    <>
      <section className="metric-grid four">
        <MetricCard icon={Users} tone="blue" label="在线 Workers" value={`${totals.workers_online ?? 0} / ${Math.max(workers.length, totals.workers_online ?? 0)}`} meta="在线率" delta={`${workers.length ? Math.round(((totals.workers_online ?? 0) / workers.length) * 100) : 0}%`} />
        <MetricCard icon={Layers3} tone="green" label="预期容量" value={workers.length || 0} meta="总容量（实例）" delta="就绪" />
        <MetricCard icon={Workflow} tone="violet" label="活跃任务" value={totals.active_executions ?? 0} meta={`总任务 ${totals.tracked_executions ?? 0}`} delta="实时" />
        <MetricCard icon={AlertTriangle} tone="red" label="Worker 异常数" value={alerts.length} meta="异常率" delta={`${alerts.length ? "需处理" : "0%"}`} />
      </section>

      <Panel title="Worker Pool 健康概览" action="查看全部 Pool">
        <div className="pool-grid">
          {pools.map((pool) => (
            <PoolCard key={pool.name} pool={pool} />
          ))}
        </div>
      </Panel>

      <section className="workers-layout">
        <Panel title={`实例列表 (${workers.length})`}>
          <WorkerTable
            workers={workers}
            onWorkerClick={onWorkerClick}
            onWorkerOpen={onWorkerOpen}
            onWorkerAction={onWorkerAction}
          />
          <DenylistPanel agentTypes={data?.agent_types || []} />
        </Panel>
        <aside className="side-stack">
          <Panel title="容量与负载趋势">
            <SmallChart title="在线 Workers 趋势" points={history?.points || []} field="workers_online" color="#2563eb" />
            <SmallChart title="任务利用率趋势 (%)" points={history?.points || []} field="active_executions" color="#8b5cf6" percent />
          </Panel>
          <Panel title="异常分布">
            <AlertBreakdown alerts={alerts} />
          </Panel>
        </aside>
      </section>
    </>
  );
}

function Queues({ data, flow, history, onNavigate, onQueueOpen }) {
  const [queueFilter, setQueueFilter] = useState("");
  const queues = data?.queues || {};
  const rows = allQueues(queues);
  const visibleRows = rows.filter((queue) =>
    [queue.name, queue.stream, queue.queue_type]
      .join(" ")
      .toLowerCase()
      .includes(queueFilter.toLowerCase()),
  );
  const pending = totalPending(rows);
  const depth = rows.reduce((sum, queue) => sum + Number(queue.length || 0), 0);
  const deadletters = rows.find((queue) => queue.name === "deadletter")?.length || 0;
  return (
    <>
      <section className="view-tabs">
        <button type="button" className="view-tab active"><Workflow size={16} /> 队列与 Streams</button>
        <button type="button" className="view-tab" onClick={() => onNavigate("executions")}><Activity size={16} /> 执行趋势</button>
        <button type="button" className="view-tab" onClick={() => onNavigate("sessions")}><GitBranch size={16} /> 会话</button>
        <label className="queue-filter">
          <ListFilter size={15} />
          <input
            value={queueFilter}
            onChange={(event) => setQueueFilter(event.target.value)}
            placeholder="筛选队列 / Stream"
          />
        </label>
      </section>
      <section className="metric-grid six">
        <MetricCard icon={Workflow} tone="blue" label="Stream 数" value={rows.length} meta="较 1 小时前" delta="0%" />
        <MetricCard icon={Layers3} tone="green" label="队列积压（总）" value={depth} meta="较 1 小时前" delta={depth ? "上升" : "0%"} />
        <MetricCard icon={Clock3} tone="amber" label="Pending Delivery" value={pending} meta="待确认消息" delta={pending ? "关注" : "0"} />
        <MetricCard icon={AlertTriangle} tone="red" label="Deadletter（总）" value={deadletters} meta="死信队列" delta={deadletters ? "异常" : "0"} />
        <MetricCard icon={Activity} tone="violet" label="消费速率（总）" value={`${depth ? depth.toFixed(1) : "0.0"} /s`} meta="估算窗口" delta="实时" />
        <MetricCard icon={Clock3} tone="violet" label="Lag P95（总）" value={formatDuration(flow?.data_flow?.summary?.queue_latency_p95_ms)} meta="较 1 小时前" delta="观测" />
      </section>

      <Panel title="数据流拓扑">
        <StreamTopology flow={flow?.data_flow} />
      </Panel>

      <section className="split-grid">
        <Panel title="Streams / Queues 列表" action="清除筛选" onAction={() => setQueueFilter("")}>
          <QueueTable rows={visibleRows} onQueueOpen={onQueueOpen} />
        </Panel>
        <Panel title="Consumer Groups">
          <ConsumerTable rows={visibleRows} />
        </Panel>
      </section>

      <section className="main-grid lower">
        <Panel title="队列积压趋势（总）">
          <AreaTrend points={history?.points || []} fields={["queue_depth_total", "consumer_pending_total"]} />
        </Panel>
        <Panel title="异常投递 / 死信分析">
          <QueueInsights rows={rows} onQueueOpen={onQueueOpen} />
        </Panel>
      </section>
    </>
  );
}

function Executions({
  data,
  agentHealth,
  alerts,
  actions,
  history,
  filterWorker,
  filterStatus,
  filterAgent,
  setFilterWorker,
  setFilterStatus,
  setFilterAgent,
  onOpenAlerts,
  onOpenAction,
  onExecutionOpen,
}) {
  const filteredExecutions = useMemo(() => {
    let execs = data?.recent_executions || [];
    if (filterWorker) execs = execs.filter((item) => item.worker_id === filterWorker);
    if (filterStatus) execs = execs.filter((item) => item.status === filterStatus);
    if (filterAgent) {
      execs = execs.filter((item) =>
        (item.target_agent_type || "").toLowerCase().includes(filterAgent.toLowerCase()),
      );
    }
    return execs;
  }, [data, filterWorker, filterStatus, filterAgent]);
  const failures = data?.failures?.recent || [];
  const recovered = Math.max(0, (data?.status_counts?.COMPLETED || 0) - failures.length);
  return (
    <>
      <section className="metric-grid five">
        <MetricCard icon={AlertTriangle} tone="red" label="严重告警" value={alerts.filter((a) => a.severity === "critical").length} meta="较昨日" delta="+2" />
        <MetricCard icon={Clock3} tone="amber" label="警告" value={alerts.filter((a) => a.severity !== "critical").length} meta="较昨日" delta="+3" />
        <MetricCard icon={CheckCircle2} tone="green" label="已恢复" value={recovered} meta="较昨日" delta="+4" />
        <MetricCard icon={Clock3} tone="blue" label="平均恢复时间 MTTR" value={formatDuration(data?.latency?.total?.avg_ms)} meta="较昨日" delta="下降" />
        <MetricCard icon={Activity} tone="blue" label="今日事件数" value={(data?.recent_executions || []).length} meta="近期窗口" delta="+9" />
      </section>

      <section className="analysis-grid">
        <Panel title="告警中心">
          <AlertCenter
            alerts={alerts}
            failures={failures}
            actions={actions}
            onOpenAlerts={onOpenAlerts}
            onOpenAction={onOpenAction}
          />
        </Panel>
        <Panel title="告警趋势">
          <AlertTrend points={history?.points || []} alerts={alerts} />
        </Panel>
      </section>

      <Panel title="执行分析">
        <div className="execution-metrics">
          <DonutMetric label="执行成功率" value={successRate(data?.status_counts || {})} color="#10b981" />
          <DonutMetric label="失败率" value={Number(errorRate(data?.status_counts || {}))} color="#ef4444" />
          <Histogram latency={data?.latency || {}} />
          <SmallChart title="P95 延迟 (ms)" points={history?.points || []} field="total_latency_p95_ms" color="#7c3aed" />
          <RouteBars executions={data?.recent_executions || []} />
        </div>
      </Panel>

      <Panel title="瓶颈定位">
        <FailureCards
          actions={actions}
          onOpenAction={onOpenAction}
        />
      </Panel>

      <Panel title="最近执行记录">
        <FilterRow
          filterWorker={filterWorker}
          filterStatus={filterStatus}
          filterAgent={filterAgent}
          setFilterWorker={setFilterWorker}
          setFilterStatus={setFilterStatus}
          setFilterAgent={setFilterAgent}
        />
        <ExecutionTable
          executions={filteredExecutions}
          totalCount={data?.recent_executions?.length ?? 0}
          onExecutionOpen={onExecutionOpen}
        />
      </Panel>

      <Panel title="Agent 执行健康状态">
        <AgentHealth agents={agentHealth} />
      </Panel>
    </>
  );
}

function Sessions({ sessionData, sessionId, setSessionId, traceId, setTraceId, traceData, loadSession }) {
  const executions = sessionData?.executions || [];
  const totals = sessionData?.totals || {};
  return (
    <>
      <Panel title="会话查询">
        <form className="query-form" onSubmit={loadSession}>
          <label>
            <span>Session ID</span>
            <input value={sessionId} onChange={(event) => setSessionId(event.target.value)} placeholder="sess-..." />
          </label>
          <label>
            <span>Trace ID</span>
            <input value={traceId} onChange={(event) => setTraceId(event.target.value)} placeholder="可选" />
          </label>
          <button type="submit" className="primary-button"><Search size={16} /> 查询</button>
        </form>
      </Panel>
      {sessionData ? (
        <section className="metric-grid four">
          <MetricCard icon={Workflow} tone="blue" label="执行节点" value={totals.executions ?? executions.length} meta="Execution Tree" delta="已加载" />
          <MetricCard icon={Activity} tone="green" label="数据事件" value={totals.events ?? 0} meta="Session Stream" delta="实时" />
          <MetricCard icon={GitBranch} tone="violet" label="Trace" value={sessionData.trace_id || traceId || "全部"} meta="Trace Context" delta="可追踪" />
          <MetricCard icon={Clock3} tone="amber" label="更新时间" value={formatTime(sessionData.generated_at)} meta="Generated At" delta="最新" />
        </section>
      ) : null}
      <Panel title="调用瀑布">
        <TraceWaterfall executions={executions} generatedAt={sessionData?.generated_at || Date.now()} />
      </Panel>
      <Panel title="Trace Span 瀑布">
        <TraceSpanWaterfall trace={traceData} />
      </Panel>
      <section className="split-grid">
        <Panel title="执行树">
          <Tree nodes={sessionData?.execution_tree || []} />
        </Panel>
        <Panel title="最近事件时间线">
          <Timeline items={sessionData?.timeline || []} />
        </Panel>
      </section>
    </>
  );
}

function SettingsView({ config, metricCatalog, onLoad, onLoadCatalog }) {
  useEffect(() => {
    if (!config) onLoad();
    if (!metricCatalog) onLoadCatalog();
  }, [config, metricCatalog, onLoad, onLoadCatalog]);

  if (!config || !metricCatalog) {
    return (
      <Panel title="配置">
        <Empty text="正在加载配置..." />
      </Panel>
    );
  }
  const metrics = Object.values(metricCatalog.metrics || {});
  const coreMetrics = metrics.filter((metric) => !metric.debug_only);
  const debugMetrics = metrics.filter((metric) => metric.debug_only);

  return (
    <>
      <section className="metric-grid four">
        <MetricCard icon={Server} tone="blue" label="Dashboard" value={`${config.dashboard.host}:${config.dashboard.port}`} meta="Auth" delta={config.dashboard.auth_enabled ? "已开启" : "未开启"} />
        <MetricCard icon={Layers3} tone="green" label="Redis" value={`${config.redis.host || "default"}:${config.redis.port || 0}`} meta="DB" delta={config.redis.db ?? 0} />
        <MetricCard icon={Clock3} tone="amber" label="Metrics TTL" value={`${config.observability.metrics_cache_ttl_seconds}s`} meta="History" delta={config.observability.history_limit} />
        <MetricCard icon={GitBranch} tone="violet" label="Trace Fallback" value={config.observability.trace_fallback_enabled ? "开启" : "关闭"} meta="Demo" delta={config.dashboard.demo_mode ? "是" : "否"} />
      </section>
      <section className="split-grid">
        <Panel title="运行配置">
          <DetailObject payload={config.dashboard} />
        </Panel>
        <Panel title="Redis 配置">
          <DetailObject payload={config.redis} />
        </Panel>
      </section>
      <Panel title="能力开关">
        <div className="capability-grid">
          {(config.capabilities || []).map((capability) => (
            <article key={capability.id}>
              <StatusDot status={capability.enabled ? "healthy" : "unknown"} />
              <strong>{capability.label}</strong>
              <code>{capability.id}</code>
            </article>
          ))}
        </div>
      </Panel>
      <section className="metric-grid four">
        <MetricCard icon={Gauge} tone="blue" label="指标目录" value={metricCatalog.total} meta="Core" delta={metricCatalog.core_count} />
        <MetricCard icon={Eye} tone="amber" label="Debug 指标" value={metricCatalog.debug_count} meta="Worker 维度" delta="隔离" />
        <MetricCard icon={Activity} tone="green" label="核心指标" value={coreMetrics.length} meta="Prometheus" delta="低基数" />
        <MetricCard icon={Layers3} tone="violet" label="兼容指标" value={metrics.filter((metric) => metric.legacy_names?.length).length} meta="Legacy" delta="_ms 保留" />
      </section>
      <Panel title="指标目录">
        <MetricCatalogTable metrics={metrics} />
      </Panel>
    </>
  );
}

function MetricCatalogTable({ metrics }) {
  if (!metrics.length) return <Empty text="暂无指标定义" />;
  const ordered = [...metrics].sort((left, right) => {
    if (left.debug_only !== right.debug_only) return left.debug_only ? 1 : -1;
    return left.name.localeCompare(right.name);
  });
  return (
    <div className="catalog-table-wrap">
      <table className="catalog-table">
        <thead>
          <tr>
            <th>指标</th>
            <th>类型</th>
            <th>单位</th>
            <th>标签</th>
            <th>解释</th>
            <th>层级</th>
          </tr>
        </thead>
        <tbody>
          {ordered.map((metric) => (
            <tr key={metric.name}>
              <td>
                <code>{metric.name}</code>
                {metric.legacy_names?.length ? (
                  <small>legacy: {metric.legacy_names.join(", ")}</small>
                ) : null}
              </td>
              <td>{metric.kind}</td>
              <td>{metric.unit}</td>
              <td>{metric.labels?.length ? metric.labels.join(", ") : "—"}</td>
              <td>
                <strong>{metric.description}</strong>
                <span>{metric.interpretation}</span>
              </td>
              <td>
                <span className={`catalog-scope ${metric.debug_only ? "debug" : "core"}`}>
                  {metric.debug_only ? "Debug" : "Core"}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Panel({ title, children, action = "", badge = null, onAction = null }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>{title}</h2>
        <div className="panel-actions">
          {badge !== null ? <span className="badge">{badge}</span> : null}
          {action && onAction ? <button type="button" onClick={onAction}>{action}<ChevronRight size={15} /></button> : null}
        </div>
      </div>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function MetricCard({ icon: Icon, tone, label, value, meta, delta }) {
  return (
    <article className="metric-card">
      <span className={`metric-icon ${tone}`}><Icon size={23} /></span>
      <div>
        <span className="metric-label">{label}</span>
        <strong>{value ?? 0}</strong>
        <p>{meta} <b>{delta}</b></p>
      </div>
    </article>
  );
}

function Fact({ icon: Icon, label, value }) {
  return (
    <article className="fact">
      <span><Icon size={20} /></span>
      <div>
        <small>{label}</small>
        <strong>{value}</strong>
      </div>
    </article>
  );
}

function DataFlow({ flow }) {
  const nodes = flow?.nodes || [];
  if (!nodes.length) return <Empty text="等待后端数据流模型" />;
  return (
    <div className="pipeline">
      {nodes.map((node, index) => {
        const Icon = NODE_ICONS[node.id] || Boxes;
        return (
          <React.Fragment key={node.id}>
            <article className={`pipeline-node ${node.status || "unknown"}`}>
              <div className="pipeline-title">
                <span><Icon size={19} /></span>
                <strong>{node.label}</strong>
              </div>
              <StatusDot status={node.status} />
              <div className="node-metrics">
                {Object.entries(node.metrics || {}).slice(0, 2).map(([key, value]) => (
                  <p key={key}>{metricLabel(key)} <b>{formatMetricValue(key, value)}</b></p>
                ))}
              </div>
            </article>
            {index < nodes.length - 1 ? <span className="pipeline-arrow">→</span> : null}
          </React.Fragment>
        );
      })}
      <div className="pipeline-legend">
        <span><i className="healthy" /> 健康</span>
        <span><i className="warning" /> 警告</span>
        <span><i className="critical" /> 严重</span>
        <span><i className="unknown" /> 未接入观测</span>
      </div>
    </div>
  );
}

function StreamTopology({ flow }) {
  const nodes = flow?.nodes || [];
  if (!nodes.length) return <Empty text="等待 Streams 拓扑数据" />;
  return (
    <div className="stream-topology">
      {nodes.map((node, index) => (
        <React.Fragment key={node.id}>
          <article>
            <strong>{node.label}</strong>
            {Object.entries(node.metrics || {}).slice(0, 3).map(([key, value]) => (
              <p key={key}>{metricLabel(key)} <span>{formatMetricValue(key, value)}</span></p>
            ))}
          </article>
          {index < nodes.length - 1 ? <span>······→</span> : null}
        </React.Fragment>
      ))}
    </div>
  );
}

function PoolCard({ pool }) {
  return (
    <article className="pool-card">
      <div className="pool-top">
        <strong>{pool.name}</strong>
        <span className={`health-badge ${pool.statusTone}`}>{pool.statusLabel}</span>
        <small>在线 {pool.online} / {pool.total}</small>
      </div>
      <div className="usage-line">
        <span style={{ width: `${pool.usage}%` }} />
      </div>
      <div className="pool-stats">
        <p>利用率 <b>{pool.usage}%</b></p>
        <p>平均延迟 <b>{pool.latency} ms</b></p>
        <p>最后心跳 <b>{pool.heartbeat}</b></p>
      </div>
      <div className="pool-stats">
        <p>活跃任务 <b>{pool.active}</b></p>
        <p>任务总量 <b>{pool.totalTasks}</b></p>
      </div>
    </article>
  );
}

function WorkerTable({ workers, onWorkerClick, onWorkerOpen, onWorkerAction }) {
  if (!workers.length) return <Empty text="未发现 Workers" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Worker ID</th>
            <th>Pool</th>
            <th>状态</th>
            <th>当前任务</th>
            <th>CPU</th>
            <th>内存</th>
            <th>心跳</th>
            <th>最近错误</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {workers.map((worker) => {
            const failed = worker.counts?.failed ?? worker.status_counts?.FAILED ?? 0;
            return (
              <tr key={worker.worker_id}>
                <td>
                  <button
                    type="button"
                    className="link-button"
                    onClick={() => onWorkerOpen(worker.worker_id)}
                    title="查看 Worker 详情"
                  >
                    {worker.worker_id}
                  </button>
                  <button
                    type="button"
                    className="mini-link"
                    onClick={() => onWorkerClick(worker.worker_id)}
                  >
                    执行记录
                  </button>
                </td>
                <td>{poolName(worker)}</td>
                <td><LifecycleBadge worker={worker} /></td>
                <td>{worker.active_count ?? 0}</td>
                <td><TinyBar value={Math.min(95, (worker.active_count || 0) * 12 + 18)} /></td>
                <td><TinyBar value={Math.min(92, (worker.total_tracked || 0) % 80 + 12)} label={`${Math.max(128, (worker.total_tracked || 1) * 12)} MB`} /></td>
                <td><span className="heartbeat" /> {formatTime(worker.last_seen)}</td>
                <td>{failed ? <span className="danger-text">{failed} failed</span> : "—"}</td>
                <td>{!isDemoMode() && onWorkerAction ? <WorkerActions worker={worker} onAction={onWorkerAction} /> : <button type="button" className="icon-button" disabled title="Demo 模式不执行管理操作"><MoreVertical size={16} /></button>}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function QueueTable({ rows, onQueueOpen }) {
  if (!rows.length) return <Empty text="未发现队列 Stream" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>名称</th>
            <th>类型</th>
            <th>长度</th>
            <th>消费组</th>
            <th>Pending</th>
            <th>最老 Pending</th>
            <th>Owner</th>
            <th>Lag P95</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((queue) => {
            const pending = (queue.consumer_groups || []).reduce((sum, group) => sum + Number(group.pending || 0), 0);
            const oldestAge = maxGroupValue(queue.consumer_groups || [], "oldest_pending_age_seconds");
            const owner = firstGroupValue(queue.consumer_groups || [], "pending_owner");
            return (
              <tr key={`${queue.name}-${queue.stream}`}>
                <td><strong>{queue.name}</strong><code>{queue.stream}</code></td>
                <td><span className="tag">{queue.queue_type === "control_plane" ? "队列" : "Stream"}</span></td>
                <td>{formatInteger(queue.length)}</td>
                <td>{(queue.consumer_groups || []).map((group) => group.name).join(", ") || "—"}</td>
                <td>{pending}</td>
                <td>{oldestAge ? formatMetricValue("oldest_pending_age_seconds", oldestAge) : "—"}</td>
                <td>{owner || "—"}</td>
                <td>{queue.lag_p95_ms ? formatDuration(queue.lag_p95_ms) : "—"}</td>
                <td><StatusDot status={pending || oldestAge ? "warning" : "healthy"} /></td>
                <td>
                  <button
                    type="button"
                    className="icon-button"
                    aria-label={`查看队列 ${queue.name}`}
                    onClick={() => onQueueOpen(queue.name)}
                  >
                    <Eye size={16} />
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ConsumerTable({ rows }) {
  const groups = rows.flatMap((queue) =>
    (queue.consumer_groups || []).map((group) => ({ ...group, queue: queue.name, stream: queue.stream })),
  );
  if (!groups.length) return <Empty text="暂无 Consumer Group 数据" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>消费组</th>
            <th>所属 Stream / Queue</th>
            <th>Pending</th>
            <th>Owner</th>
            <th>Delivery</th>
            <th>Lag</th>
            <th>Idle Time</th>
            <th>最老 Pending</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((group) => (
            <tr key={`${group.queue}-${group.name}`}>
              <td>{group.name}</td>
              <td>{group.queue}</td>
              <td>{group.pending ?? 0}</td>
              <td>{group.pending_owner || "—"}</td>
              <td>{group.max_delivery_count ? `${group.max_delivery_count} 次` : "—"}</td>
              <td>{group.lag ?? "—"}</td>
              <td>{group.idle_ms ? formatDuration(group.idle_ms) : "—"}</td>
              <td>{group.oldest_pending_age_seconds ? formatMetricValue("oldest_pending_age_seconds", group.oldest_pending_age_seconds) : "—"}</td>
              <td><StatusDot status={group.pending || group.oldest_pending_age_seconds ? "warning" : "healthy"} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AlertList({ alerts, onOpenAll }) {
  if (!alerts.length) return <Empty text="无活跃健康告警" />;
  return (
    <div className="alert-list">
      {alerts.map((alert, index) => (
        <article key={`${alert.code}-${index}`} className={`alert-item ${alert.severity || "info"}`}>
          <span className="alert-dot" />
          <div>
            <strong>{alert.message}</strong>
            <p>{alert.code} · {severityLabel(alert.severity)}</p>
          </div>
        </article>
      ))}
      <button type="button" className="text-action" onClick={onOpenAll}>查看全部告警 <ChevronRight size={15} /></button>
    </div>
  );
}

function AlertCenter({ alerts, failures, actions, onOpenAlerts, onOpenAction }) {
  const rows = actions.length
    ? actions.slice(0, 7).map((action) => ({
      level: severityLabel(action.severity),
      title: action.title,
      target: action.component || action.source,
      time: action.started_at,
      status: action.severity === "critical" ? "未恢复" : "观察中",
      actionId: action.id,
    }))
    : [
      ...alerts.map((alert) => ({
        level: severityLabel(alert.severity),
        title: alert.message,
        target: alert.code,
        time: Date.now(),
        status: alert.severity === "critical" ? "未恢复" : "观察中",
      })),
      ...failures.slice(0, 3).map((failure) => ({
        level: "严重",
        title: failure.error_type || "执行失败",
        target: failure.target_agent_type || failure.worker_id,
        time: failure.updated_at,
        status: "未恢复",
      })),
    ];
  if (!rows.length) return <Empty text="暂无告警事件" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th />
            <th>级别</th>
            <th>告警标题</th>
            <th>影响组件</th>
            <th>开始时间</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 7).map((row, index) => (
            <tr key={`${row.title}-${index}`}>
              <td><input type="checkbox" aria-label={`选择 ${row.title}`} /></td>
              <td><span className={`severity ${row.level === "严重" ? "critical" : "warning"}`}>{row.level}</span></td>
              <td>{row.title}</td>
              <td>{row.target}</td>
              <td>{formatTime(row.time)}</td>
              <td><span className="red-dot small" /> {row.status}</td>
              <td>
                <button
                  type="button"
                  className="mini-button"
                  onClick={() => (row.actionId ? onOpenAction(row.actionId) : onOpenAlerts())}
                >
                  查看详情
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <button type="button" className="text-action table-action" onClick={onOpenAlerts}>
        查看后端告警详情 <ChevronRight size={15} />
      </button>
    </div>
  );
}

function AlertTrend({ points, alerts }) {
  return (
    <div className="trend-with-donut">
      <StackedBars points={points} />
      <DonutMetric label="告警级别分布" value={alerts.length ? Math.round((alerts.filter((a) => a.severity === "critical").length / alerts.length) * 100) : 0} color="#ef4444" center={alerts.length || 0} />
    </div>
  );
}

function FilterRow({ filterWorker, filterStatus, filterAgent, setFilterWorker, setFilterStatus, setFilterAgent }) {
  const hasFilter = filterWorker || filterStatus || filterAgent;
  return (
    <div className="filter-row">
      <label>
        <span>Worker</span>
        <input value={filterWorker} onChange={(event) => setFilterWorker(event.target.value)} placeholder="搜索 Worker ID" />
      </label>
      <label>
        <span>状态</span>
        <select value={filterStatus} onChange={(event) => setFilterStatus(event.target.value)}>
          <option value="">全部状态</option>
          {STATUS_ORDER.map((status) => <option key={status} value={status}>{status}</option>)}
        </select>
      </label>
      <label>
        <span>Agent 类型</span>
        <input value={filterAgent} onChange={(event) => setFilterAgent(event.target.value)} placeholder="按 Agent 类型过滤" />
      </label>
      {hasFilter ? (
        <button type="button" className="secondary-button compact" onClick={() => {
          setFilterWorker("");
          setFilterStatus("");
          setFilterAgent("");
        }}>
          清除过滤
        </button>
      ) : null}
    </div>
  );
}

function ExecutionTable({ executions, totalCount, onExecutionOpen }) {
  if (!executions.length) return <Empty text="暂无执行记录" />;
  const showCount = totalCount > 0 && executions.length !== totalCount;
  return (
    <>
      {showCount ? <p className="filter-count">显示 {executions.length} / {totalCount} 条记录</p> : null}
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
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {executions.map((execution) => (
              <tr key={execution.execution_id}>
                <td><code>{execution.execution_id}</code><span>{execution.message_id}</span></td>
                <td>{execution.worker_id}</td>
                <td>{execution.target_agent_type}</td>
                <td><span className={`pill status-${String(execution.status || "").toLowerCase()}`}>{execution.status}</span></td>
                <td>队列 {formatDuration(execution.queue_latency_ms)}<br />运行 {formatDuration(execution.run_latency_ms)}</td>
                <td>{execution.route_status}<span>{execution.route_policy}</span></td>
                <td>{formatTime(execution.updated_at)}</td>
                <td>
                  <button
                    type="button"
                    className="mini-button"
                    onClick={() => onExecutionOpen(execution.execution_id)}
                  >
                    查看
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function AgentHealth({ agents }) {
  if (!agents.length) return <Empty text="暂无 Agent 健康数据" />;
  return (
    <div className="agent-grid">
      {agents.map((agent) => (
        <article key={agent.agent_type} className="agent-card">
          <div>
            <strong>{agent.agent_type}</strong>
            <span>{agent.worker_count ?? 0} Workers</span>
          </div>
          <TinyBar value={Math.min(100, Number(agent.queue_depth || 0) * 10)} />
          <p>队列深度 <b>{agent.queue_depth ?? 0}</b></p>
          <p>活跃 <b>{agent.recent_active_executions ?? 0}</b></p>
          <p>失败 <b>{agent.recent_failed_executions ?? 0}</b></p>
        </article>
      ))}
    </div>
  );
}

function ActionList({ actions, onOpenAction, onNavigate }) {
  if (!actions.length) return <Empty text="暂无待处理事项" />;
  return (
    <div className="action-list">
      {actions.slice(0, 4).map((action) => (
        <article key={action.id}>
          <span className={`round-icon ${action.tone}`}><AlertTriangle size={16} /></span>
          <div>
            <strong>{action.title}</strong>
            <p>{action.description}</p>
          </div>
          <time>{formatTime(action.started_at)}</time>
          <button
            type="button"
            className="primary-button compact"
            onClick={() => {
              onNavigate(action.target_view || "executions");
              onOpenAction(action.id);
            }}
          >
            去处理
          </button>
        </article>
      ))}
    </div>
  );
}

function AlertBreakdown({ alerts }) {
  const critical = alerts.filter((alert) => alert.severity === "critical").length;
  const warning = alerts.length - critical;
  const rows = [
    ["心跳丢失", critical, "#ef4444"],
    ["任务超时", warning, "#f59e0b"],
    ["频繁重启", 0, "#f97316"],
    ["启动失败", 0, "#2563eb"],
  ];
  return (
    <div className="breakdown">
      {rows.map(([label, value, color]) => (
        <p key={label}><i style={{ background: color }} /> {label}<b>{value}</b><span>占比 {alerts.length ? Math.round((value / alerts.length) * 100) : 0}%</span></p>
      ))}
    </div>
  );
}

function QueueInsights({ rows, onQueueOpen }) {
  const problems = rows
    .map((queue) => ({
      name: queue.name,
      pending: (queue.consumer_groups || []).reduce((sum, group) => sum + Number(group.pending || 0), 0),
      length: Number(queue.length || 0),
      oldestAge: maxGroupValue(queue.consumer_groups || [], "oldest_pending_age_seconds"),
      owner: firstGroupValue(queue.consumer_groups || [], "pending_owner"),
    }))
    .sort((left, right) => right.pending + right.length + right.oldestAge - (left.pending + left.length + left.oldestAge))
    .slice(0, 3);
  return (
    <div className="insight-list">
      {problems.map((item) => (
        <article key={item.name}>
          <strong>{item.name}</strong>
          <p>当前 {item.pending} 条 Pending，队列长度 {item.length}，最老 {item.oldestAge ? formatMetricValue("oldest_pending_age_seconds", item.oldestAge) : "—"}</p>
          <small>Owner {item.owner || "—"}</small>
          <button type="button" className="mini-button" onClick={() => onQueueOpen(item.name)}>查看队列详情</button>
        </article>
      ))}
      <ul className="suggestions">
        <li>检查 deadletter 消息内容并尽快处理</li>
        <li>优化 observable_stream 消费性能</li>
        <li>评估 Worker 扩容以降低 Pending 和 Lag</li>
      </ul>
    </div>
  );
}

function FailureCards({ actions, onOpenAction }) {
  const cards = actions.filter((action) => action.severity !== "info").slice(0, 4);
  if (!cards.length) return <Empty text="暂无瓶颈风险" />;
  return (
    <div className="failure-cards">
      {cards.map((card) => (
        <article key={card.id}>
          <span className={`severity ${card.severity === "critical" ? "critical" : "warning"}`}>{severityLabel(card.severity)}</span>
          <strong>{card.title}</strong>
          <p>{card.description}</p>
          <small>影响组件 {card.component}</small>
          <TinyBar value={card.score} />
          <button
            type="button"
            className="mini-button"
            onClick={() => onOpenAction(card.id)}
          >
            查看详情
          </button>
        </article>
      ))}
    </div>
  );
}

function DetailModal({ modal, onClose }) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="detail-modal"
        role="dialog"
        aria-modal="true"
        aria-label={modal.title}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <h2>{modal.title}</h2>
          <button type="button" className="icon-button" onClick={onClose} aria-label="关闭详情">×</button>
        </header>
        <div className="modal-body">
          {modal.kind === "queue" ? <QueueDetail payload={modal.payload} /> : null}
          {modal.kind === "alerts" ? <AlertsDetail payload={modal.payload} /> : null}
          {modal.kind === "worker" ? <WorkerDetail payload={modal.payload} /> : null}
          {modal.kind === "execution" ? <ExecutionDetail payload={modal.payload} /> : null}
          {modal.kind === "action" ? <ActionDetail payload={modal.payload} /> : null}
          {!["queue", "alerts", "worker", "execution", "action"].includes(modal.kind) ? (
            <DetailObject payload={modal.payload} />
          ) : null}
        </div>
      </section>
    </div>
  );
}

function QueueDetail({ payload }) {
  const queue = payload.queue || {};
  return (
    <div className="detail-stack">
      <div className="detail-grid">
        <MetricCard icon={Workflow} tone="blue" label="队列长度" value={queue.length ?? 0} meta="Pending" delta={queue.pending_total ?? 0} />
        <MetricCard icon={Activity} tone="green" label="状态" value={flowStatusLabel(queue.status)} meta="类型" delta={queue.queue_type} />
      </div>
      <DetailObject payload={queue} />
      <RecommendationList items={payload.recommendations || []} />
    </div>
  );
}

function AlertsDetail({ payload }) {
  return (
    <div className="detail-stack">
      <div className="detail-grid">
        <MetricCard icon={AlertTriangle} tone="red" label="告警总数" value={payload.summary?.total ?? 0} meta="Critical" delta={payload.summary?.critical ?? 0} />
        <MetricCard icon={Clock3} tone="amber" label="待处理" value={payload.summary?.open ?? 0} meta="Warning" delta={payload.summary?.warning ?? 0} />
      </div>
      <RecommendationList items={payload.recommendations || []} />
      <div className="modal-alert-list">
        {(payload.alerts || []).map((alert) => (
          <article key={alert.id}>
            <strong>{alert.message}</strong>
            <p>{alert.code} · {severityLabel(alert.severity)} · {alert.component}</p>
            {alert.runbook ? (
              <div className="runbook-box">
                <b>{alert.runbook.title}</b>
                <ul>
                  {(alert.runbook.actions || []).map((action) => (
                    <li key={action}>{action}</li>
                  ))}
                </ul>
              </div>
            ) : null}
            <RecommendationList items={alert.recommendations || []} compact />
          </article>
        ))}
      </div>
    </div>
  );
}

function WorkerDetail({ payload }) {
  const worker = payload.worker || {};
  return (
    <div className="detail-stack">
      <div className="detail-grid">
        <MetricCard icon={Users} tone="blue" label="当前任务" value={worker.active_count ?? 0} meta="生命周期" delta={worker.lifecycle || "active"} />
        <MetricCard icon={Activity} tone="green" label="总任务" value={worker.total_tracked ?? 0} meta="最后心跳" delta={formatTime(worker.last_seen)} />
      </div>
      <RecommendationList items={payload.recommendations || []} />
      <DetailObject payload={worker} />
      <section className="modal-alert-list">
        {(payload.executions || []).slice(0, 6).map((execution) => (
          <article key={execution.execution_id}>
            <strong>{execution.execution_id}</strong>
            <p>{execution.status} · {execution.target_agent_type} · {formatTime(execution.updated_at)}</p>
          </article>
        ))}
      </section>
    </div>
  );
}

function ExecutionDetail({ payload }) {
  const execution = payload.execution || {};
  return (
    <div className="detail-stack">
      <div className="detail-grid">
        <MetricCard icon={Workflow} tone="blue" label="状态" value={execution.status || "unknown"} meta="Agent" delta={execution.target_agent_type || "unknown"} />
        <MetricCard icon={Clock3} tone="amber" label="端到端" value={formatDuration(execution.total_latency_ms)} meta="运行" delta={formatDuration(execution.run_latency_ms)} />
      </div>
      <RecommendationList items={payload.recommendations || []} />
      <AgentConfigAuditPanel agentConfig={payload.agent_config} />
      <DetailObject payload={execution} />
      {(payload.failures || []).length ? (
        <section className="modal-alert-list">
          {payload.failures.map((failure) => (
            <article key={failure.execution_id || failure.error_type}>
              <strong>{failure.error_type || "Failure"}</strong>
              <p>{failure.error_message || "无错误详情"}</p>
            </article>
          ))}
        </section>
      ) : null}
    </div>
  );
}

function AgentConfigAuditPanel({ agentConfig }) {
  if (!agentConfig) {
    return (
      <section className="agent-config-panel empty">
        <strong>Agent 配置快照</strong>
        <p>该执行没有可反查的配置投影。</p>
      </section>
    );
  }
  const target = agentConfig.target_agent_config || {};
  const tools = Object.entries(target.tools || {});
  const skills = Object.keys(target.skills || {});
  const promptHashes = Object.entries(target.prompt_hashes || {});
  return (
    <section className="agent-config-panel">
      <header>
        <div>
          <strong>Agent 配置快照</strong>
          <p>{agentConfig.target_agent_type || target.agent_id || "unknown"} · v{agentConfig.version ?? 0}</p>
        </div>
        <code>{agentConfig.snapshot_hash || "no-hash"}</code>
      </header>
      {target.agent_id ? (
        <div className="agent-config-grid">
          <article>
            <span>Agent</span>
            <strong>{target.name || target.agent_id}</strong>
            <small>{target.registered === false ? "未注册 AgentConfig" : (target.description || "无描述")}</small>
          </article>
          <article>
            <span>Tools</span>
            <strong>{tools.length}</strong>
            <small>{tools.map(([name]) => name).join(", ") || "未声明"}</small>
          </article>
          <article>
            <span>Skills</span>
            <strong>{skills.length}</strong>
            <small>{skills.join(", ") || "未声明"}</small>
          </article>
          <article>
            <span>Sub Agents</span>
            <strong>{(target.sub_agents || []).length}</strong>
            <small>{(target.sub_agents || []).join(", ") || "未声明"}</small>
          </article>
        </div>
      ) : (
        <p>本次执行没有捕获到目标 Agent 配置。</p>
      )}
      {promptHashes.length ? (
        <div className="agent-config-hashes">
          {promptHashes.map(([name, hash]) => (
            <code key={name}>{name}: {hash}</code>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function ActionDetail({ payload }) {
  const action = payload.action || {};
  return (
    <div className="detail-stack">
      <div className="detail-grid">
        <MetricCard icon={AlertTriangle} tone={action.tone || "amber"} label="级别" value={severityLabel(action.severity)} meta="目标页面" delta={action.target_view || "executions"} />
        <MetricCard icon={Workflow} tone="blue" label="影响组件" value={action.component || "unknown"} meta="来源" delta={action.source || action.kind} />
      </div>
      <RecommendationList items={payload.recommendations || []} />
      <DetailObject payload={action} />
      {payload.related ? <DetailObject payload={payload.related} /> : null}
    </div>
  );
}

function RecommendationList({ items, compact = false }) {
  if (!items.length) return null;
  return (
    <ul className={compact ? "recommendations compact" : "recommendations"}>
      {items.map((item) => <li key={item}>{item}</li>)}
    </ul>
  );
}

function DetailObject({ payload }) {
  return (
    <dl className="detail-object">
      {Object.entries(payload || {}).map(([key, value]) => (
        <React.Fragment key={key}>
          <dt>{key}</dt>
          <dd>{typeof value === "object" ? JSON.stringify(value) : String(value ?? "")}</dd>
        </React.Fragment>
      ))}
    </dl>
  );
}

function TraceSpanWaterfall({ trace }) {
  const timeline = trace?.timeline || [];
  if (!timeline.length) return <Empty text="输入 Trace ID 后将显示 Span 瀑布" />;
  const duration = Math.max(1, Number(trace.duration_ms || 0));
  return (
    <div className="waterfall-list">
      <div className="trace-topline">
        <strong>{trace.trace_id}</strong>
        <span className={`pill status-${String(trace.status || "").toLowerCase()}`}>{trace.status}</span>
        <span>{formatDuration(duration)}</span>
        <span>{trace.span_count ?? timeline.length} spans</span>
      </div>
      {timeline.map((span) => (
        <WaterfallRow
          key={span.span_id}
          title={span.operation}
          meta={`${span.component} · ${span.worker_id || span.execution_id || span.message_id || "span"}`}
          status={span.status}
          offset={span.offset_ms}
          duration={span.duration_ms || 1}
          total={duration}
        />
      ))}
      <MetricsWindowPanel metricsWindow={trace.metrics_window} />
    </div>
  );
}

function MetricsWindowPanel({ metricsWindow }) {
  if (!metricsWindow) return null;
  const summary = metricsWindow.summary || {};
  const sloWindow = summary.slo_window || null;
  const signalExplain = Array.isArray(summary.signal_explain) ? summary.signal_explain : [];
  const rows = Object.entries(summary)
    .filter(([, value]) => value && typeof value === "object" && "last" in value && "max" in value)
    .slice(0, 6);
  return (
    <section className="metrics-window-panel">
      <header>
        <strong>Trace 时间窗指标</strong>
        <span>{summary.sample_count ?? 0} samples · {metricsWindow.status || "ok"}</span>
      </header>
      {sloWindow ? (
        <div className="slo-window-strip">
          <article>
            <span>SLO 窗口</span>
            <strong>{sloWindow.window || "window"}</strong>
            <small>{formatInteger(sloWindow.successful_executions)} / {formatInteger(sloWindow.terminal_executions)} success</small>
          </article>
          <article className={sloWindow.success_ratio_objective_met ? "ok" : "bad"}>
            <span>成功率</span>
            <strong>{formatMetricValue("success_ratio_ppm", sloWindow.success_ratio_ppm)}</strong>
            <small>target {formatMetricValue("success_ratio_ppm", sloWindow.success_ratio_target_ppm)}</small>
          </article>
          <article className={Number(sloWindow.burn_rate || 0) <= 1 ? "ok" : "bad"}>
            <span>Burn Rate</span>
            <strong>{Number(sloWindow.burn_rate || 0).toFixed(2)}x</strong>
            <small>{sloWindow.success_ratio_objective_met ? "budget steady" : "budget burning"}</small>
          </article>
          <article className={sloWindow.latency_objective_met ? "ok" : "bad"}>
            <span>总 P95</span>
            <strong>{formatDuration(sloWindow.total_latency_p95_ms)}</strong>
            <small>{sloWindow.latency_objective_met ? "within SLO" : "over SLO"}</small>
          </article>
        </div>
      ) : null}
      {signalExplain.length ? (
        <div className="signal-explain-list">
          {signalExplain.map((signal) => (
            <article key={signal.category} className={signal.severity === "ok" ? "ok" : "warning"}>
              <div>
                <strong>{signalCategoryLabel(signal.category)}</strong>
                <span>{signal.severity === "ok" ? "正常" : "需关注"}</span>
              </div>
              <p>{signal.message}</p>
              <small>{formatSignalMetrics(signal.metrics || {})}</small>
            </article>
          ))}
        </div>
      ) : null}
      {rows.length ? (
        <div className="metrics-window-grid">
          {rows.map(([key, value]) => (
            <article key={key}>
              <span>{metricLabel(key)}</span>
              <strong>{formatMetricValue(key, value.last)}</strong>
              <small>max {formatMetricValue(key, value.max)}</small>
            </article>
          ))}
        </div>
      ) : <Empty text="该 Trace 时间窗暂无指标样本" />}
      {(metricsWindow.diagnostics || []).length ? (
        <ul className="metrics-diagnostics">
          {metricsWindow.diagnostics.slice(0, 4).map((diagnostic) => (
            <li key={diagnostic.code}>
              <b>{diagnostic.code}</b>
              <span>{diagnostic.message}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}

function TraceWaterfall({ executions, generatedAt }) {
  const rows = useMemo(() => buildTraceRows(executions, generatedAt), [executions, generatedAt]);
  if (!rows.length) return <Empty text="查询会话后将显示调用瀑布" />;
  const minStart = Math.min(...rows.map((row) => row.start));
  const maxEnd = Math.max(...rows.map((row) => row.end));
  const range = Math.max(1, maxEnd - minStart);
  return (
    <div className="waterfall-list">
      {rows.map((row) => (
        <WaterfallRow
          key={row.key}
          title={row.agent}
          meta={`${row.worker} · ${row.executionId}`}
          status={row.status}
          offset={row.start - minStart}
          duration={row.duration}
          total={range}
        />
      ))}
    </div>
  );
}

function WaterfallRow({ title, meta, status, offset, duration, total }) {
  const left = percent(offset, total);
  const width = Math.max(2, percent(duration, total));
  return (
    <article className="waterfall-row">
      <div>
        <strong>{title}</strong>
        <code>{meta}</code>
      </div>
      <div className="waterfall-track">
        <span className={`waterfall-bar status-${String(status || "").toLowerCase()}`} style={{ left: `${left}%`, width: `${width}%` }}>
          {formatDuration(duration)}
        </span>
      </div>
      <span className={`pill status-${String(status || "").toLowerCase()}`}>{status}</span>
    </article>
  );
}

function Tree({ nodes }) {
  if (!nodes.length) return <Empty text="该会话无执行记录" />;
  return (
    <div className="tree">
      {nodes.map((node) => (
        <article key={node.execution_id} className="tree-node">
          <div><strong>{node.target_agent_type || "未知 Agent"}</strong><span className={`pill status-${String(node.status || "").toLowerCase()}`}>{node.status || "未知"}</span></div>
          <code>{node.execution_id}<br />message={node.message_id}</code>
          {node.children?.length ? <Tree nodes={node.children} /> : null}
        </article>
      ))}
    </div>
  );
}

function Timeline({ items }) {
  if (!items.length) return <Empty text="无时间线条目" />;
  return (
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
  );
}

function LifecycleBadge({ worker }) {
  const lc = worker.lifecycle || "active";
  const online = worker.online !== false;
  const label = lc === "suspended" ? "已暂停" : lc === "evicted" ? "已驱逐" : online ? "在线" : "离线";
  const cls = lc === "suspended" ? "status-dot warning" : lc === "evicted" ? "status-dot critical" : online ? "status-dot healthy" : "status-dot unknown";
  return <span className={cls} title={worker.lifecycle_reason || ""}>{label}</span>;
}

function WorkerActions({ worker, onAction }) {
  const [confirming, setConfirming] = useState(false);
  const lc = worker.lifecycle || "active";
  return (
    <div className="worker-actions" onClick={(event) => event.stopPropagation()}>
      {lc === "active" ? <button type="button" className="mini-button warn" onClick={() => onAction("suspend", worker.worker_id)}>暂停</button> : null}
      {lc === "suspended" ? <button type="button" className="mini-button ok" onClick={() => onAction("resume", worker.worker_id)}>恢复</button> : null}
      {lc === "evicted" ? <button type="button" className="mini-button ok" onClick={() => onAction("allow-rejoin", worker.worker_id)}>允许重加入</button> : null}
      {lc !== "evicted" && (confirming ? (
        <>
          <button type="button" className="mini-button danger" onClick={() => { onAction("evict", worker.worker_id); setConfirming(false); }}>确认驱逐</button>
          <button type="button" className="mini-button" onClick={() => setConfirming(false)}>取消</button>
        </>
      ) : (
        <button type="button" className="mini-button danger" onClick={() => setConfirming(true)}>驱逐</button>
      ))}
    </div>
  );
}

function DenylistPanel({ agentTypes }) {
  const [open, setOpen] = useState(false);
  const [denylistData, setDenylistData] = useState({});
  const [denyInput, setDenyInput] = useState({});
  const [denyError, setDenyError] = useState("");

  const loadDenylists = async () => {
    const results = {};
    await Promise.all(
      agentTypes.map(async (agentType) => {
        try {
          const data = await fetchJson(`/api/admin/type/${encodeURIComponent(agentType)}/denylist`);
          results[agentType] = data.denied || [];
        } catch {
          results[agentType] = [];
        }
      }),
    );
    setDenylistData(results);
  };

  const handleToggle = async () => {
    if (!open) await loadDenylists();
    setOpen((value) => !value);
  };

  const handleAllow = async (agentType, workerId) => {
    if (isDemoMode()) return;
    try {
      await postJson(`/api/admin/type/${encodeURIComponent(agentType)}/allow`, { worker_id: workerId });
      await loadDenylists();
    } catch (err) {
      setDenyError(err.message);
    }
  };

  const handleDeny = async (agentType) => {
    if (isDemoMode()) return;
    const workerId = (denyInput[agentType] || "").trim();
    if (!workerId) return;
    try {
      await postJson(`/api/admin/type/${encodeURIComponent(agentType)}/deny`, { worker_id: workerId });
      setDenyInput((prev) => ({ ...prev, [agentType]: "" }));
      await loadDenylists();
    } catch (err) {
      setDenyError(err.message);
    }
  };

  if (!agentTypes?.length) return null;
  return (
    <div className="denylist-panel">
      <button type="button" className="secondary-button compact" onClick={handleToggle}>
        Agent-type 准入管控 {open ? "收起" : "展开"}
      </button>
      {open ? (
        <div className="denylist-body">
          {denyError ? <p className="danger-text">{denyError}</p> : null}
          {agentTypes.map((agentType) => (
            <section key={agentType} className="denylist-type-row">
              <strong>{agentType}</strong>
              <div className="chips">
                {(denylistData[agentType] || []).length ? (
                  (denylistData[agentType] || []).map((workerId) => (
                    <span key={workerId} className="tag">
                      {workerId}
                      {!isDemoMode() ? <button type="button" className="mini-button ok" onClick={() => handleAllow(agentType, workerId)}>解除</button> : null}
                    </span>
                  ))
                ) : (
                  <span className="muted">无禁止项</span>
                )}
              </div>
              {!isDemoMode() ? (
                <div className="inline-form">
                  <input
                    value={denyInput[agentType] || ""}
                    onChange={(event) => setDenyInput((prev) => ({ ...prev, [agentType]: event.target.value }))}
                    placeholder="worker_id"
                  />
                  <button type="button" className="mini-button warn" onClick={() => handleDeny(agentType)}>禁止</button>
                </div>
              ) : null}
            </section>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function ApiHealthPill({ health }) {
  const degraded = health.status !== "ok";
  return (
    <span className={`api-health-pill ${degraded ? "degraded" : "ok"}`} title={degraded ? health.last_error_message : "Dashboard API 正常"}>
      API {degraded ? "异常" : "正常"}
    </span>
  );
}

function TinyBar({ value, label = "" }) {
  const width = Math.max(2, Math.min(100, Number(value || 0)));
  return (
    <span className="tiny-bar">
      <i style={{ width: `${width}%` }} />
      {label ? <b>{label}</b> : <b>{Math.round(width)}%</b>}
    </span>
  );
}

function StatusDot({ status }) {
  return <span className={`status-dot ${status || "unknown"}`}>{flowStatusLabel(status)}</span>;
}

function SmallChart({ title, points, field, color, percent: asPercent = false }) {
  const current = points[points.length - 1]?.[field] || 0;
  return (
    <article className="small-chart">
      <div><strong>{title}</strong><span>{asPercent ? `${Math.min(100, current * 12)}%` : formatMetricValue(field, current)}</span></div>
      {points.length ? <Sparkline points={points} field={field} color={color} /> : <Empty text="暂无趋势" />}
    </article>
  );
}

function TrendDuo({ points }) {
  return (
    <div className="trend-duo">
      <SmallChart title="吞吐量 (msg/s)" points={points} field="active_executions" color="#2563eb" />
      <SmallChart title="延迟 P95 (ms) & 错误率 (%)" points={points} field="total_latency_p95_ms" color="#7c3aed" />
    </div>
  );
}

function Sparkline({ points, field, color = "#2563eb" }) {
  return (
    <svg className="sparkline" viewBox="0 0 220 72" role="img" aria-label={`${field} 趋势`}>
      <path d={sparklinePath(points, field, 220, 72)} fill="none" stroke={color} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function AreaTrend({ points, fields }) {
  if (!points.length) return <Empty text="收集快照后将显示趋势历史" />;
  return (
    <svg className="area-trend" viewBox="0 0 520 180" role="img" aria-label="队列积压趋势">
      {fields.map((field, index) => (
        <path key={field} d={sparklinePath(points, field, 520, 170)} fill="none" stroke={index ? "#8b5cf6" : "#10b981"} strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
      ))}
    </svg>
  );
}

function StackedBars({ points }) {
  const slice = points.slice(-10);
  if (!slice.length) return <Empty text="暂无告警趋势" />;
  const max = Math.max(1, ...slice.map((point) => Number(point.alert_count || 0) + 2));
  return (
    <div className="stacked-bars">
      {slice.map((point) => (
        <span key={point.generated_at}>
          <i className="green" style={{ height: `${20 + ((point.alert_count || 0) / max) * 80}px` }} />
          <i className="amber" style={{ height: `${10 + ((point.consumer_pending_total || 0) / max) * 35}px` }} />
          <i className="red" style={{ height: `${8 + ((point.failed_executions || 0) / max) * 30}px` }} />
        </span>
      ))}
    </div>
  );
}

function Histogram({ latency }) {
  const values = [
    latency.queue?.p95_ms || 0,
    latency.run?.avg_ms || latency.avg_ms || 0,
    latency.run?.p95_ms || latency.p95_ms || 0,
    latency.total?.p95_ms || 0,
    latency.total?.avg_ms || 0,
  ];
  const max = Math.max(1, ...values);
  return (
    <article className="histogram">
      <strong>耗时分布 (ms)</strong>
      <div>
        {values.map((value, index) => (
          <span key={`${value}-${index}`} style={{ height: `${Math.max(8, (value / max) * 96)}px` }} title={formatDuration(value)} />
        ))}
      </div>
      <p>平均 {formatDuration(latency.run?.avg_ms || latency.avg_ms)} · P95 {formatDuration(latency.total?.p95_ms)}</p>
    </article>
  );
}

function DonutMetric({ label, value, color, center = null }) {
  const safe = Math.max(0, Math.min(100, Number(value || 0)));
  return (
    <article className="donut-card">
      <div className="donut" style={{ background: `conic-gradient(${color} ${safe * 3.6}deg, #edf2f7 0deg)` }}>
        <span>{center ?? `${safe.toFixed(safe % 1 ? 1 : 0)}%`}</span>
      </div>
      <strong>{label}</strong>
    </article>
  );
}

function RouteBars({ executions }) {
  const counts = executions.reduce((acc, execution) => {
    const key = execution.route_policy || execution.target_agent_type || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const rows = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 5);
  const max = Math.max(1, ...rows.map(([, count]) => count));
  return (
    <article className="route-bars">
      <strong>Top Error Routes</strong>
      {rows.map(([name, count]) => (
        <p key={name}><span>{name}</span><i style={{ width: `${(count / max) * 100}%` }} /><b>{count}</b></p>
      ))}
    </article>
  );
}

function Empty({ text }) {
  return <p className="empty-state">{text}</p>;
}

function allQueues(queues) {
  const agentRows = (queues.agent_type_streams || []).map((queue) => ({
    ...queue,
    name: queue.agent_type,
    queue_type: "agent_type",
  }));
  const controlRows = Object.entries(queues.control_plane || {}).map(([name, queue]) => ({
    ...queue,
    name,
    queue_type: "control_plane",
  }));
  return [...agentRows, ...controlRows];
}

function totalPending(rows) {
  return rows.reduce(
    (sum, queue) =>
      sum + (queue.consumer_groups || []).reduce((inner, group) => inner + Number(group.pending || 0), 0),
    0,
  );
}

function maxGroupValue(groups, key) {
  return Math.max(0, ...groups.map((group) => Number(group[key] || 0)));
}

function firstGroupValue(groups, key) {
  return groups.map((group) => group[key]).find((value) => value) || "";
}

function buildWorkerPools(workers) {
  const groups = workers.reduce((acc, worker) => {
    const name = poolName(worker);
    if (!acc[name]) acc[name] = [];
    acc[name].push(worker);
    return acc;
  }, {});
  const entries = Object.entries(groups);
  if (!entries.length) {
    return [
      {
        name: "Default Pool",
        total: 0,
        online: 0,
        failed: 0,
        usage: 0,
        latency: 0,
        heartbeat: "未知",
        active: 0,
        totalTasks: 0,
        statusLabel: "健康",
        statusTone: "ok",
      },
    ];
  }
  return entries.slice(0, 3).map(([name, pool]) => {
    const active = pool.reduce((sum, worker) => sum + Number(worker.active_count || 0), 0);
    const totalTasks = pool.reduce((sum, worker) => sum + Number(worker.total_tracked || 0), 0);
    const failed = pool.reduce((sum, worker) => sum + Number(worker.counts?.failed || worker.status_counts?.FAILED || 0), 0);
    const latestHeartbeat = Math.max(...pool.map((worker) => Number(worker.last_seen || 0)));
    const status = poolStatus(pool, failed, latestHeartbeat);
    return {
      name,
      total: pool.length,
      online: pool.filter((worker) => worker.online !== false).length,
      failed,
      usage: Math.min(100, Math.round((active / Math.max(1, pool.length * 4)) * 100)),
      latency: 40 + (totalTasks % 120),
      heartbeat: formatTime(latestHeartbeat),
      active,
      totalTasks,
      statusLabel: status.label,
      statusTone: status.tone,
    };
  });
}

function poolStatus(workers, failed, latestHeartbeat) {
  const now = Date.now();
  const onlineCount = workers.filter((worker) => worker.online !== false).length;
  const hasOffline = onlineCount < workers.length;
  const hasLifecycleError = workers.some((worker) => {
    const lifecycle = worker.lifecycle || "active";
    return lifecycle === "suspended" || lifecycle === "evicted";
  });
  const heartbeatStale = !latestHeartbeat || now - latestHeartbeat > 45000;

  if (hasOffline || hasLifecycleError || heartbeatStale) {
    return { label: "异常", tone: "danger" };
  }
  if (failed > 0 || workers.some((worker) => Number(worker.active_count || 0) < 0)) {
    return { label: "有失败", tone: "warning" };
  }
  return { label: "健康", tone: "ok" };
}

function poolName(worker) {
  const first = worker.agent_types?.[0];
  if (!first) return "Default Pool";
  if (first.includes("writer")) return "Heavy Tasks Pool";
  if (first.includes("research")) return "Retry Pool";
  return "Default Pool";
}

function mergeAgentHealthQueues(agentHealth, queues) {
  const depthByAgent = Object.fromEntries((queues.agent_type_streams || []).map((queue) => [queue.agent_type, queue.length ?? 0]));
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
      const end = Math.max(start, firstPositive(finished, updated, generatedAt, runStart));
      const rowStart = Math.min(start, runStart);
      const rowEnd = Math.max(end, runStart);
      const status = String(execution.status || "UNKNOWN");
      return {
        key: execution.execution_id || execution.message_id || `${execution.worker_id}-${start}`,
        executionId: execution.execution_id || execution.message_id || "unknown",
        agent: execution.target_agent_type || "unknown agent",
        worker: execution.worker_id || "unknown worker",
        status,
        start: rowStart,
        end: rowEnd,
        duration: Math.max(1, rowEnd - rowStart),
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

function formatTime(value) {
  const numeric = Number(value);
  if (!numeric) return "未知";
  return new Date(numeric).toLocaleTimeString("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function formatDuration(value) {
  const numeric = Number(value || 0);
  if (numeric <= 0) return "0 ms";
  if (numeric < 1000) return `${Math.round(numeric)} ms`;
  const seconds = Math.round(numeric / 1000);
  if (seconds < 60) return `${seconds} 秒`;
  return `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`;
}

function formatInteger(value) {
  return String(Math.round(Number(value || 0)));
}

function healthFromAlerts(alerts) {
  const criticalAlerts = alerts.filter((alert) => alert.severity === "critical").length;
  const warningAlerts = alerts.filter((alert) => alert.severity === "warning").length;
  const score = Math.max(0, 100 - criticalAlerts * 40 - warningAlerts * 10);
  if (criticalAlerts) return { status: "critical", score, summary: `${criticalAlerts} 个严重告警，${warningAlerts} 个警告告警` };
  if (warningAlerts) return { status: "warning", score, summary: `${warningAlerts} 个警告告警` };
  return { status: "healthy", score, summary: "集群暂无活跃健康告警" };
}

function successRate(counts) {
  const total = Object.values(counts).reduce((sum, count) => sum + Number(count || 0), 0);
  return total ? Number((((counts.COMPLETED || 0) / total) * 100).toFixed(2)) : 100;
}

function errorRate(counts) {
  const total = Object.values(counts).reduce((sum, count) => sum + Number(count || 0), 0);
  return total ? (((counts.FAILED || 0) / total) * 100).toFixed(1) : "0";
}

function dedupeAlerts(alerts) {
  const seen = new Set();
  return alerts.filter((alert) => {
    const key = [alert.code || "", alert.severity || "", alert.message || "", alert.value ?? "", alert.threshold ?? ""].join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function severityLabel(status) {
  if (status === "critical") return "严重";
  if (status === "warning") return "警告";
  return "信息";
}

function flowStatusLabel(status) {
  if (status === "critical") return "严重";
  if (status === "warning") return "警告";
  if (status === "healthy") return "健康";
  return "未接入观测";
}

function metricLabel(key) {
  const labels = {
    active_executions: "活跃",
    alert_count: "告警数",
    consumer_pending: "Pending",
    consumer_pending_total: "Pending",
    control_queue_depth: "控制队列",
    deadletter_count: "死信",
    deadletters: "死信",
    failed_executions: "失败",
    fanout_observable: "推送观测",
    freshness_age_ms: "新鲜度",
    max_delivery_count: "最大投递",
    observable_from_framework: "框架观测",
    oldest_pending_age_seconds: "最老 Pending",
    pending_deliveries: "待投递",
    queue_depth: "队列深度",
    queue_depth_total: "队列深度",
    queue_latency_p95_ms: "队列 P95",
    recent_events: "近期事件",
    run_latency_p95_ms: "运行 P95",
    success_ratio_ppm: "成功率",
    total_latency_p95_ms: "总 P95",
    tracked_executions: "追踪任务",
    workers_online: "在线",
  };
  return labels[key] || key;
}

function signalCategoryLabel(category) {
  const labels = {
    queue: "队列",
    worker: "Worker",
    errors: "错误",
  };
  return labels[category] || category;
}

function formatSignalMetrics(metrics) {
  return Object.entries(metrics)
    .map(([key, value]) => `${metricLabel(key)} ${formatMetricValue(key, value)}`)
    .join(" · ");
}

function formatMetricValue(key, value) {
  if (key.endsWith("_ms")) return formatDuration(value);
  if (key.endsWith("_seconds")) return formatDuration(Number(value || 0) * 1000);
  if (key.endsWith("_ppm")) return `${(Number(value || 0) / 10000).toFixed(2)}%`;
  return formatInteger(value);
}

function sparklinePath(points, key, width = 120, height = 36) {
  const values = points.map((point) => Number(point[key] || 0));
  if (!values.length) return "";
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(1, max - min);
  const xStep = values.length > 1 ? (width - 4) / (values.length - 1) : 0;
  return values
    .map((value, index) => {
      const x = 2 + index * xStep;
      const y = height - 4 - ((value - min) / range) * (height - 8);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function downloadJson(data, filename) {
  const blob = new Blob([JSON.stringify(data, null, 2)], {
    type: "application/json;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  const withoutExtension = filename.replace(/\.json$/i, "");
  link.download = `${withoutExtension.replace(/[:]/g, "-")}.json`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

createRoot(document.getElementById("root")).render(<App />);
