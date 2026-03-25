import asyncio

import pytest

from byclaw_gateway_sdk import (
    AgentConfig,
    AgentContext,
    Plugin,
    PluginManifest,
    PluginRegistry,
    PromptTemplate,
)


class CountingBundle(Plugin):

    def __init__(self, plugin_id: str, enabled: bool = True, priority: int = 0):
        super().__init__(
            PluginManifest(plugin_id=plugin_id, enabled=enabled, priority=priority)
        )
        self.shutdown_count = 0

    async def register_agent_configs(
        self, agent_context=None
    ) -> list[AgentConfig] | None:
        return None

    async def on_worker_shutdown(self, worker):
        self.shutdown_count += 1

    async def on_task_start(self, context):
        context["order"].append(self.name)


class SlowBundle(Plugin):

    def __init__(self, plugin_id: str, hook_timeout_seconds: float):
        super().__init__(
            PluginManifest(plugin_id=plugin_id),
            hook_timeout_seconds=hook_timeout_seconds,
        )
        self.completed = False

    async def register_agent_configs(
        self, agent_context=None
    ) -> list[AgentConfig] | None:
        return None

    async def on_task_start(self, context):
        await asyncio.sleep(0.05)
        self.completed = True


class AtomicBundle(Plugin):

    def __init__(self):
        super().__init__(PluginManifest(plugin_id="atomic_bundle", version="2.0.0"))

    async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
        return [
            AgentConfig(
                agent_id="atomic_agent",
                tools={"bundle_tool": lambda: "ok"},
                prompts={"bundle_prompt": PromptTemplate(content="hello")},
            )
        ]


class FailingBundle(Plugin):

    def __init__(self):
        super().__init__(PluginManifest(plugin_id="failing_bundle", version="1.0.0"))

    async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
        raise RuntimeError("register failed")


@pytest.mark.asyncio
async def test_initialize_plugins_is_idempotent_and_respects_enabled():
    """Test that initialize_plugins is idempotent and skips disabled plugins."""
    registry = PluginRegistry()
    enabled_bundle = CountingBundle(plugin_id="enabled_bundle", enabled=True)
    disabled_bundle = CountingBundle(plugin_id="disabled_bundle", enabled=False)

    registry.register_bundle(enabled_bundle)
    registry.register_bundle(disabled_bundle)

    await registry.initialize_plugins()
    await registry.initialize_plugins()

    assert "enabled_bundle" in registry.get_hook_stats()
    assert "disabled_bundle" not in registry.get_hook_stats()


@pytest.mark.asyncio
async def test_lifecycle_skips_disabled_plugins():
    """Test that lifecycle hooks are not called for disabled plugins."""
    registry = PluginRegistry()
    enabled_bundle = CountingBundle(plugin_id="enabled_bundle", enabled=True)
    disabled_bundle = CountingBundle(plugin_id="disabled_bundle", enabled=False)

    registry.register_bundle(enabled_bundle)
    registry.register_bundle(disabled_bundle)

    await registry.on_worker_shutdown(worker=object())

    assert enabled_bundle.shutdown_count == 1
    assert disabled_bundle.shutdown_count == 0


@pytest.mark.asyncio
async def test_lifecycle_orders_by_priority_then_name():
    """Test that lifecycle hooks are called in priority order (lowest first), then by name."""
    registry = PluginRegistry()
    p1 = CountingBundle(plugin_id="b_bundle", priority=10)
    p2 = CountingBundle(plugin_id="a_bundle", priority=10)
    p3 = CountingBundle(plugin_id="c_bundle", priority=1)

    registry.register_bundle(p1)
    registry.register_bundle(p2)
    registry.register_bundle(p3)

    context = {"order": []}
    await registry.on_task_start(context)

    assert context["order"] == ["c_bundle", "a_bundle", "b_bundle"]


@pytest.mark.asyncio
async def test_agent_config_conflict_strategy_error_skip_overwrite():
    """Test that agent config conflict strategies (error, skip, overwrite) work correctly."""
    registry = PluginRegistry()

    class FirstBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="first"))

        async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
            return [AgentConfig(agent_id="dup_agent")]

    class ErrorBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="error"))

        async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
            return [AgentConfig(agent_id="dup_agent", on_conflict="error")]

    class SkipBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="skip"))

        async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
            return [AgentConfig(agent_id="dup_agent", on_conflict="skip")]

    class OverwriteBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="overwrite"))

        async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
            return [AgentConfig(agent_id="dup_agent", on_conflict="overwrite")]

    registry.register_bundle(FirstBundle())
    await registry.initialize_plugins()

    registry.register_bundle(ErrorBundle())
    await registry.initialize_plugins()
    assert registry.agent_config("dup_agent") is not None

    registry.register_bundle(SkipBundle())
    await registry.initialize_plugins()

    registry.register_bundle(OverwriteBundle())
    await registry.initialize_plugins()

    assert registry.agent_config("dup_agent") is not None


@pytest.mark.asyncio
async def test_agent_context_call_tool_supports_sync_and_async():
    """Test that AgentContext.call_tool works with both sync and async tools."""
    registry = PluginRegistry()

    def sync_tool(x: int):
        return x + 1

    async def async_tool(x: int):
        return x + 2

    class ToolsBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="tools_bundle"))

        async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
            return [
                AgentConfig(
                    agent_id="tools_agent",
                    tools={"sync_tool": sync_tool, "async_tool": async_tool},
                )
            ]

    registry.register_bundle(ToolsBundle())
    await registry.initialize_plugins()

    context = AgentContext(
        session_id="s-1",
        trace_id="t-1",
        redis_client=object(),
    )
    context.set_agent_configs(registry.agent_configs)

    assert await context.call_tool("sync_tool", x=3) == 4
    assert await context.call_tool("async_tool", x=3) == 5


def test_prompt_template_render_reports_missing_variables():
    """Test that PromptTemplate.render raises KeyError with message about missing variables."""
    prompt = PromptTemplate(content="Hello {name}, from {city}")

    with pytest.raises(KeyError) as exc_info:
        prompt.render(name="Alice")

    assert "missing variables" in str(exc_info.value)
    assert "city" in str(exc_info.value)


@pytest.mark.asyncio
async def test_hook_timeout_and_stats_are_recorded():
    """Test that hook timeout is recorded in stats when a hook exceeds timeout."""
    registry = PluginRegistry()
    slow = SlowBundle(plugin_id="slow_bundle", hook_timeout_seconds=0.01)
    registry.register_bundle(slow)

    await registry.on_task_start({"order": []})

    assert slow.completed is False

    stats = registry.get_hook_stats()
    assert "slow_bundle" in stats
    assert "on_task_start" in stats["slow_bundle"]
    hook_stat = stats["slow_bundle"]["on_task_start"]
    assert hook_stat["failure"] == 1
    assert hook_stat["timeout"] == 1
    assert hook_stat["total_runs"] == 1
    assert hook_stat["avg_ms"] >= 0


@pytest.mark.asyncio
async def test_hook_stats_record_success_and_failure():
    """Test that hook stats correctly record success and failure counts."""

    class FailingBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="failing"))

        async def register_agent_configs(
            self, agent_context=None
        ) -> list[AgentConfig] | None:
            return None

        async def on_task_complete(self, context, result):
            raise RuntimeError("boom")

    class SuccessBundle(Plugin):

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="ok"))

        async def register_agent_configs(
            self, agent_context=None
        ) -> list[AgentConfig] | None:
            return None

        async def on_task_complete(self, context, result):
            return None

    registry = PluginRegistry()
    registry.register_bundle(FailingBundle())
    registry.register_bundle(SuccessBundle())

    await registry.on_task_complete({"order": []}, result={"ok": True})

    stats = registry.get_hook_stats()
    assert stats["failing"]["on_task_complete"]["failure"] == 1
    assert stats["failing"]["on_task_complete"]["success"] == 0
    assert stats["ok"]["on_task_complete"]["success"] == 1
    assert stats["ok"]["on_task_complete"]["failure"] == 0


def test_apply_default_hook_timeout_only_fills_unset_values():
    """Test that apply_default_hook_timeout only sets timeout on plugins that don't have it set."""
    registry = PluginRegistry()
    p1 = CountingBundle(plugin_id="p1")
    p2 = CountingBundle(plugin_id="p2")
    p2.hook_timeout_seconds = 3.0

    registry.register_bundle(p1)
    registry.register_bundle(p2)

    registry.apply_default_hook_timeout(1.5)

    assert p1.hook_timeout_seconds == 1.5
    assert p2.hook_timeout_seconds == 3.0


@pytest.mark.asyncio
async def test_reset_hook_stats_behaviors():
    """Test that reset_hook_stats can reset stats for specific plugins/hooks or all at once."""
    registry = PluginRegistry()
    registry.register_bundle(CountingBundle(plugin_id="p1"))
    registry.register_bundle(CountingBundle(plugin_id="p2"))

    await registry.on_task_start({"order": []})
    stats = registry.get_hook_stats()
    assert "p1" in stats and "p2" in stats

    registry.reset_hook_stats(plugin_name="p1", hook_name="on_task_start")
    stats = registry.get_hook_stats()
    assert "p1" not in stats
    assert "p2" in stats

    registry.reset_hook_stats(plugin_name="p2")
    stats = registry.get_hook_stats()
    assert "p2" not in stats

    await registry.on_task_start({"order": []})
    stats = registry.get_hook_stats()
    assert "p1" in stats and "p2" in stats

    registry.reset_hook_stats()
    assert registry.get_hook_stats() == {}


@pytest.mark.asyncio
async def test_agent_config_registration():
    """Test that AtomicBundle correctly registers agent config with tools and prompts."""
    registry = PluginRegistry()
    registry.register_bundle(AtomicBundle())

    await registry.initialize_plugins()

    config = registry.agent_config("atomic_agent")
    assert config is not None
    assert "bundle_tool" in config.tools
    assert "bundle_prompt" in config.prompts


@pytest.mark.asyncio
async def test_registration_failure_does_not_leave_partial_state():
    """Test that if a plugin's register_agent_configs raises, no partial state is left."""
    registry = PluginRegistry()
    registry.register_bundle(FailingBundle())

    await registry.initialize_plugins()

    assert registry.agent_config("failing_bundle") is None
