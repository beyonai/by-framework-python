"""Tests for execution-bound agent config audit projections."""

from by_framework import AgentConfig, PluginRegistry
from by_framework.core.extensions.agent_config_audit import build_agent_config_audit_projection


def test_agent_config_audit_projection_is_json_safe_and_redacted():
    """Execution audit projection keeps searchable config metadata without secrets."""
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="weather-agent",
                name="Weather Agent",
                description="Forecast lookup",
                prompts={"system": "Use api_key=secret-token to answer."},
                tools={
                    "weather_api": {
                        "endpoint": "https://weather.example",
                        "api_key": "secret-token",
                    }
                },
                skills={"forecast": {"version": "2026.6"}},
                knowledge_bases={"weather_docs": {"version": "v2"}},
                sub_agents=["summarizer"],
                extra={"owner": "platform", "password": "hidden"},
            )
        ]
    )

    projection = build_agent_config_audit_projection(
        snapshot=registry.get_agent_configs_snapshot(),
        target_agent_type="weather-agent",
    )

    assert projection["target_agent_type"] == "weather-agent"
    assert projection["version"] == 1
    assert projection["config_count"] == 1
    assert projection["snapshot_hash"].startswith("sha256:")
    assert projection["target_agent_config"]["agent_id"] == "weather-agent"
    assert projection["target_agent_config"]["prompt_keys"] == ["system"]
    assert projection["target_agent_config"]["prompt_hashes"]["system"].startswith(
        "sha256:"
    )
    assert projection["target_agent_config"]["tools"]["weather_api"][
        "config_hash"
    ].startswith("sha256:")
    assert (
        projection["target_agent_config"]["tools"]["weather_api"]["redacted_config"][
            "api_key"
        ]
        == "[REDACTED]"
    )
    assert projection["target_agent_config"]["extra"]["owner"] == "platform"
    assert projection["target_agent_config"]["extra"]["password"] == "[REDACTED]"
    assert "secret-token" not in str(projection)


def test_agent_config_audit_projection_marks_unregistered_target_agent():
    """Workers may handle an agent_type even when no AgentConfig was registered."""
    registry = PluginRegistry()

    projection = build_agent_config_audit_projection(
        snapshot=registry.get_agent_configs_snapshot(),
        target_agent_type="langgraph-extension-demo",
    )

    assert projection["target_agent_type"] == "langgraph-extension-demo"
    assert projection["target_agent_registered"] is False
    assert projection["target_agent_config"]["agent_id"] == "langgraph-extension-demo"
    assert projection["target_agent_config"]["registered"] is False
    assert projection["target_agent_config"]["source"] == "worker_target_agent_type"
