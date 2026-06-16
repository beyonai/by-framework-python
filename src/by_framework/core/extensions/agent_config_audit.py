"""JSON-safe audit projection for execution-bound agent configuration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from typing import Any

from .agent_config import AgentConfig, CallbackType
from .plugin import AgentConfigsSnapshot

SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
)


def build_agent_config_audit_projection(
    *,
    snapshot: AgentConfigsSnapshot,
    target_agent_type: str,
) -> dict[str, Any]:
    """Build a redacted, JSON-safe projection of an agent config snapshot."""
    config_projections = [
        _agent_config_projection(config) for config in sorted(snapshot.configs, key=_id)
    ]
    target_config = next(
        (
            config
            for config in config_projections
            if config.get("agent_id") == target_agent_type
        ),
        None,
    )
    target_agent_registered = target_config is not None
    if target_config is None and target_agent_type:
        target_config = _unregistered_target_projection(target_agent_type)
    payload = {
        "version": int(snapshot.version),
        "target_agent_type": target_agent_type,
        "target_agent_registered": target_agent_registered,
        "config_count": len(config_projections),
        "agent_ids": [str(config.get("agent_id", "")) for config in config_projections],
        "target_agent_config": target_config,
        "configs": config_projections,
    }
    payload["snapshot_hash"] = _hash_payload(payload)
    return payload


def _agent_config_projection(config: AgentConfig) -> dict[str, Any]:
    tools = {
        name: {
            "config_hash": _hash_payload(value),
            "redacted_config": _redacted_json(value),
        }
        for name, value in sorted(config.tools.items())
    }
    return {
        "agent_id": config.agent_id,
        "registered": True,
        "source": "agent_config",
        "name": config.name,
        "description": config.description,
        "prompt_keys": sorted(str(key) for key in config.prompts.keys()),
        "prompt_hashes": {
            str(key): _hash_payload(value)
            for key, value in sorted(config.prompts.items())
        },
        "tools": tools,
        "skills": _redacted_json(config.skills),
        "knowledge_bases": _redacted_json(config.knowledge_bases),
        "sub_agents": list(config.sub_agents),
        "extra": _redacted_json(config.extra),
        "on_conflict": config.on_conflict,
    }


def _unregistered_target_projection(agent_id: str) -> dict[str, Any]:
    return {
        "agent_id": agent_id,
        "registered": False,
        "source": "worker_target_agent_type",
        "name": agent_id,
        "description": (
            "Worker handled this agent_type, but no AgentConfig was registered."
        ),
        "prompt_keys": [],
        "prompt_hashes": {},
        "tools": {},
        "skills": {},
        "knowledge_bases": {},
        "sub_agents": [],
        "extra": {},
        "on_conflict": "",
    }


def _id(config: AgentConfig) -> str:
    return config.agent_id


def _hash_payload(value: Any) -> str:
    encoded = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _redacted_json(value: Any) -> Any:
    return _json_safe(value, redact=True)


def _json_safe(value: Any, *, redact: bool = False, key_hint: str = "") -> Any:
    if redact and _is_sensitive_key(key_hint):
        return "[REDACTED]"
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, CallbackType):
        return value.value
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item, redact=redact, key_hint=str(key))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item, redact=redact, key_hint=key_hint) for item in value]
    if is_dataclass(value):
        return _json_safe(asdict(value), redact=redact, key_hint=key_hint)
    return repr(value)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in SENSITIVE_KEY_PARTS)
