"""
Agent configuration management.

Provides management for AgentConfig instances including:
- Storage and retrieval
- Search and filtering
- Conflict resolution
- Default agent configuration handling
"""

from typing import Dict, Iterable, List, Optional

from by_framework.core.extensions import AgentConfig


class AgentConfigManager:
    """Agent configuration manager.

    Provides management operations for AgentConfig instances.
    """

    def __init__(
        self,
        configs: Optional[List[AgentConfig]] = None,
    ):
        """Initialize the agent config manager.

        Args:
            configs: Initial list of AgentConfig instances
        """
        self._configs: Dict[str, AgentConfig] = {}

        if configs:
            for config in configs:
                self.add_config(config)

    def add_config(self, config: AgentConfig) -> bool:
        """Add a single agent configuration.

        Args:
            config: AgentConfig instance to add

        Returns:
            True if added successfully, False if already exists
        """
        if config.agent_id in self._configs:
            # Apply conflict strategy
            if config.on_conflict == "overwrite":
                self._configs[config.agent_id] = config
                return True
            elif config.on_conflict == "error":
                raise ValueError(
                    f"AgentConfig with id '{config.agent_id}' already exists"
                )
            elif config.on_conflict == "skip":
                return False

        self._configs[config.agent_id] = config
        return True

    def add_configs(self, configs: Iterable[AgentConfig]) -> List[bool]:
        """Add multiple agent configurations.

        Args:
            configs: Iterable of AgentConfig instances to add

        Returns:
            List of boolean success/failure indicators for each config
        """
        results = []
        for config in configs:
            try:
                results.append(self.add_config(config))
            except Exception:  # pylint: disable=broad-exception-caught
                results.append(False)
        return results

    def remove_config(self, agent_id: str) -> bool:
        """Remove an agent configuration.

        Args:
            agent_id: ID of the configuration to remove

        Returns:
            True if removed successfully, False if not found
        """
        if agent_id in self._configs:
            del self._configs[agent_id]
            return True
        return False

    def remove_all_configs(self) -> None:
        """Remove all agent configurations."""
        self._configs.clear()

    def get_config(self, agent_id: str) -> Optional[AgentConfig]:
        """Get an agent configuration by ID.

        Args:
            agent_id: ID of the configuration to retrieve

        Returns:
            AgentConfig instance or None if not found
        """
        return self._configs.get(agent_id)

    def list_configs(self) -> List[AgentConfig]:
        """Get all agent configurations as a list.

        Returns:
            List of all AgentConfig instances
        """
        return list(self._configs.values())

    def list_agent_ids(self) -> List[str]:
        """Get all agent configuration IDs.

        Returns:
            List of all agent IDs
        """
        return list(self._configs.keys())

    def count(self) -> int:
        """Get the number of agent configurations.

        Returns:
            Number of AgentConfig instances stored
        """
        return len(self._configs)

    def has_config(self, agent_id: str) -> bool:
        """Check if an agent configuration exists.

        Args:
            agent_id: ID to check

        Returns:
            True if configuration exists, False otherwise
        """
        return agent_id in self._configs

    def search_configs(
        self,
        name: Optional[str] = None,
        tool_name: Optional[str] = None,
        callback_type: Optional[str] = None,
        has_sub_agents: Optional[bool] = None,
    ) -> List[AgentConfig]:
        """Search for agent configurations based on criteria.

        Args:
            name: Name or partial name to match
            tool_name: Tool name to check for
            callback_type: Callback type to check for
            has_sub_agents: Whether to filter by sub-agent presence

        Returns:
            List of matching AgentConfig instances
        """
        results = self.list_configs()

        if name:
            name_lower = name.lower()
            results = [
                c
                for c in results
                if name_lower in c.name.lower() or name_lower in c.agent_id.lower()
            ]

        if tool_name:
            results = [c for c in results if tool_name in c.tools]

        if callback_type:
            results = [
                c
                for c in results
                if callback_type in c.callbacks and c.callbacks[callback_type]
            ]

        if has_sub_agents is not None:
            if has_sub_agents:
                results = [c for c in results if c.sub_agents]
            else:
                results = [c for c in results if not c.sub_agents]

        return results

    def get_agent_by_tool(self, tool_name: str) -> List[AgentConfig]:
        """Get all agent configurations that provide a specific tool.

        Args:
            tool_name: Name of the tool to search for

        Returns:
            List of AgentConfig instances that have the tool
        """
        return [c for c in self.list_configs() if tool_name in c.tools]

    def get_agent_by_skill(self, skill_name: str) -> List[AgentConfig]:
        """Get all agent configurations that have a specific skill.

        Args:
            skill_name: Name of the skill to search for

        Returns:
            List of AgentConfig instances that have the skill
        """
        return [c for c in self.list_configs() if skill_name in c.skills]

    def get_agent_by_knowledge_base(self, kb_name: str) -> List[AgentConfig]:
        """Get all agent configurations with a specific knowledge base.

        Args:
            kb_name: Name of the knowledge base to search for

        Returns:
            List of AgentConfig instances that have the knowledge base
        """
        return [c for c in self.list_configs() if kb_name in c.knowledge_bases]

    def get_sub_agents(self, agent_id: str) -> List[AgentConfig]:
        """Get sub-agent configurations for a specific agent.

        Args:
            agent_id: Parent agent ID

        Returns:
            List of sub-agent AgentConfig instances
        """
        config = self.get_config(agent_id)
        if not config or not config.sub_agents:
            return []

        sub_agents = []
        for sub_agent_id in config.sub_agents:
            sub_config = self.get_config(sub_agent_id)
            if sub_config:
                sub_agents.append(sub_config)

        return sub_agents

    def update_config(self, agent_id: str, updates: dict) -> Optional[AgentConfig]:
        """Update an existing agent configuration.

        Args:
            agent_id: ID of the agent configuration to update
            updates: Dictionary of fields to update

        Returns:
            Updated AgentConfig instance or None if not found
        """
        if agent_id not in self._configs:
            return None

        config = self._configs[agent_id]
        for key, value in updates.items():
            if hasattr(config, key):
                setattr(config, key, value)

        return self._configs[agent_id]

    def set_configs(self, configs: Iterable[AgentConfig]) -> None:
        """Replace all agent configurations with the given list.

        Args:
            configs: New list of AgentConfig instances
        """
        self._configs.clear()
        self._default_agent_id = None

        for config in configs:
            self.add_config(config)

    def to_dict(self) -> dict:
        """Convert the config manager state to a dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "agent_count": self.count(),
            "agent_ids": self.list_agent_ids(),
        }
