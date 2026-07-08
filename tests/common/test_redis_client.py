import os
import unittest
from unittest.mock import MagicMock, patch

from by_framework.client.client import GatewayClient
from by_framework.common.config import RedisConfig
from by_framework.common.exceptions import RedisConnectionError
from by_framework.common.redis_client import close_redis, get_redis, init_redis
from by_framework.core.discovery import DiscoveryClient, ServiceRegistry
from by_framework.core.registry import WorkerRegistry

REDIS_ENV_VARS = (
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_DB",
    "REDIS_PASSWORD",
    "REDIS_USERNAME",
    "REDIS_MAX_CONNECTIONS",
    "REDIS_MODE",
    "REDIS_CLUSTER_NODES",
    "REDIS_CLUSTER_HOST",
    "REDIS_KEY_SCHEMA_VERSION",
)


class TestRedisClient(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # Manually clear the singleton; don't call close_redis (may have MagicMock)
        import by_framework.common.redis_client as rc

        if rc._redis_client is not None:
            rc._redis_client = None

    async def test_init_redis_cluster_mode_requires_v2_schema(self):
        """Cluster mode with the default (v1) key schema must fail fast,
        synchronously, without constructing any client."""
        old_value = os.environ.get("REDIS_KEY_SCHEMA_VERSION")
        os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)
        try:
            with (
                patch("by_framework.common.redis_client.Redis") as mock_redis_cls,
                patch(
                    "by_framework.common.redis_client.RedisCluster"
                ) as mock_cluster_cls,
            ):
                with self.assertRaises(RedisConnectionError):
                    init_redis(
                        config=RedisConfig(
                            mode="cluster",
                            cluster_nodes=[("unreachable-host", 6379)],
                        )
                    )
                mock_redis_cls.assert_not_called()
                mock_cluster_cls.assert_not_called()
        finally:
            if old_value is not None:
                os.environ["REDIS_KEY_SCHEMA_VERSION"] = old_value
            else:
                os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)

    async def test_init_redis_cluster_mode_with_v2_schema_builds_cluster_client(self):
        """Cluster mode + v2 schema constructs RedisCluster, not standalone Redis."""
        old_value = os.environ.get("REDIS_KEY_SCHEMA_VERSION")
        os.environ["REDIS_KEY_SCHEMA_VERSION"] = "v2"
        try:
            with (
                patch("by_framework.common.redis_client.Redis") as mock_redis_cls,
                patch(
                    "by_framework.common.redis_client.RedisCluster"
                ) as mock_cluster_cls,
            ):
                init_redis(
                    config=RedisConfig(
                        mode="cluster",
                        cluster_nodes=[("h1", 6379), ("h2", 6380)],
                    )
                )

                mock_redis_cls.assert_not_called()
                mock_cluster_cls.assert_called_once()
                _, kwargs = mock_cluster_cls.call_args
                self.assertEqual(
                    [(n.host, n.port) for n in kwargs["startup_nodes"]],
                    [("h1", 6379), ("h2", 6380)],
                )
        finally:
            if old_value is not None:
                os.environ["REDIS_KEY_SCHEMA_VERSION"] = old_value
            else:
                os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)

    async def test_init_redis_from_config_standalone(self):
        """Verify that a standalone RedisConfig builds the standalone client."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(config=RedisConfig(host="redis.example.com", port=6380))

            args, kwargs = mock_redis_cls.call_args
            self.assertEqual(kwargs["host"], "redis.example.com")
            self.assertEqual(kwargs["port"], 6380)

    async def test_init_redis_config_none_max_conns_keeps_explicit_kwarg(
        self,
    ):
        """An explicitly-passed max_connections kwarg must win when
        config.max_connections is None ("not specified"), not be silently
        discarded — this is what worker/app.py's cluster branch relies on
        to scale the connection pool with max_concurrency."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(config=RedisConfig(max_connections=None), max_connections=60)

            _, kwargs = mock_redis_cls.call_args
            self.assertEqual(kwargs["max_connections"], 60)

    async def test_init_redis_config_max_connections_set_still_wins_over_explicit_kwarg(
        self,
    ):
        """When config.max_connections IS explicitly set, it still takes
        precedence over a separately-passed max_connections kwarg (config
        represents the caller's fuller intent when both are given)."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(config=RedisConfig(max_connections=100), max_connections=60)

            _, kwargs = mock_redis_cls.call_args
            self.assertEqual(kwargs["max_connections"], 100)

    async def test_init_redis_singleton(self):
        """Verify that init_redis returns a singleton."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            client1 = init_redis(host="localhost", port=6379)
            client2 = init_redis(host="localhost", port=6379)

            # Constructor should only be called once
            self.assertEqual(mock_redis_cls.call_count, 1)
            self.assertIs(client1, client2)

    async def test_init_redis_with_max_connections(self):
        """Verify max_connections param is passed to Redis constructor."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(max_connections=42)

            # Check parameters passed to Redis constructor
            args, kwargs = mock_redis_cls.call_args
            self.assertEqual(kwargs["max_connections"], 42)
            self.assertTrue(kwargs["decode_responses"])

    async def test_get_redis_auto_init(self):
        """Verify that get_redis automatically calls init_redis when not initialized."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            client = get_redis()
            self.assertEqual(mock_redis_cls.call_count, 1)
            self.assertIsNotNone(client)

    async def test_get_redis_uses_standalone_defaults_without_env(self):
        """The default singleton path remains localhost standalone compatible."""
        with patch.dict(os.environ, {}, clear=False):
            for name in REDIS_ENV_VARS:
                os.environ.pop(name, None)

            with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
                get_redis()

                _, kwargs = mock_redis_cls.call_args
                self.assertEqual(kwargs["host"], "localhost")
                self.assertEqual(kwargs["port"], 6379)
                self.assertEqual(kwargs["db"], 0)
                self.assertTrue(kwargs["decode_responses"])

    async def test_get_redis_uses_cluster_config_from_env(self):
        """The shared default lookup honors REDIS_MODE=cluster env config."""
        with patch.dict(
            os.environ,
            {
                "REDIS_MODE": "cluster",
                "REDIS_CLUSTER_NODES": "h1:6379,h2:6380",
                "REDIS_USERNAME": "cluster-user",
                "REDIS_PASSWORD": "cluster-secret",
                "REDIS_KEY_SCHEMA_VERSION": "v2",
            },
            clear=False,
        ):
            with (
                patch("by_framework.common.redis_client.Redis") as mock_redis_cls,
                patch(
                    "by_framework.common.redis_client.RedisCluster"
                ) as mock_cluster_cls,
            ):
                get_redis()

                mock_redis_cls.assert_not_called()
                mock_cluster_cls.assert_called_once()
                _, kwargs = mock_cluster_cls.call_args
                self.assertEqual(
                    [(n.host, n.port) for n in kwargs["startup_nodes"]],
                    [("h1", 6379), ("h2", 6380)],
                )
                self.assertEqual(kwargs["username"], "cluster-user")
                self.assertEqual(kwargs["password"], "cluster-secret")
                self.assertTrue(kwargs["decode_responses"])

    async def test_get_redis_uses_cluster_host_env_without_explicit_mode(self):
        """Setting only REDIS_CLUSTER_HOST (no REDIS_MODE) is enough to
        switch the shared default lookup to cluster mode."""
        with patch.dict(
            os.environ,
            {
                "REDIS_CLUSTER_HOST": "10.10.168.203:6371,10.10.168.203:6372",
                "REDIS_KEY_SCHEMA_VERSION": "v2",
            },
            clear=False,
        ):
            os.environ.pop("REDIS_MODE", None)
            with (
                patch("by_framework.common.redis_client.Redis") as mock_redis_cls,
                patch(
                    "by_framework.common.redis_client.RedisCluster"
                ) as mock_cluster_cls,
            ):
                get_redis()

                mock_redis_cls.assert_not_called()
                mock_cluster_cls.assert_called_once()
                _, kwargs = mock_cluster_cls.call_args
                self.assertEqual(
                    [(n.host, n.port) for n in kwargs["startup_nodes"]],
                    [("10.10.168.203", 6371), ("10.10.168.203", 6372)],
                )

    async def test_get_redis_cluster_env_requires_v2_schema(self):
        """Cluster env config fails fast before constructing a client."""
        with patch.dict(
            os.environ,
            {
                "REDIS_MODE": "cluster",
                "REDIS_CLUSTER_NODES": "h1:6379,h2:6380",
            },
            clear=False,
        ):
            os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)
            with (
                patch("by_framework.common.redis_client.Redis") as mock_redis_cls,
                patch(
                    "by_framework.common.redis_client.RedisCluster"
                ) as mock_cluster_cls,
            ):
                with self.assertRaises(RedisConnectionError):
                    get_redis()

                mock_redis_cls.assert_not_called()
                mock_cluster_cls.assert_not_called()

    async def test_default_component_construction_uses_cluster_env(self):
        """Public constructors using the shared default get a cluster client."""
        with patch.dict(
            os.environ,
            {
                "REDIS_MODE": "cluster",
                "REDIS_CLUSTER_NODES": "h1:6379,h2:6380",
                "REDIS_KEY_SCHEMA_VERSION": "v2",
            },
            clear=False,
        ):
            with patch(
                "by_framework.common.redis_client.RedisCluster"
            ) as mock_cluster_cls:
                service_registry = ServiceRegistry()
                worker_registry = WorkerRegistry()
                gateway_client = GatewayClient()

                mock_cluster_cls.assert_called_once()
                self.assertIs(service_registry.redis, mock_cluster_cls.return_value)
                self.assertIs(worker_registry.redis, mock_cluster_cls.return_value)
                self.assertIs(gateway_client.redis, mock_cluster_cls.return_value)

    async def test_explicit_redis_clients_take_precedence(self):
        """Injected Redis clients are not replaced by the shared default lookup."""
        redis = MagicMock()

        with (
            patch("by_framework.core.discovery.get_redis") as discovery_get_redis,
            patch("by_framework.core.registry.get_redis") as registry_get_redis,
            patch("by_framework.client.client.get_redis") as client_get_redis,
        ):
            service_registry = ServiceRegistry(redis_client=redis)
            discovery_client = DiscoveryClient(redis_client=redis)
            worker_registry = WorkerRegistry(redis_client=redis)
            gateway_client = GatewayClient(redis_client=redis)

            discovery_get_redis.assert_not_called()
            registry_get_redis.assert_not_called()
            client_get_redis.assert_not_called()
            self.assertIs(service_registry.redis, redis)
            self.assertIs(discovery_client.redis, redis)
            self.assertIs(worker_registry.redis, redis)
            self.assertIs(gateway_client.redis, redis)

    async def test_close_redis(self):
        """Verify that close_redis closes the connection and clears the singleton."""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            mock_instance = mock_redis_cls.return_value

            async def mock_aclose():
                pass

            mock_instance.aclose = mock_aclose

            init_redis()
            self.assertIsNotNone(get_redis())

            await close_redis()

            # Getting again should trigger new initialization
            get_redis()
            self.assertEqual(mock_redis_cls.call_count, 2)


if __name__ == "__main__":
    unittest.main()
