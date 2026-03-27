"""
Tests for by_framework.common.config module.
"""

import os
import unittest

from by_framework.common.config import (
    LoggingConfig,
    RedisConfig,
    SDKConfig,
    WorkerConfig,
    get_config,
    init_config,
)


class TestRedisConfig(unittest.TestCase):
    """Tests for RedisConfig."""

    def test_default_values(self):
        """Test default values."""
        config = RedisConfig()
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 6379)
        self.assertEqual(config.db, 0)
        self.assertEqual(config.password, "")
        self.assertIsNone(config.username)
        self.assertTrue(config.decode_responses)
        self.assertIsNone(config.max_connections)

    def test_frozen_immutability(self):
        """Test that frozen dataclass is immutable."""
        config = RedisConfig()
        with self.assertRaises(AttributeError):
            config.host = "other"

    def test_from_env_with_defaults(self):
        """Test from_env with no environment variables set."""
        # Clear relevant env vars
        env_vars = [
            "REDIS_HOST",
            "REDIS_PORT",
            "REDIS_DB",
            "REDIS_PASSWORD",
            "REDIS_USERNAME",
            "REDIS_MAX_CONNECTIONS",
        ]
        old_values = {k: os.environ.get(k) for k in env_vars}
        try:
            for k in env_vars:
                os.environ.pop(k, None)

            config = RedisConfig.from_env()
            self.assertEqual(config.host, "localhost")
            self.assertEqual(config.port, 6379)
            self.assertEqual(config.db, 0)
        finally:
            for k, v in old_values.items():
                if v is not None:
                    os.environ[k] = v

    def test_from_env_with_custom_values(self):
        """Test from_env with custom environment variables."""
        old_values = {
            k: os.environ.get(k)
            for k in [
                "REDIS_HOST",
                "REDIS_PORT",
                "REDIS_PASSWORD",
                "REDIS_MAX_CONNECTIONS",
            ]
        }
        try:
            os.environ["REDIS_HOST"] = "redis.example.com"
            os.environ["REDIS_PORT"] = "6380"
            os.environ["REDIS_PASSWORD"] = "secret"
            os.environ["REDIS_MAX_CONNECTIONS"] = "100"

            config = RedisConfig.from_env()
            self.assertEqual(config.host, "redis.example.com")
            self.assertEqual(config.port, 6380)
            self.assertEqual(config.password, "secret")
            self.assertEqual(config.max_connections, 100)
        finally:
            for k, v in old_values.items():
                if v is not None:
                    os.environ[k] = v
                else:
                    os.environ.pop(k, None)


class TestWorkerConfig(unittest.TestCase):
    """Tests for WorkerConfig."""

    def test_default_values(self):
        """Test default values."""
        config = WorkerConfig()
        self.assertEqual(config.max_concurrency, 50)
        self.assertEqual(config.fetch_count, 10)
        self.assertEqual(config.heartbeat_interval, 30)
        self.assertEqual(config.lock_ttl_seconds, 60)
        self.assertEqual(config.stream_block_ms, 2000)

    def test_frozen_immutability(self):
        """Test that frozen dataclass is immutable."""
        config = WorkerConfig()
        with self.assertRaises(AttributeError):
            config.max_concurrency = 100


class TestLoggingConfig(unittest.TestCase):
    """Tests for LoggingConfig."""

    def test_default_values(self):
        """Test default values."""
        config = LoggingConfig()
        self.assertEqual(config.level, "INFO")
        self.assertFalse(config.use_json)
        self.assertEqual(config.log_file, "gateway-sdk.log")

    def test_from_env_defaults(self):
        """Test from_env with no environment variables."""
        old_values = {
            k: os.environ.get(k) for k in ["LOG_LEVEL", "LOG_USE_JSON", "LOG_FILE"]
        }
        try:
            for k in ["LOG_LEVEL", "LOG_USE_JSON", "LOG_FILE"]:
                os.environ.pop(k, None)

            config = LoggingConfig.from_env()
            self.assertEqual(config.level, "INFO")
            self.assertFalse(config.use_json)
            self.assertEqual(config.log_file, None)  # "" -> None
        finally:
            for k, v in old_values.items():
                if v is not None:
                    os.environ[k] = v
                else:
                    os.environ.pop(k, None)

    def test_from_env_debug_level(self):
        """Test LOG_LEVEL parsing."""
        old_values = {k: os.environ.get(k) for k in ["LOG_LEVEL"]}
        try:
            os.environ["LOG_LEVEL"] = "debug"
            config = LoggingConfig.from_env()
            self.assertEqual(config.level, "DEBUG")
        finally:
            if old_values["LOG_LEVEL"]:
                os.environ["LOG_LEVEL"] = old_values["LOG_LEVEL"]
            else:
                os.environ.pop("LOG_LEVEL", None)

    def test_from_env_json_true(self):
        """Test LOG_USE_JSON parsing."""
        old_values = os.environ.get("LOG_USE_JSON")
        try:
            os.environ["LOG_USE_JSON"] = "true"
            config = LoggingConfig.from_env()
            self.assertTrue(config.use_json)
        finally:
            if old_values:
                os.environ["LOG_USE_JSON"] = old_values
            else:
                os.environ.pop("LOG_USE_JSON", None)

    def test_from_env_json_1(self):
        """Test LOG_USE_JSON with '1'."""
        old_values = os.environ.get("LOG_USE_JSON")
        try:
            os.environ["LOG_USE_JSON"] = "1"
            config = LoggingConfig.from_env()
            self.assertTrue(config.use_json)
        finally:
            if old_values:
                os.environ["LOG_USE_JSON"] = old_values
            else:
                os.environ.pop("LOG_USE_JSON", None)

    def test_from_env_log_file(self):
        """Test LOG_FILE environment variable."""
        old_values = os.environ.get("LOG_FILE")
        try:
            os.environ["LOG_FILE"] = "/var/log/gateway.log"
            config = LoggingConfig.from_env()
            self.assertEqual(config.log_file, "/var/log/gateway.log")
        finally:
            if old_values:
                os.environ["LOG_FILE"] = old_values
            else:
                os.environ.pop("LOG_FILE", None)


class TestSDKConfig(unittest.TestCase):
    """Tests for SDKConfig."""

    def test_default_values(self):
        """Test default values."""
        config = SDKConfig()
        self.assertIsInstance(config.redis, RedisConfig)
        self.assertIsInstance(config.worker, WorkerConfig)
        self.assertIsInstance(config.logging, LoggingConfig)

    def test_nested_config(self):
        """Test that nested configs are independent."""
        config = SDKConfig()
        self.assertEqual(config.redis.host, "localhost")
        self.assertEqual(config.worker.max_concurrency, 50)
        self.assertEqual(config.logging.level, "INFO")


class TestGlobalConfigFunctions(unittest.TestCase):
    """Tests for global config functions."""

    def setUp(self):
        """Save original config state."""
        import by_framework.common.config as config_module

        self._original_config = config_module._config

    def tearDown(self):
        """Restore original config state."""
        import by_framework.common.config as config_module

        config_module._config = self._original_config

    def test_get_config_returns_singleton(self):
        """Test that get_config returns the same instance."""
        custom_config = SDKConfig(redis=RedisConfig(host="custom-host"))
        init_config(custom_config)

        result = get_config()
        self.assertIs(result, custom_config)

    def test_init_config_sets_global(self):
        """Test that init_config sets the global config."""
        custom_config = SDKConfig(redis=RedisConfig(host="test-host"))
        init_config(custom_config)

        import by_framework.common.config as config_module

        self.assertIs(config_module._config, custom_config)

    def test_get_config_lazy_loads(self):
        """Test that get_config loads from env when not set."""
        import by_framework.common.config as config_module

        config_module._config = None

        result = get_config()
        self.assertIsInstance(result, SDKConfig)


if __name__ == "__main__":
    unittest.main()
