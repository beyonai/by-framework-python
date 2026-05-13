import unittest
from unittest.mock import patch

from by_framework.common.redis_client import close_redis, get_redis, init_redis


class TestRedisClient(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # Manually clear the singleton; don't call close_redis (may have MagicMock)
        import by_framework.common.redis_client as rc

        if rc._redis_client is not None:
            rc._redis_client = None

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
