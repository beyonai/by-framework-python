import unittest
from unittest.mock import patch

from by_framework.common.redis_client import (close_redis, get_redis, init_redis)


class TestRedisClient(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # 手动清除单例，不调用可能包含 MagicMock 的 close_redis
        import by_framework.common.redis_client as rc

        if rc._redis_client is not None:
            rc._redis_client = None

    async def test_init_redis_singleton(self):
        """验证 init_redis 返回单例"""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            client1 = init_redis(host="localhost", port=6379)
            client2 = init_redis(host="localhost", port=6379)

            # 构造函数应该只被调用一次
            self.assertEqual(mock_redis_cls.call_count, 1)
            self.assertIs(client1, client2)

    async def test_init_redis_with_max_connections(self):
        """验证 max_connections 参数正确传递给 Redis 构造函数"""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(max_connections=42)

            # 检查传给 Redis 构造函数的参数
            args, kwargs = mock_redis_cls.call_args
            self.assertEqual(kwargs["max_connections"], 42)
            self.assertTrue(kwargs["decode_responses"])

    async def test_get_redis_auto_init(self):
        """验证 get_redis 在未初始化时会自动调用 init_redis"""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            client = get_redis()
            self.assertEqual(mock_redis_cls.call_count, 1)
            self.assertIsNotNone(client)

    async def test_close_redis(self):
        """验证 close_redis 会关闭连接并清除单例"""
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            mock_instance = mock_redis_cls.return_value

            async def mock_aclose():
                pass

            mock_instance.aclose = mock_aclose

            init_redis()
            self.assertIsNotNone(get_redis())

            await close_redis()

            # 再次获取应该触发新的初始化
            get_redis()
            self.assertEqual(mock_redis_cls.call_count, 2)


if __name__ == "__main__":
    unittest.main()
