import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework.common.constants import RedisKeys
from by_framework.core.discovery import DiscoveryClient, ServiceRegistry


@pytest.fixture
def mock_redis():
    mock = MagicMock()
    # Async methods for direct execution
    mock.hset = AsyncMock()
    mock.zadd = AsyncMock()
    mock.sadd = AsyncMock()
    mock.zrangebyscore = AsyncMock()
    mock.zrange = AsyncMock()
    mock.hmget = AsyncMock()

    # Sync methods returning objects
    pipeline_mock = MagicMock()
    mock.pipeline.return_value = pipeline_mock
    pipeline_mock.hdel = MagicMock()
    pipeline_mock.zrem = MagicMock()
    pipeline_mock.execute = AsyncMock()

    return mock


@pytest.mark.asyncio
async def test_register_service(mock_redis):
    discovery = ServiceRegistry(redis_client=mock_redis)
    service_name = "test-service"
    host = "127.0.0.1"
    port = 8080

    # Start registration
    await discovery.register(service_name, host, port, metadata={"version": "1.0"})

    # Sub-method verification
    assert mock_redis.hset.called
    assert mock_redis.sadd.called

    await discovery.unregister()


@pytest.mark.asyncio
async def test_register_automatic_discovery_with_redis_host(mock_redis):
    # Simulate redis client connection configuration
    mock_redis.connection_pool.connection_kwargs = {"host": "10.0.0.5", "port": 6379}
    discovery = ServiceRegistry(redis_client=mock_redis)
    service_name = "auto-service"

    # Mock socket discovery process
    with patch("socket.socket") as mock_sock_cls:
        mock_sock = mock_sock_cls.return_value
        mock_sock.getsockname.return_value = ["10.0.0.100"]

        await discovery.register(service_name)

        # Verify that connection to Redis address was attempted for discovery
        mock_sock.connect.assert_called_with(("10.0.0.5", 6379))

        # Verify collected instance data
        call_args = mock_redis.hset.call_args
        instance_json = call_args.args[2]
        import json

        instance_data = json.loads(instance_json)

        assert instance_data["host"] == "10.0.0.100"
        assert instance_data["port"] == 0

    await discovery.unregister()


@pytest.mark.asyncio
async def test_discovery_client_cache(mock_redis):
    service_name = "cached-service"
    mock_redis.zrange.return_value = [("inst_1", time.time() * 1000)]
    mock_redis.hmget.return_value = ['{"id": "inst_1", "host": "1.1.1.1", "port": 80}']

    # Default cache time is long, ensuring it won't expire during test
    client = DiscoveryClient(redis_client=mock_redis, cache_interval=60)

    # 1. First fetch: query Redis
    insts1 = await client.get_instances(service_name)
    assert len(insts1) == 1
    assert mock_redis.zrange.call_count == 1

    # 2. Second fetch: use cache, don't query Redis
    insts2 = await client.get_instances(service_name)
    assert len(insts2) == 1
    assert mock_redis.zrange.call_count == 1  # Still 1


@pytest.mark.asyncio
async def test_discovery_client_force_refresh(mock_redis):
    service_name = "refresh-service"
    mock_redis.zrange.return_value = [("inst_1", time.time() * 1000)]
    mock_redis.hmget.return_value = ['{"id": "inst_1", "host": "1.1.1.1", "port": 80}']

    client = DiscoveryClient(redis_client=mock_redis)
    await client.get_instances(service_name)
    assert mock_redis.zrange.call_count == 1

    # Force refresh: query Redis again
    await client.get_instances(service_name, force_refresh=True)
    assert mock_redis.zrange.call_count == 2


@pytest.mark.asyncio
async def test_discovery_client_round_robin_lb(mock_redis):
    service_name = "lb-service"
    now_ms = time.time() * 1000
    mock_redis.zrange.return_value = [("i1", now_ms), ("i2", now_ms)]
    mock_redis.hmget.return_value = [
        '{"id": "i1", "host": "1.1.1.1", "port": 80}',
        '{"id": "i2", "host": "2.2.2.2", "port": 81}',
    ]

    client = DiscoveryClient(redis_client=mock_redis)

    # Load balancing should be executed based on cached data
    r1 = await client.discover(service_name, strategy="round-robin")
    r2 = await client.discover(service_name, strategy="round-robin")
    r3 = await client.discover(service_name, strategy="round-robin")

    assert r1.id == "i1"
    assert r2.id == "i2"
    assert r3.id == "i1"


@pytest.mark.asyncio
async def test_register_service_with_protocol_and_path_prefix(mock_redis):
    discovery = ServiceRegistry(redis_client=mock_redis)

    await discovery.register(
        "test-service",
        host="127.0.0.1",
        port=8443,
        protocol="https",
        path_prefix="/v2",
    )

    call_args = mock_redis.hset.call_args
    instance_json = call_args.args[2]

    assert '"protocol": "https"' in instance_json
    assert '"path_prefix": "/v2"' in instance_json

    await discovery.unregister()


@pytest.mark.asyncio
async def test_register_only_writes_visible_instance_without_heartbeat_loop(mock_redis):
    discovery = ServiceRegistry(redis_client=mock_redis)

    await discovery.register_only(
        "register-only-service",
        port=8080,
        protocol="https",
        path_prefix="/api",
    )

    assert discovery._heartbeat_task is None
    assert mock_redis.zadd.await_count == 1
    mock_redis.zadd.assert_awaited_with(
        RedisKeys.sd_active_instances("register-only-service"),
        {
            discovery._current_instance.id: mock_redis.zadd.await_args.args[1][
                discovery._current_instance.id
            ]
        },
    )

    await discovery.unregister()


@pytest.mark.asyncio
async def test_discovery_client_filters_by_health_threshold(mock_redis):
    service_name = "health-service"
    now_ms = int(time.time() * 1000)
    mock_redis.zrange.return_value = [
        ("healthy", now_ms - 10_000),
        ("stale", now_ms - 40_000),
    ]
    mock_redis.hmget.return_value = [
        '{"id": "healthy", "host": "1.1.1.1", "port": 80}',
        '{"id": "stale", "host": "2.2.2.2", "port": 81}',
    ]

    client = DiscoveryClient(redis_client=mock_redis)

    healthy_instances = await client.get_instances(service_name)
    all_instances = await client.get_instances(
        service_name,
        force_refresh=True,
        health_threshold_ms=RedisKeys.SD_NO_HEALTH_CHECK,
    )

    assert [instance.id for instance in healthy_instances] == ["healthy"]
    assert [instance.id for instance in all_instances] == ["healthy", "stale"]
