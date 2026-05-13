"""Service discovery module with Redis-based registry and local cache."""

import asyncio
import json
import random
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis, get_redis


def get_local_ip(target_host: str = "8.8.8.8", target_port: int = 80) -> str:
    """Get the local machine's outbound IP address.

    Detects the local network interface IP by attempting to connect to the target
    host (without sending data). If target_host is localhost or 127.0.0.1, attempts
    to connect to a public address for detection. If that fails or the environment
    is restricted, attempts to return a non-loopback address.
    """
    # If the target is a loopback address, detection may return 127.0.0.1, which is
    # meaningless for cross-machine communication. In this case, try connecting to a
    # public or external address to detect the actual local outbound IP
    is_loopback = target_host in ("127.0.0.1", "localhost", "::1")
    actual_target = ("8.8.8.8", 80) if is_loopback else (target_host, target_port)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(actual_target)
        ip = s.getsockname()[0]
    except Exception:  # pylint: disable=broad-exception-caught
        # Fallback: get IP for the hostname
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:  # pylint: disable=broad-exception-caught
            ip = "127.0.0.1"
    finally:
        s.close()

    # If the acquired address is a loopback but other network interfaces exist,
    # more complex scanning could be expanded here
    return ip


@dataclass
class ServiceInstance:
    """Service instance data structure."""

    id: str
    host: str
    port: int
    protocol: str = "http"
    path_prefix: Optional[str] = None
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_heartbeat: int = field(default=0, repr=False)

    def to_json(self) -> str:
        payload = asdict(self)
        payload.pop("last_heartbeat", None)
        return json.dumps(payload, ensure_ascii=False)

    @classmethod
    def from_json(cls, data: str) -> "ServiceInstance":
        return cls(**json.loads(data))


class ServiceRegistry:
    """Redis-based service registry SDK.

    Used by server-side for service registration, automatic heartbeat
    maintenance, and deregistration.
    """

    def __init__(self, redis_client: Optional[Redis] = None):
        self.redis = redis_client or get_redis()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._current_instance: Optional[ServiceInstance] = None
        self._current_service_name: Optional[str] = None

    async def register(
        self,
        service_name: str,
        host: Optional[str] = None,
        port: int = 0,
        weight: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
        heartbeat_interval: int = RedisKeys.SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        protocol: str = "http",
        path_prefix: Optional[str] = None,
    ) -> None:
        """Register the current service instance and start background heartbeat.

        Args:
            service_name: Service name.
            host: Instance listening address. If None, auto-detected from Redis
                connection address.
            port: Instance listening port. Defaults to 0 (meaning no listening port).
            weight: Load balancing weight.
            metadata: Metadata.
            heartbeat_interval: Heartbeat interval.
            protocol: Service protocol used by discovery-aware clients.
            path_prefix: Optional URL prefix advertised by this instance.
        """
        if host is None:
            # Attempt to get target address from Redis connection config
            redis_host = "8.8.8.8"
            redis_port = 80
            try:
                pool = getattr(self.redis, "connection_pool", None)
                if pool:
                    redis_host = pool.connection_kwargs.get("host", "8.8.8.8")
                    redis_port = pool.connection_kwargs.get("port", 6379)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            host = get_local_ip(target_host=redis_host, target_port=redis_port)

        instance_id = f"{service_name}:{uuid.uuid4().hex[:8]}"
        self._current_instance = ServiceInstance(
            id=instance_id,
            protocol=protocol or "http",
            host=host,
            port=port,
            path_prefix=path_prefix,
            weight=weight,
            metadata=metadata or {},
        )
        self._current_service_name = service_name

        now_ms = int(time.time() * 1000)

        # 1. Write instance details
        await self.redis.hset(
            RedisKeys.sd_instance_details(service_name),
            instance_id,
            self._current_instance.to_json(),
        )
        # 2. Make the instance immediately discoverable.
        await self.redis.zadd(
            RedisKeys.sd_active_instances(service_name), {instance_id: now_ms}
        )
        self._current_instance.last_heartbeat = now_ms
        # 3. Add service name to global index
        await self.redis.sadd(RedisKeys.SD_SERVICES, service_name)

        # 4. Start recurring heartbeats only when requested.
        if heartbeat_interval > RedisKeys.SD_NO_HEARTBEAT:
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(service_name, instance_id, heartbeat_interval)
            )

    async def register_only(
        self,
        service_name: str,
        host: Optional[str] = None,
        port: int = 0,
        weight: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
        protocol: str = "http",
        path_prefix: Optional[str] = None,
    ) -> None:
        """Register an instance without starting recurring heartbeats."""
        await self.register(
            service_name=service_name,
            host=host,
            port=port,
            weight=weight,
            metadata=metadata,
            heartbeat_interval=RedisKeys.SD_NO_HEARTBEAT,
            protocol=protocol,
            path_prefix=path_prefix,
        )

    async def unregister(self):
        """Deregister the current service instance and stop heartbeat."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._current_instance and self._current_service_name:
            pipe = self.redis.pipeline()
            pipe.hdel(
                RedisKeys.sd_instance_details(self._current_service_name),
                self._current_instance.id,
            )
            pipe.zrem(
                RedisKeys.sd_active_instances(self._current_service_name),
                self._current_instance.id,
            )
            await pipe.execute()

    async def _send_heartbeat(self):
        if self._current_instance and self._current_service_name:
            now = int(time.time() * 1000)
            self._current_instance.last_heartbeat = now
            await self.redis.zadd(
                RedisKeys.sd_active_instances(self._current_service_name),
                {self._current_instance.id: now},
            )

    # pylint: disable=unused-argument
    async def _heartbeat_loop(self, service_name: str, instance_id: str, interval: int):
        """Background heartbeat coroutine."""
        while True:
            try:
                await asyncio.sleep(interval)
                await self._send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception:  # pylint: disable=broad-exception-caught
                await asyncio.sleep(1)


class DiscoveryClient:
    """Efficient service discovery client with local cache.

    Used by consumers. Reduces Redis access frequency through in-memory cache
    and background refresh mechanisms.
    """

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        cache_interval: int = 5,
    ):
        self.redis = redis_client or get_redis()
        self.cache_interval = cache_interval
        self._cache: Dict[str, List[ServiceInstance]] = {}
        self._last_refresh: Dict[str, float] = {}
        self._watched_services: set[str] = set()
        self._refresh_task: Optional[asyncio.Task] = None
        self._rr_counters: Dict[str, int] = {}

    async def get_instances(
        self,
        service_name: str,
        force_refresh: bool = False,
        health_threshold_ms: int = RedisKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS,
    ) -> List[ServiceInstance]:
        """Get service instances. Prefers using cache."""
        now = time.time()

        # Determine if cache is valid
        is_stale = now - self._last_refresh.get(service_name, 0) > self.cache_interval

        if force_refresh or is_stale or service_name not in self._cache:
            await self._refresh_service(service_name, health_threshold_ms)

        instances = self._cache.get(service_name, [])
        if health_threshold_ms == RedisKeys.SD_NO_HEALTH_CHECK:
            return instances

        min_score = int(time.time() * 1000) - health_threshold_ms
        return [
            instance for instance in instances if instance.last_heartbeat >= min_score
        ]

    async def _refresh_service(self, service_name: str, health_threshold_ms: int):
        """Sync instance list from Redis and update cache."""
        del health_threshold_ms

        # 1. Get active IDs together with their latest heartbeat timestamp.
        raw_instances = await self.redis.zrange(
            RedisKeys.sd_active_instances(service_name), 0, -1, withscores=True
        )

        if not raw_instances:
            self._cache[service_name] = []
            self._last_refresh[service_name] = time.time()
            return

        instance_ids: list[str] = []
        heartbeat_by_id: dict[str, int] = {}
        for instance_id, score in raw_instances:
            normalized_id = (
                instance_id.decode("utf-8")
                if isinstance(instance_id, bytes)
                else instance_id
            )
            instance_ids.append(normalized_id)
            heartbeat_by_id[normalized_id] = int(score)

        # 2. Get details
        details_raw = await self.redis.hmget(
            RedisKeys.sd_instance_details(service_name), instance_ids
        )

        instances = []
        for raw in details_raw:
            if raw:
                data = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                instance = ServiceInstance.from_json(data)
                instance.last_heartbeat = heartbeat_by_id.get(instance.id, 0)
                instances.append(instance)

        self._cache[service_name] = instances
        self._last_refresh[service_name] = time.time()

    def watch(self, service_name: str):
        """Add service to background auto-refresh list."""
        self._watched_services.add(service_name)
        if not self._refresh_task:
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def _refresh_loop(self):
        """Background periodic cache refresh coroutine."""
        while True:
            try:
                await asyncio.sleep(self.cache_interval)
                for service_name in list(self._watched_services):
                    await self._refresh_service(
                        service_name, RedisKeys.SD_NO_HEALTH_CHECK
                    )
            except asyncio.CancelledError:
                break
            except Exception:  # pylint: disable=broad-exception-caught
                await asyncio.sleep(1)

    async def discover(
        self,
        service_name: str,
        strategy: str = "random",
        health_threshold_ms: int = RedisKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS,
    ) -> Optional[ServiceInstance]:
        """Perform load-balanced discovery."""
        instances = await self.get_instances(
            service_name, health_threshold_ms=health_threshold_ms
        )
        if not instances:
            return None

        if strategy == "random":
            return random.choice(instances)

        if strategy == "round-robin":
            counter = self._rr_counters.get(service_name, 0)
            instance = instances[counter % len(instances)]
            self._rr_counters[service_name] = counter + 1
            return instance

        return random.choice(instances)

    async def close(self):
        """Close the client, stop background tasks."""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
