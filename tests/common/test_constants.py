"""
Tests for by_framework.common.constants module.
"""

import fnmatch
import os
import unittest

from redis.crc import key_slot

from by_framework.common.constants import RedisKeys, get_key_schema_version


class _KeySchemaVersionTestCase(unittest.TestCase):
    """Base class that saves/restores REDIS_KEY_SCHEMA_VERSION around each test."""

    def setUp(self):
        self._old_value = os.environ.get("REDIS_KEY_SCHEMA_VERSION")

    def tearDown(self):
        if self._old_value is not None:
            os.environ["REDIS_KEY_SCHEMA_VERSION"] = self._old_value
        else:
            os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)

    def set_schema_version(self, version: str) -> None:
        os.environ["REDIS_KEY_SCHEMA_VERSION"] = version


class TestRedisKeysWorkerScanPatterns(_KeySchemaVersionTestCase):
    """Tests for the SCAN-based worker discovery helpers under v1/v2.

    Redis's SCAN/KEYS MATCH glob only treats `*`, `?`, `[seq]` as special —
    `{`/`}` are literal characters, same as Python's fnmatch semantics used
    here as a faithful proxy for "would Redis actually match this pattern"
    without needing a real server.
    """

    def test_online_lease_scan_pattern_matches_a_real_v1_key(self):
        self.set_schema_version("v1")
        real_key = RedisKeys.worker_online_lease("worker-online")
        pattern = RedisKeys.worker_online_lease_scan_pattern()
        self.assertTrue(fnmatch.fnmatchcase(real_key, pattern))
        self.assertEqual(
            RedisKeys.worker_id_from_online_lease_key(real_key), "worker-online"
        )

    def test_online_lease_scan_pattern_matches_a_real_v2_key(self):
        self.set_schema_version("v2")
        real_key = RedisKeys.worker_online_lease("worker-online")
        pattern = RedisKeys.worker_online_lease_scan_pattern()
        self.assertTrue(fnmatch.fnmatchcase(real_key, pattern))
        self.assertEqual(
            RedisKeys.worker_id_from_online_lease_key(real_key), "worker-online"
        )

    def test_admin_scan_pattern_matches_a_real_v1_key(self):
        self.set_schema_version("v1")
        real_key = RedisKeys.worker_admin("worker-scanned")
        pattern = RedisKeys.worker_admin_scan_pattern()
        self.assertTrue(fnmatch.fnmatchcase(real_key, pattern))
        self.assertEqual(RedisKeys.worker_id_from_admin_key(real_key), "worker-scanned")

    def test_admin_scan_pattern_matches_a_real_v2_key(self):
        self.set_schema_version("v2")
        real_key = RedisKeys.worker_admin("worker-scanned")
        pattern = RedisKeys.worker_admin_scan_pattern()
        self.assertTrue(fnmatch.fnmatchcase(real_key, pattern))
        self.assertEqual(RedisKeys.worker_id_from_admin_key(real_key), "worker-scanned")


class TestRedisKeysSameEntityWorkerGroupVersioning(_KeySchemaVersionTestCase):
    """Tests for a same-entity (worker group) factory method under v1/v2."""

    def test_worker_admin_v1_unchanged(self):
        """v1 output is byte-for-byte identical to the pre-versioning format."""
        self.set_schema_version("v1")
        self.assertEqual(
            RedisKeys.worker_admin("worker-01"),
            "byai_gateway:registry:worker:admin:worker-01",
        )

    def test_worker_admin_v2_wraps_worker_id_in_hash_tag(self):
        """v2 mode wraps the worker_id in Cluster hash-tag braces."""
        self.set_schema_version("v2")
        self.assertEqual(
            RedisKeys.worker_admin("worker-01"),
            "byai_gateway:v2:registry:worker:{worker-01}:admin",
        )


class TestRedisKeysAgentTypeGroupVersioning(_KeySchemaVersionTestCase):
    """Tests for the mandatory agent_type hash-tag group under v2."""

    def test_agent_type_members_and_denied_share_the_same_cluster_slot(self):
        """agent_type_members/agent_type_denied must be co-located: the
        deny-worker-for-type path writes both together and must keep working
        atomically, so this shared tag is mandatory, not optional."""
        self.set_schema_version("v2")
        members_key = RedisKeys.agent_type_members("chat")
        denied_key = RedisKeys.agent_type_denied("chat")
        self.assertEqual(key_slot(members_key.encode()), key_slot(denied_key.encode()))


class TestRedisKeysTraceNamespaceVersioning(_KeySchemaVersionTestCase):
    """Tests for the trace group's namespace unification under v2."""

    def test_trace_meta_v1_stays_on_historical_by_framework_namespace(self):
        """v1 keeps Python's historical by_framework:trace:* namespace,
        unchanged (this is the pre-existing format, not touched by v2)."""
        self.set_schema_version("v1")
        self.assertEqual(
            RedisKeys.trace_meta("trace-xyz"), "by_framework:trace:trace-xyz"
        )

    def test_trace_meta_v2_moves_to_unified_byai_gateway_namespace(self):
        """v2 unifies onto the shared byai_gateway:v2:trace:{id} format used
        by all three language SDKs, replacing the historical namespace."""
        self.set_schema_version("v2")
        self.assertEqual(
            RedisKeys.trace_meta("trace-xyz"), "byai_gateway:v2:trace:{trace-xyz}"
        )

    def test_trace_spans_v2_shares_trace_meta_slot(self):
        """trace_meta/trace_spans are written together in one pipeline, so
        they must share the same Cluster slot under v2."""
        self.set_schema_version("v2")
        self.assertEqual(
            RedisKeys.trace_spans("trace-xyz"),
            "byai_gateway:v2:trace:spans:{trace-xyz}",
        )
        self.assertEqual(
            key_slot(RedisKeys.trace_meta("trace-xyz").encode()),
            key_slot(RedisKeys.trace_spans("trace-xyz").encode()),
        )

    def test_trace_index_session_v2_gets_new_prefix_without_tag(self):
        """trace_index_* is cross-entity (written alongside the trace group
        today, split apart in a later issue) so it must NOT share the trace
        group's tag — only the prefix moves from by_framework: to v2."""
        self.set_schema_version("v2")
        self.assertEqual(
            RedisKeys.trace_index_session("sess-abc123"),
            "byai_gateway:v2:trace:idx:session:sess-abc123",
        )


class TestRedisKeysGlobalIndexVersioning(_KeySchemaVersionTestCase):
    """Tests for global-index keys (untagged, but still version-prefixed)."""

    def test_known_workers_v1_unchanged(self):
        """v1 output matches the pre-versioning KNOWN_WORKERS literal."""
        self.set_schema_version("v1")
        self.assertEqual(RedisKeys.known_workers(), "byai_gateway:registry:workers")

    def test_known_workers_v2_gets_prefix_no_tag(self):
        """v2 still gets the version prefix even though it's a global index
        with no single owning entity to tag."""
        self.set_schema_version("v2")
        self.assertEqual(RedisKeys.known_workers(), "byai_gateway:v2:registry:workers")

    def test_admin_workers_v1_unchanged(self):
        """v1 output matches the pre-versioning ADMIN_WORKERS literal."""
        self.set_schema_version("v1")
        self.assertEqual(
            RedisKeys.admin_workers(), "byai_gateway:registry:worker:admin_workers"
        )

    def test_admin_workers_v2_gets_prefix_no_tag(self):
        """v2 still gets the version prefix even though it's a global index."""
        self.set_schema_version("v2")
        self.assertEqual(
            RedisKeys.admin_workers(), "byai_gateway:v2:registry:worker:admin_workers"
        )

    def test_sd_services_v1_unchanged(self):
        """v1 output matches the pre-versioning SD_SERVICES literal."""
        self.set_schema_version("v1")
        self.assertEqual(RedisKeys.sd_services(), "byai_gateway:sd:services")

    def test_sd_services_v2_gets_prefix_no_tag(self):
        """v2 still gets the version prefix even though it's a global index."""
        self.set_schema_version("v2")
        self.assertEqual(RedisKeys.sd_services(), "byai_gateway:v2:sd:services")


class TestRedisKeysSingleKeyVersioning(_KeySchemaVersionTestCase):
    """Tests for a single-key (no hash tag) factory method under v1/v2."""

    def test_plugin_reload_ack_stream_v1_unchanged(self):
        """v1 output is byte-for-byte identical to the pre-versioning format."""
        self.set_schema_version("v1")
        self.assertEqual(
            RedisKeys.plugin_reload_ack_stream("reload-1"),
            "byai_gateway:plugin_reload:reload-1:ack",
        )

    def test_plugin_reload_ack_stream_v2_gets_v2_prefix_no_tag(self):
        """v2 mode prefixes single-key methods without adding a hash tag."""
        self.set_schema_version("v2")
        self.assertEqual(
            RedisKeys.plugin_reload_ack_stream("reload-1"),
            "byai_gateway:v2:plugin_reload:reload-1:ack",
        )


class TestRedisKeysGoldenV2Keys(_KeySchemaVersionTestCase):
    """Golden-key test: fixed IDs -> exact v2 key strings for every factory
    method (issue #43 acceptance criteria #6). Expected values are
    transcribed from the design doc's key matrix, independent of the
    implementation, so this test can actually disagree with the code."""

    IDS = {
        "session_id": "sess-abc123",
        "worker_id": "worker-01",
        "trace_id": "trace-xyz",
        "agent_type": "chat",
        "group_id": "tg-1",
        "service_name": "svc-a",
        "reload_id": "reload-1",
        "execution_id": "exec-1",
        "user_code": "user-1",
        "region": "us-east",
        "snapshot_key": "snap-1",
        "consumer_name": "consumer-1",
    }

    def setUp(self):
        super().setUp()
        self.set_schema_version("v2")

    def test_golden_v2_keys(self):
        ids = self.IDS
        cases = {
            "ctrl_stream": (
                RedisKeys.ctrl_stream(ids["agent_type"]),
                "byai_gateway:v2:ctrl:agent_type:chat",
            ),
            "worker_ctrl_stream": (
                RedisKeys.worker_ctrl_stream(ids["worker_id"]),
                "byai_gateway:v2:ctrl:worker:{worker-01}",
            ),
            "plugin_reload_ack_stream": (
                RedisKeys.plugin_reload_ack_stream(ids["reload_id"]),
                "byai_gateway:v2:plugin_reload:reload-1:ack",
            ),
            "control_plane_wakeup_stream": (
                RedisKeys.control_plane_wakeup_stream(),
                "byai_gateway:v2:control_plane:mgmt:wakeup",
            ),
            "control_plane_wakeup_result_stream": (
                RedisKeys.control_plane_wakeup_result_stream(ids["execution_id"]),
                "byai_gateway:v2:control_plane:mgmt:wakeup:result:exec-1",
            ),
            "control_plane_delivery_pending_stream": (
                RedisKeys.control_plane_delivery_pending_stream(),
                "byai_gateway:v2:control_plane:mgmt:delivery:pending",
            ),
            "control_plane_deadletter_stream": (
                RedisKeys.control_plane_deadletter_stream(),
                "byai_gateway:v2:control_plane:mgmt:deadletter",
            ),
            "control_plane_agent_availability": (
                RedisKeys.control_plane_agent_availability(ids["agent_type"]),
                "byai_gateway:v2:control_plane:availability:agent_type:chat",
            ),
            "control_plane_agent_circuit": (
                RedisKeys.control_plane_agent_circuit(ids["agent_type"]),
                "byai_gateway:v2:control_plane:circuit:agent_type:chat",
            ),
            "control_plane_agent_fallback": (
                RedisKeys.control_plane_agent_fallback(ids["agent_type"]),
                "byai_gateway:v2:control_plane:fallback:agent_type:chat",
            ),
            "control_plane_user_quota": (
                RedisKeys.control_plane_user_quota(ids["user_code"]),
                "byai_gateway:v2:control_plane:quota:user:user-1",
            ),
            "control_plane_tenant_quota": (
                RedisKeys.control_plane_tenant_quota(ids["user_code"]),
                "byai_gateway:v2:control_plane:quota:user:user-1",
            ),
            "control_plane_wakeup_dedupe": (
                RedisKeys.control_plane_wakeup_dedupe(
                    ids["agent_type"], ids["user_code"], ids["region"]
                ),
                "byai_gateway:v2:control_plane:wakeup:dedupe:chat:user-1:us-east",
            ),
            "agent_configs_snapshot": (
                RedisKeys.agent_configs_snapshot(ids["snapshot_key"]),
                "byai_gateway:v2:agent_configs_snapshot:snap-1",
            ),
            "session_data_stream": (
                RedisKeys.session_data_stream(ids["session_id"]),
                "byai_gateway:v2:session:{sess-abc123}:data_stream",
            ),
            "session_data_checkpoint": (
                RedisKeys.session_data_checkpoint(
                    ids["session_id"], ids["consumer_name"]
                ),
                "byai_gateway:v2:session:{sess-abc123}:consumer:consumer-1:checkpoint",
            ),
            "trace_meta": (
                RedisKeys.trace_meta(ids["trace_id"]),
                "byai_gateway:v2:trace:{trace-xyz}",
            ),
            "trace_spans": (
                RedisKeys.trace_spans(ids["trace_id"]),
                "byai_gateway:v2:trace:spans:{trace-xyz}",
            ),
            "trace_index_session": (
                RedisKeys.trace_index_session(ids["session_id"]),
                "byai_gateway:v2:trace:idx:session:sess-abc123",
            ),
            "trace_index_worker": (
                RedisKeys.trace_index_worker(ids["worker_id"]),
                "byai_gateway:v2:trace:idx:worker:worker-01",
            ),
            "trace_index_agent": (
                RedisKeys.trace_index_agent(ids["agent_type"]),
                "byai_gateway:v2:trace:idx:agent:chat",
            ),
            "task_group": (
                RedisKeys.task_group(ids["group_id"]),
                "byai_gateway:v2:task_group:{tg-1}",
            ),
            "task_group_results": (
                RedisKeys.task_group_results(ids["group_id"]),
                "byai_gateway:v2:task_group:{tg-1}:results",
            ),
            "known_workers": (
                RedisKeys.known_workers(),
                "byai_gateway:v2:registry:workers",
            ),
            "admin_workers": (
                RedisKeys.admin_workers(),
                "byai_gateway:v2:registry:worker:admin_workers",
            ),
            "worker_declared_agent_types": (
                RedisKeys.worker_declared_agent_types(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:agent_types",
            ),
            "agent_type_members": (
                RedisKeys.agent_type_members(ids["agent_type"]),
                "byai_gateway:v2:registry:agent_type:{chat}:workers",
            ),
            "worker_lock": (
                RedisKeys.worker_lock(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:lock",
            ),
            "worker_online_lease": (
                RedisKeys.worker_online_lease(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:online",
            ),
            "worker_status": (
                RedisKeys.worker_status(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:status",
            ),
            "worker_executions": (
                RedisKeys.worker_executions(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:executions",
            ),
            "worker_active_executions": (
                RedisKeys.worker_active_executions(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:active_executions",
            ),
            "worker_active_execution_index": (
                RedisKeys.worker_active_execution_index(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:active_execution_index",
            ),
            "worker_active_snapshots": (
                RedisKeys.worker_active_snapshots(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:active_snapshots",
            ),
            "worker_history_snapshots": (
                RedisKeys.worker_history_snapshots(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:history_snapshots",
            ),
            "worker_admin": (
                RedisKeys.worker_admin(ids["worker_id"]),
                "byai_gateway:v2:registry:worker:{worker-01}:admin",
            ),
            "agent_type_denied": (
                RedisKeys.agent_type_denied(ids["agent_type"]),
                "byai_gateway:v2:registry:agent_type:{chat}:denied",
            ),
            "session_registry": (
                RedisKeys.session_registry(ids["session_id"]),
                "byai_gateway:v2:session:{sess-abc123}:registry",
            ),
            "sd_active_instances": (
                RedisKeys.sd_active_instances(ids["service_name"]),
                "byai_gateway:v2:sd:{svc-a}:active",
            ),
            "sd_instance_details": (
                RedisKeys.sd_instance_details(ids["service_name"]),
                "byai_gateway:v2:sd:{svc-a}:instances",
            ),
            "sd_services": (
                RedisKeys.sd_services(),
                "byai_gateway:v2:sd:services",
            ),
        }
        for label, (actual, expected) in cases.items():
            with self.subTest(label=label):
                self.assertEqual(actual, expected)


class TestRedisKeysSameEntitySlotVerification(_KeySchemaVersionTestCase):
    """Slot-verification test (issue #43 acceptance criteria #5): every
    same-entity group's member keys must hash to the same Cluster slot
    under v2, verified via redis-py's actual CLUSTER-KEYSLOT implementation
    (the same one a real RedisCluster client uses), not a reimplementation."""

    def setUp(self):
        super().setUp()
        self.set_schema_version("v2")

    def assert_same_slot(self, *keys: str) -> None:
        slots = {key_slot(k.encode()) for k in keys}
        self.assertEqual(len(slots), 1, f"keys landed on different slots: {keys}")

    def test_session_group_shares_a_slot(self):
        session_id = "sess-abc123"
        self.assert_same_slot(
            RedisKeys.session_data_stream(session_id),
            RedisKeys.session_data_checkpoint(session_id, "consumer-1"),
            RedisKeys.session_registry(session_id),
        )

    def test_worker_group_shares_a_slot(self):
        worker_id = "worker-01"
        self.assert_same_slot(
            RedisKeys.worker_ctrl_stream(worker_id),
            RedisKeys.worker_declared_agent_types(worker_id),
            RedisKeys.worker_lock(worker_id),
            RedisKeys.worker_online_lease(worker_id),
            RedisKeys.worker_status(worker_id),
            RedisKeys.worker_executions(worker_id),
            RedisKeys.worker_active_executions(worker_id),
            RedisKeys.worker_active_execution_index(worker_id),
            RedisKeys.worker_active_snapshots(worker_id),
            RedisKeys.worker_history_snapshots(worker_id),
            RedisKeys.worker_admin(worker_id),
        )

    def test_task_group_shares_a_slot(self):
        group_id = "tg-1"
        self.assert_same_slot(
            RedisKeys.task_group(group_id),
            RedisKeys.task_group_results(group_id),
        )

    def test_agent_type_group_shares_a_slot(self):
        agent_type = "chat"
        self.assert_same_slot(
            RedisKeys.agent_type_members(agent_type),
            RedisKeys.agent_type_denied(agent_type),
        )

    def test_service_discovery_group_shares_a_slot(self):
        service_name = "svc-a"
        self.assert_same_slot(
            RedisKeys.sd_active_instances(service_name),
            RedisKeys.sd_instance_details(service_name),
        )

    def test_trace_group_shares_a_slot(self):
        trace_id = "trace-xyz"
        self.assert_same_slot(
            RedisKeys.trace_meta(trace_id),
            RedisKeys.trace_spans(trace_id),
        )


class TestGetKeySchemaVersion(unittest.TestCase):
    """Tests for get_key_schema_version."""

    def setUp(self):
        self._old_value = os.environ.get("REDIS_KEY_SCHEMA_VERSION")
        self._old_cluster_host = os.environ.get("REDIS_CLUSTER_HOST")
        os.environ.pop("REDIS_CLUSTER_HOST", None)

    def tearDown(self):
        if self._old_value is not None:
            os.environ["REDIS_KEY_SCHEMA_VERSION"] = self._old_value
        else:
            os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)
        if self._old_cluster_host is not None:
            os.environ["REDIS_CLUSTER_HOST"] = self._old_cluster_host
        else:
            os.environ.pop("REDIS_CLUSTER_HOST", None)

    def test_defaults_to_v1(self):
        """Test that the schema version defaults to v1 when unset."""
        os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)
        self.assertEqual(get_key_schema_version(), "v1")

    def test_explicit_v2(self):
        """Test that an explicit v2 value is returned as-is."""
        os.environ["REDIS_KEY_SCHEMA_VERSION"] = "v2"
        self.assertEqual(get_key_schema_version(), "v2")

    def test_invalid_value_raises(self):
        """Test that an invalid schema version raises ValueError."""
        os.environ["REDIS_KEY_SCHEMA_VERSION"] = "v3"
        with self.assertRaises(ValueError):
            get_key_schema_version()

    def test_defaults_to_v2_when_cluster_host_set(self):
        """REDIS_CLUSTER_HOST alone (no explicit schema version) implies v2."""
        os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)
        os.environ["REDIS_CLUSTER_HOST"] = "h1:6379,h2:6380"
        self.assertEqual(get_key_schema_version(), "v2")

    def test_explicit_value_wins_over_cluster_host(self):
        """An explicit REDIS_KEY_SCHEMA_VERSION always wins, even if it
        contradicts REDIS_CLUSTER_HOST's implied v2 - redis_client.init_redis's
        fail-fast check is what catches this combination, not silent
        auto-correction here."""
        os.environ["REDIS_CLUSTER_HOST"] = "h1:6379"
        os.environ["REDIS_KEY_SCHEMA_VERSION"] = "v1"
        self.assertEqual(get_key_schema_version(), "v1")

    def test_empty_cluster_host_is_treated_as_unset(self):
        """An empty REDIS_CLUSTER_HOST (var present but blank) must not imply v2."""
        os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)
        os.environ["REDIS_CLUSTER_HOST"] = ""
        self.assertEqual(get_key_schema_version(), "v1")


if __name__ == "__main__":
    unittest.main()
