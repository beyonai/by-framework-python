"""
Tests for by_framework.common.constants module.
"""

import os
import unittest

from by_framework.common.constants import get_key_schema_version


class TestGetKeySchemaVersion(unittest.TestCase):
    """Tests for get_key_schema_version."""

    def setUp(self):
        self._old_value = os.environ.get("REDIS_KEY_SCHEMA_VERSION")

    def tearDown(self):
        if self._old_value is not None:
            os.environ["REDIS_KEY_SCHEMA_VERSION"] = self._old_value
        else:
            os.environ.pop("REDIS_KEY_SCHEMA_VERSION", None)

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


if __name__ == "__main__":
    unittest.main()
