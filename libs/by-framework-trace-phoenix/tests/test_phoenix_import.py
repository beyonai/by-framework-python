"""Placeholder test to satisfy pytest and verify package imports."""

from by_framework_trace_phoenix import PhoenixTraceProviderFactory


def test_import_factory():
    """Verify that the factory can be imported."""
    factory = PhoenixTraceProviderFactory()
    assert factory.provider_name == "phoenix"
