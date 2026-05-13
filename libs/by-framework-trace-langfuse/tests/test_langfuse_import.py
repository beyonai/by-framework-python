"""Placeholder test to satisfy pytest and verify package imports."""

from by_framework_trace_langfuse import LangfuseTraceProviderFactory


def test_import_factory():
    """Verify that the factory can be imported."""
    factory = LangfuseTraceProviderFactory()
    assert factory.provider_name == "langfuse"
