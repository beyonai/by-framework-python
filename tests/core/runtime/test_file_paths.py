from by_framework.core.runtime.file_paths import (FileAccessContext,
                                                  RuntimePathMapper)


def test_runtime_path_mapper_normalizes_virtual_paths() -> None:
    mapper = RuntimePathMapper(FileAccessContext(session_id="s1", user_code="user-a"))

    assert (
        mapper.normalize_virtual_path("./sessions/s1/docs/guide.md")
        == "sessions/s1/docs/guide.md"
    )
    assert mapper.normalize_virtual_path("") == ""


def test_runtime_path_mapper_builds_storage_and_large_result_paths() -> None:
    mapper = RuntimePathMapper(FileAccessContext(session_id="s1", user_code="user-a"))

    assert (
        mapper.to_storage_path("sessions/s1/docs/guide.md")
        == "user-a/sessions/s1/docs/guide.md"
    )
    assert (
        mapper.from_storage_path("user-a/sessions/s1/docs/guide.md")
        == "sessions/s1/docs/guide.md"
    )
    assert (
        mapper.build_large_result_path("grep", "abc12345")
        == "sessions/s1/large_results/grep_abc12345.json"
    )
