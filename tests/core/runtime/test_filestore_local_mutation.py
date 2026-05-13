import pytest

from by_framework.core.runtime.filestore.local import LocalFileStorage


@pytest.mark.asyncio
async def test_local_filestore_write_returns_structured_result(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()

    result = await storage.write("user/sessions/s1/docs/guide.md", "# hello\n")

    assert result["path"] == "user/sessions/s1/docs/guide.md"
    assert result["absolute_path"].endswith("/user/sessions/s1/docs/guide.md")
    assert result["bytes_written"] == 8


@pytest.mark.asyncio
async def test_local_filestore_edit_returns_structured_result(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
    await storage.write("user/sessions/s1/docs/guide.md", "hello world\n")

    result = await storage.edit(
        "user/sessions/s1/docs/guide.md",
        "world",
        "codex",
    )

    assert result["path"] == "user/sessions/s1/docs/guide.md"
    assert result["absolute_path"].endswith("/user/sessions/s1/docs/guide.md")
    assert result["occurrences"] == 1
