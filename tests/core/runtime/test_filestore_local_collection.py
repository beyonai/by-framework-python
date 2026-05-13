import pytest

from by_framework.core.runtime.filestore.local import LocalFileStorage


@pytest.mark.asyncio
async def test_local_filestore_list_returns_structured_result(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
    await storage.write("user/sessions/s1/docs/guide.md", "# hello\n")
    await storage.write("user/sessions/s1/docs/notes.txt", "todo\n")

    result = await storage.list("user/sessions/s1/docs")

    assert sorted(result["paths"], key=lambda item: item["path"]) == [
        {
            "path": "guide.md",
            "absolute_path": str(
                tmp_path / "user" / "sessions" / "s1" / "docs" / "guide.md"
            ),
        },
        {
            "path": "notes.txt",
            "absolute_path": str(
                tmp_path / "user" / "sessions" / "s1" / "docs" / "notes.txt"
            ),
        },
    ]


@pytest.mark.asyncio
async def test_local_filestore_delete_returns_structured_result(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
    await storage.write("user/sessions/s1/docs/guide.md", "# hello\n")

    result = await storage.delete("user/sessions/s1/docs/guide.md")

    assert result["path"] == "user/sessions/s1/docs/guide.md"
    assert result["absolute_path"].endswith("/user/sessions/s1/docs/guide.md")
    assert result["deleted"] is True
