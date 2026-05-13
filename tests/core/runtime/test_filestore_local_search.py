"""Tests for local file storage search helpers."""

import pytest

from by_framework.core.runtime.filestore.local import LocalFileStorage


@pytest.mark.asyncio
async def test_local_filestore_grep_returns_structured_matches(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
    assert (await storage.write("user/sessions/s1/src/a.py", "TODO: one\n"))[
        "bytes_written"
    ] == 10
    assert (await storage.write("user/sessions/s1/src/b.py", "TODO: two\n"))[
        "bytes_written"
    ] == 10

    result = await storage.grep("TODO", "user/sessions/s1/**/*.py")

    assert result["matches"] == [
        {
            "path": "user/sessions/s1/src/a.py",
            "absolute_path": str(
                tmp_path / "user" / "sessions" / "s1" / "src" / "a.py"
            ),
            "line_number": 1,
            "content": "TODO: one",
        },
        {
            "path": "user/sessions/s1/src/b.py",
            "absolute_path": str(
                tmp_path / "user" / "sessions" / "s1" / "src" / "b.py"
            ),
            "line_number": 1,
            "content": "TODO: two",
        },
    ]


@pytest.mark.asyncio
async def test_local_filestore_glob_returns_structured_paths(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
    assert (await storage.write("user/sessions/s1/docs/guide.md", "# hello\n"))[
        "bytes_written"
    ] == 8
    assert (await storage.write("user/sessions/s1/docs/notes.txt", "todo\n"))[
        "bytes_written"
    ] == 5

    result = await storage.glob("user/sessions/s1/**/*.md")

    assert result["paths"] == [
        {
            "path": "user/sessions/s1/docs/guide.md",
            "absolute_path": str(
                tmp_path / "user" / "sessions" / "s1" / "docs" / "guide.md"
            ),
        }
    ]
