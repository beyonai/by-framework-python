import pytest

from by_framework.core.runtime.file_manager import FileManager
from by_framework.core.runtime.filestore.local import LocalFileStorage


@pytest.mark.asyncio
async def test_file_manager_uses_local_storage_by_default(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))

    assert isinstance(manager.storage, LocalFileStorage)


@pytest.mark.asyncio
async def test_file_manager_glob_files_returns_matches(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()

    await manager.write_file("sessions/s1/docs/guide.md", "# hello\n")
    await manager.write_file("sessions/s1/docs/notes.txt", "todo\n")
    await manager.write_file("public/readme.md", "shared\n")

    result = await manager.glob_files("sessions/s1/**/*.md")

    assert result["success"] is True
    assert result["data"] == [
        {
            "path": "sessions/s1/docs/guide.md",
            "absolute_path": str(
                tmp_path / "default" / "sessions" / "s1" / "docs" / "guide.md"
            ),
        }
    ]


@pytest.mark.asyncio
async def test_file_manager_list_files_returns_cleaned_paths(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()

    await manager.write_file("sessions/s1/docs/guide.md", "# hello\n")
    await manager.write_file("sessions/s1/docs/notes.txt", "todo\n")

    result = await manager.list_files("sessions/s1/docs")

    assert result["success"] is True
    assert sorted(result["data"], key=lambda item: item["path"]) == [
        {
            "path": "guide.md",
            "absolute_path": str(
                tmp_path / "default" / "sessions" / "s1" / "docs" / "guide.md"
            ),
        },
        {
            "path": "notes.txt",
            "absolute_path": str(
                tmp_path / "default" / "sessions" / "s1" / "docs" / "notes.txt"
            ),
        },
    ]


@pytest.mark.asyncio
async def test_file_manager_delete_file_returns_structured_result(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()
    await manager.write_file("sessions/s1/docs/guide.md", "# hello\n")

    result = await manager.delete_file("sessions/s1/docs/guide.md")

    assert result["success"] is True
    assert result["data"] == {
        "path": "sessions/s1/docs/guide.md",
        "absolute_path": str(
            tmp_path / "default" / "sessions" / "s1" / "docs" / "guide.md"
        ),
        "deleted": True,
    }


@pytest.mark.asyncio
async def test_file_manager_write_and_edit_return_virtual_and_absolute_paths(
    tmp_path,
) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()

    write_result = await manager.write_file(
        "sessions/s1/docs/guide.md", "hello world\n"
    )
    edit_result = await manager.edit_file(
        "sessions/s1/docs/guide.md",
        "world",
        "codex",
    )

    expected_absolute_path = str(
        tmp_path / "default" / "sessions" / "s1" / "docs" / "guide.md"
    )
    assert write_result["data"] == {
        "path": "sessions/s1/docs/guide.md",
        "absolute_path": expected_absolute_path,
        "bytes_written": 12,
    }
    assert edit_result["data"] == {
        "path": "sessions/s1/docs/guide.md",
        "absolute_path": expected_absolute_path,
        "occurrences": 1,
    }


@pytest.mark.asyncio
async def test_file_manager_grep_files_supports_files_only_mode(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()

    await manager.write_file("sessions/s1/src/a.py", "TODO: one\n")
    await manager.write_file("sessions/s1/src/b.py", "TODO: two\nTODO: three\n")

    result = await manager.grep_files(
        "TODO",
        glob_pattern="sessions/s1/**/*.py",
        output_mode="files_with_matches",
    )

    assert result["success"] is True
    assert sorted(result["data"], key=lambda item: item["path"]) == [
        {
            "path": "sessions/s1/src/a.py",
            "absolute_path": str(
                tmp_path / "default" / "sessions" / "s1" / "src" / "a.py"
            ),
        },
        {
            "path": "sessions/s1/src/b.py",
            "absolute_path": str(
                tmp_path / "default" / "sessions" / "s1" / "src" / "b.py"
            ),
        },
    ]


@pytest.mark.asyncio
async def test_file_manager_grep_files_evicts_large_content_results(tmp_path) -> None:
    manager = FileManager(
        session_id="s1",
        workspace_dir=str(tmp_path),
        tool_result_max_chars=80,
    )
    await manager.initialize()

    await manager.write_file(
        "sessions/s1/src/large.py",
        "needle alpha\nneedle beta\nneedle gamma\nneedle delta\n",
    )

    result = await manager.grep_files(
        "needle",
        glob_pattern="sessions/s1/**/*.py",
        output_mode="content",
    )

    assert result["success"] is True
    assert result["data"]["evicted"] is True
    assert result["data"]["path"].startswith("sessions/s1/large_results/grep_")
    assert result["data"]["absolute_path"].endswith(".json")
    assert "read_file" in result["message"]

    stored = await manager.read_file(result["data"]["path"])
    assert stored["success"] is True
    assert "needle alpha" in stored["data"]["content"]


@pytest.mark.asyncio
async def test_file_manager_read_file_supports_offset_and_limit(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()
    await manager.write_file("sessions/s1/docs/guide.md", "line1\nline2\nline3\n")

    content = await manager.read_file("sessions/s1/docs/guide.md", offset=1, limit=2)

    assert content["success"] is True
    assert content["data"] == {
        "path": "sessions/s1/docs/guide.md",
        "absolute_path": str(
            tmp_path / "default" / "sessions" / "s1" / "docs" / "guide.md"
        ),
        "content": "line2\nline3",
    }


@pytest.mark.asyncio
async def test_file_manager_read_file_returns_image_payload_for_png(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()

    png_bytes = (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR"
        b"\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00"
        b"\x90wS\xde"
        b"\x00\x00\x00\x0cIDATx\x9cc``\x00\x00\x00\x04\x00\x01"
        b"\x0b\xe7\x02\x9d"
        b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    write_result = await manager.storage.write(
        "default/sessions/s1/images/pixel.png",
        png_bytes,
        encoding="utf-8",
    )
    assert write_result["bytes_written"] == len(png_bytes)

    result = await manager.read_file("sessions/s1/images/pixel.png")

    assert result["success"] is True
    assert result["data"]["path"] == "sessions/s1/images/pixel.png"
    assert result["data"]["absolute_path"].endswith(
        "/default/sessions/s1/images/pixel.png"
    )
    assert result["data"]["type"] == "image/png"
    assert result["data"]["base64"].startswith("iVBORw0KGgo")
