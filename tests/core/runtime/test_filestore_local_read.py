import pytest

from by_framework.core.runtime.filestore.local import LocalFileStorage


@pytest.mark.asyncio
async def test_local_filestore_read_returns_structured_text_result(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
    write_result = await storage.write(
        "user/sessions/s1/docs/guide.md", "line1\nline2\n"
    )
    assert write_result["bytes_written"] == 12

    result = await storage.read(
        "user/sessions/s1/docs/guide.md",
        offset=1,
        limit=1,
    )

    assert result["kind"] == "text"
    assert result["path"] == "user/sessions/s1/docs/guide.md"
    assert result["absolute_path"].endswith("/user/sessions/s1/docs/guide.md")
    assert result["content"] == "line2"


@pytest.mark.asyncio
async def test_local_filestore_read_returns_structured_image_result(tmp_path) -> None:
    storage = LocalFileStorage(base_dir=str(tmp_path))
    await storage.initialize()
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
    write_result = await storage.write("user/sessions/s1/images/pixel.png", png_bytes)
    assert write_result["bytes_written"] == len(png_bytes)

    result = await storage.read("user/sessions/s1/images/pixel.png", encoding="")

    assert result["kind"] == "image"
    assert result["path"] == "user/sessions/s1/images/pixel.png"
    assert result["absolute_path"].endswith("/user/sessions/s1/images/pixel.png")
    assert result["media_type"] == "image/png"
    assert result["content"].startswith(b"\x89PNG")
