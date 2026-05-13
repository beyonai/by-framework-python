import pytest

from by_framework.core.protocol.byai_codec import (
    deserialize_byai_content,
    serialize_byai_content,
)
from by_framework.core.protocol.message import (
    BaiYingMessage,
    BaiYingMessageRole,
    MessageContent,
    MessageFile,
    Resource,
)


def test_serialize_single_baiying_message_to_wire_format():
    message = BaiYingMessage(
        role=BaiYingMessageRole.USER,
        content=MessageContent(
            text="hello",
            files=[
                MessageFile(
                    fileId=1,
                    fileUrl="https://example.com/a.txt",
                    fileType="txt",
                    fileName="a.txt",
                )
            ],
            resources=[
                Resource(
                    resourceId="r1",
                    resourceName="doc",
                    resourceType="file",
                )
            ],
        ),
    )

    payload = serialize_byai_content(message)

    assert payload == [
        {
            "role": "user",
            "content": {
                "text": "hello",
                "files": [
                    {
                        "fileId": 1,
                        "fileUrl": "https://example.com/a.txt",
                        "fileType": "txt",
                        "fileName": "a.txt",
                    }
                ],
                "resources": [
                    {
                        "resourceId": "r1",
                        "resourceName": "doc",
                        "resourceType": "file",
                        "id": "",
                        "path": "",
                        "resourceDesc": "",
                        "resourceMetaData": {},
                    }
                ],
            },
        }
    ]


def test_deserialize_single_wire_message_to_baiying_message():
    payload = [{"role": "assistant", "content": "hi"}]

    result = deserialize_byai_content(payload)

    assert isinstance(result, BaiYingMessage)
    assert result.role == BaiYingMessageRole.ASSISTANT
    assert result.content == "hi"


def test_deserialize_multiple_wire_messages_to_list_of_baiying_message():
    payload = [
        {"role": "user", "content": "hello"},
        {
            "role": "assistant",
            "content": {
                "text": "world",
                "files": [],
                "resources": [],
            },
        },
    ]

    result = deserialize_byai_content(payload)

    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(item, BaiYingMessage) for item in result)
    assert result[0].role == BaiYingMessageRole.USER
    assert result[1].content == MessageContent(text="world", files=[], resources=[])


@pytest.mark.parametrize("content", ["hello", [{"foo": "bar"}], []])
def test_non_byai_payloads_are_preserved(content):
    assert serialize_byai_content(content) == content
    assert deserialize_byai_content(content) == content
