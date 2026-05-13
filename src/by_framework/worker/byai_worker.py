"""Byai worker base class with automatic content decoding."""

from by_framework.core.protocol.byai_codec import ByaiContentCodec
from by_framework.core.protocol.byai_command import (
    ByaiAskAgentCommand,
    ByaiResumeCommand,
)
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    GatewayCommand,
    ResumeCommand,
)
from by_framework.core.protocol.content_codec import ContentCodec

from .byai_context import ByaiAgentContext
from .worker import GatewayWorker


class ByaiWorker(GatewayWorker):
    """GatewayWorker variant that decodes Byai message payloads for business logic."""

    def get_context_class(self) -> type[ByaiAgentContext]:
        return ByaiAgentContext

    def get_content_codec(self) -> ContentCodec | None:
        return ByaiContentCodec()

    def prepare_command_for_processing(self, command: GatewayCommand) -> GatewayCommand:
        if not hasattr(command, "content"):
            return command

        codec = self.get_content_codec()
        if codec is None:
            return command

        decoded_content = codec.deserialize(command.content)
        if isinstance(command, AskAgentCommand):
            return ByaiAskAgentCommand(
                header=command.header,
                content=decoded_content,
                wait_for_reply=command.wait_for_reply,
                extra_payload=dict(command.extra_payload),
            )
        if isinstance(command, ResumeCommand):
            return ByaiResumeCommand(
                header=command.header,
                content=decoded_content,
                status=command.status,
                reply_data=command.reply_data,
                extra_payload=dict(command.extra_payload),
            )
        return command
