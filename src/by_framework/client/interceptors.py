"""
Message interceptor definitions for Gateway client.

Provides interceptors that can modify message parameters before
they are sent to the Gateway via Redis streams.
"""

from enum import Enum
from typing import Any, Dict, Protocol


class MessageInterceptor(Protocol):
    """
    Protocol for message interceptors that process params before sending.

    This defines the structural interface - any object with a before_send
    method that takes a dict and returns a dict can be used as an interceptor.
    """

    def before_send(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executed before the message is packaged and sent.

        Args:
            params: A dictionary containing 'target_agent_type', 'session_id',
                   'content', 'payload', etc.

        Returns:
            Modified params dictionary.
        """
        ...


class GatewayInterceptor:
    """
    Base class for SDK interceptors.
    Interceptors can modify request arguments before the message is sent to Redis.
    """

    def before_send(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executed before the message is packaged and sent.
        :param params: A dictionary containing 'target_agent_type', 'session_id', 'content', 'payload', etc.
        :return: Modified params dictionary.
        """
        return params


class ByaiMessageInterceptor(GatewayInterceptor):
    """
    Interceptor that handles conversion of domain-specific BaiYingMessage objects
    into protocol-compatible formats (str or List[Dict]).
    """

    def before_send(self, params: Dict[str, Any]) -> Dict[str, Any]:
        content = params.get("content")
        if content is None:
            return params

        params["content"] = self._format_content(content)
        return params

    def _format_content(self, content: Any) -> Any:
        if isinstance(content, str):
            return content

        # Consistent list-based processing
        input_list = content if isinstance(content, list) else [content]
        formatted_content = []

        for m in input_list:
            if isinstance(m, dict):
                formatted_content.append(m)
            elif hasattr(m, "role") and hasattr(m, "content"):
                role_val = m.role.value if isinstance(m.role, Enum) else m.role

                # Handling specialized MessageContent objects
                if hasattr(m.content, "__dict__"):
                    c = m.content
                    formatted_content.append(
                        {
                            "role": role_val,
                            "content": {
                                "text": getattr(c, "text", ""),
                                "files": [
                                    f.__dict__ if hasattr(f, "__dict__") else f
                                    for f in getattr(c, "files", [])
                                ],
                                "resources": [
                                    r.__dict__ if hasattr(r, "__dict__") else r
                                    for r in getattr(c, "resources", [])
                                ],
                            },
                        }
                    )
                else:
                    formatted_content.append({"role": role_val, "content": m.content})
            else:
                formatted_content.append(m)

        return formatted_content
