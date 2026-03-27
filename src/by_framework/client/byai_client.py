from typing import List, Optional, Union

from by_framework.common.redis_client import Redis
from by_framework.core.protocol.message import BaiYingMessage
from by_framework.core.registry import WorkerRegistry

from .client import GatewayClient
from .interceptors import ByaiMessageInterceptor, GatewayInterceptor


class ByaiGatewayClient(
    GatewayClient[Union[str, BaiYingMessage, List[BaiYingMessage]]]
):
    """
    A specialized GatewayClient for the Byai domain.
    It automatically includes the ByaiMessageInterceptor to handle BaiYingMessage objects.
    """

    def __init__(
        self,
        registry: Optional[WorkerRegistry] = None,
        redis_client: Optional[Redis] = None,
        interceptors: Optional[List[GatewayInterceptor]] = None,
    ):
        # 1. Start with the ByaiMessageInterceptor
        default_interceptors = [ByaiMessageInterceptor()]

        # 2. Append any additional user-provided interceptors
        if interceptors:
            default_interceptors.extend(interceptors)

        # 3. Initialize the base GatewayClient
        super().__init__(
            registry=registry,
            redis_client=redis_client,
            interceptors=default_interceptors,
        )
