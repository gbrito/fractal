"""SSE router factory for FastAPI.

This module provides a factory function for creating SSE endpoints
with common patterns like Redis-backed event streaming.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional

if TYPE_CHECKING:
    from fastapi import APIRouter, Request

logger = logging.getLogger("app")


def create_sse_endpoint(
    redis_url: str,
    path: str = "/events",
    channel_factory: Optional[Callable[[Any], List[str]]] = None,
    default_channels: Optional[List[str]] = None,
    heartbeat_interval: float = 15.0,
) -> "APIRouter":
    """Create a FastAPI router with an SSE endpoint.

    This factory creates a router with an SSE endpoint that streams
    events from Redis pub/sub channels.

    Example:
        >>> # Simple global channel
        >>> router = create_sse_endpoint(
        ...     redis_url="redis://localhost:6379/0",
        ...     default_channels=["events:global"],
        ... )
        >>>
        >>> # User-specific channels
        >>> def get_user_channels(user_id: str) -> list[str]:
        ...     return ["events:global", f"events:user:{user_id}"]
        >>>
        >>> router = create_sse_endpoint(
        ...     redis_url="redis://localhost:6379/0",
        ...     channel_factory=get_user_channels,
        ... )

    Args:
        redis_url: Redis connection URL
        path: URL path for the SSE endpoint
        channel_factory: Optional callable that takes request params and
            returns a list of channels to subscribe to
        default_channels: Default channels when channel_factory is not provided
        heartbeat_interval: Interval for sending keepalive comments (seconds)

    Returns:
        A FastAPI router with the SSE endpoint
    """
    from fastapi import APIRouter, Request
    from sse_starlette.sse import EventSourceResponse

    from fractal.contrib.fastapi.sse.generator import RedisSSEGenerator

    router = APIRouter()

    @router.get(path)
    async def sse_events(
        request: Request,
        user_id: Optional[str] = None,
    ) -> EventSourceResponse:
        """SSE endpoint for real-time events.

        Query Parameters:
            user_id: Optional user ID for user-specific channels
        """
        # Determine channels to subscribe to
        if channel_factory is not None:
            channels = channel_factory(user_id)
        elif default_channels is not None:
            channels = default_channels.copy()
            if user_id:
                channels.append(f"events:user:{user_id}")
        else:
            channels = ["events:global"]
            if user_id:
                channels.append(f"events:user:{user_id}")

        generator = RedisSSEGenerator(
            redis_url=redis_url,
            channels=channels,
            heartbeat_interval=heartbeat_interval,
        )

        return EventSourceResponse(
            generator.stream(request),
            media_type="text/event-stream",
        )

    @router.get(f"{path}/health")
    async def sse_health() -> dict[str, str]:
        """Health check for SSE endpoint."""
        import redis.asyncio as redis

        try:
            client = redis.from_url(redis_url)
            await client.ping()
            await client.close()
            return {"status": "healthy", "redis": "connected"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    return router
