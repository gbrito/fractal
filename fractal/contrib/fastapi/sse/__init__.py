"""Server-Sent Events (SSE) support for FastAPI.

This module provides utilities for creating SSE endpoints that stream
events from Redis or other message sources.

Example usage:

    from fractal.contrib.fastapi.sse import (
        RedisSSEGenerator,
        create_sse_endpoint,
    )

    # Create an SSE generator that streams from Redis pub/sub
    async def event_generator(request: Request):
        generator = RedisSSEGenerator(
            redis_url="redis://localhost:6379/0",
            channels=["events:user:123"],
        )
        async for event in generator.stream(request):
            yield event

    # Or use the factory function
    router = create_sse_endpoint(
        redis_url="redis://localhost:6379/0",
        channel_factory=lambda user_id: [f"events:user:{user_id}"],
    )
"""

from fractal.contrib.fastapi.sse.generator import (
    RedisSSEGenerator,
    RedisStreamSSEGenerator,
    SSEEvent,
)
from fractal.contrib.fastapi.sse.router import create_sse_endpoint

__all__ = [
    "SSEEvent",
    "RedisSSEGenerator",
    "RedisStreamSSEGenerator",
    "create_sse_endpoint",
]
