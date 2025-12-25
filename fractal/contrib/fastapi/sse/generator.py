"""SSE event generators for FastAPI.

This module provides async generators that stream events from Redis
to SSE clients.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncGenerator, List, Optional

if TYPE_CHECKING:
    from fastapi import Request

logger = logging.getLogger("app")


@dataclass
class SSEEvent:
    """A Server-Sent Event.

    Attributes:
        data: The event data (will be JSON serialized if not a string)
        event: Optional event type/name
        id: Optional event ID for client reconnection
        retry: Optional retry interval in milliseconds
    """

    data: Any
    event: Optional[str] = None
    id: Optional[str] = None
    retry: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sse-starlette."""
        result: dict[str, Any] = {}

        if isinstance(self.data, str):
            result["data"] = self.data
        else:
            result["data"] = json.dumps(self.data)

        if self.event:
            result["event"] = self.event
        if self.id:
            result["id"] = self.id
        if self.retry:
            result["retry"] = self.retry

        return result


class RedisSSEGenerator:
    """Async generator for streaming Redis pub/sub messages as SSE events.

    This class connects to Redis and subscribes to specified channels,
    yielding SSE events as messages arrive.

    Example:
        >>> async def event_stream(request: Request):
        ...     generator = RedisSSEGenerator(
        ...         redis_url="redis://localhost:6379/0",
        ...         channels=["events:global", "events:user:123"],
        ...     )
        ...     async for event in generator.stream(request):
        ...         yield event

    Args:
        redis_url: Redis connection URL
        channels: List of channels to subscribe to
        reconnect_delay: Delay between reconnection attempts (seconds)
        heartbeat_interval: Interval for sending keepalive comments (seconds)
    """

    def __init__(
        self,
        redis_url: str,
        channels: List[str],
        reconnect_delay: float = 1.0,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self.redis_url = redis_url
        self.channels = channels
        self.reconnect_delay = reconnect_delay
        self.heartbeat_interval = heartbeat_interval

    async def stream(
        self,
        request: Request,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Stream events from Redis as SSE events.

        This generator yields SSE event dictionaries compatible with
        sse-starlette's EventSourceResponse.

        Args:
            request: FastAPI request object for disconnect detection

        Yields:
            SSE event dictionaries with 'data', 'event', etc. keys
        """
        import redis.asyncio as redis

        redis_client = redis.from_url(self.redis_url)
        pubsub = redis_client.pubsub()

        try:
            await pubsub.subscribe(*self.channels)

            # Send initial connection event
            yield SSEEvent(
                data={"channels": self.channels, "status": "connected"},
                event="connected",
            ).to_dict()

            last_heartbeat = asyncio.get_event_loop().time()

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.debug("SSE client disconnected")
                    break

                # Get message with timeout
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )

                if message and message["type"] == "message":
                    try:
                        data = message["data"]
                        if isinstance(data, bytes):
                            data = data.decode("utf-8")

                        # Try to parse as JSON
                        try:
                            parsed = json.loads(data)
                            event_type = parsed.get("event_type", "message")
                        except json.JSONDecodeError:
                            parsed = data
                            event_type = "message"

                        yield SSEEvent(
                            data=parsed,
                            event=event_type,
                        ).to_dict()

                    except Exception as e:
                        logger.exception(f"Error processing SSE message: {e}")

                # Send heartbeat to keep connection alive
                current_time = asyncio.get_event_loop().time()
                if current_time - last_heartbeat >= self.heartbeat_interval:
                    yield SSEEvent(
                        data="",
                        event="heartbeat",
                    ).to_dict()
                    last_heartbeat = current_time

                # Small delay to prevent busy loop
                await asyncio.sleep(0.01)

        except Exception as e:
            logger.exception(f"SSE stream error: {e}")
            yield SSEEvent(
                data={"error": str(e)},
                event="error",
            ).to_dict()

        finally:
            await pubsub.unsubscribe(*self.channels)
            await pubsub.close()
            await redis_client.close()


class RedisStreamSSEGenerator:
    """Async generator for streaming Redis Stream messages as SSE events.

    This class consumes messages from a Redis Stream using consumer groups
    and yields them as SSE events.

    Example:
        >>> async def event_stream(request: Request):
        ...     generator = RedisStreamSSEGenerator(
        ...         redis_url="redis://localhost:6379/0",
        ...         stream_name="events",
        ...         group_name="sse-consumers",
        ...         consumer_name="consumer-1",
        ...     )
        ...     async for event in generator.stream(request):
        ...         yield event

    Args:
        redis_url: Redis connection URL
        stream_name: Name of the Redis stream
        group_name: Consumer group name
        consumer_name: Unique name for this consumer
        batch_size: Number of messages to fetch per read
        block_ms: Time to block waiting for new messages
        heartbeat_interval: Interval for sending keepalive comments (seconds)
    """

    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        batch_size: int = 10,
        block_ms: int = 1000,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.heartbeat_interval = heartbeat_interval

    async def stream(
        self,
        request: Request,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Stream events from Redis Stream as SSE events.

        Args:
            request: FastAPI request object for disconnect detection

        Yields:
            SSE event dictionaries
        """
        import redis.asyncio as redis

        redis_client = redis.from_url(self.redis_url)

        try:
            # Create consumer group if it doesn't exist
            try:
                await redis_client.xgroup_create(
                    self.stream_name,
                    self.group_name,
                    id="$",  # Start from new messages
                    mkstream=True,
                )
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

            # Send initial connection event
            yield SSEEvent(
                data={
                    "stream": self.stream_name,
                    "group": self.group_name,
                    "status": "connected",
                },
                event="connected",
            ).to_dict()

            last_heartbeat = asyncio.get_event_loop().time()

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.debug("SSE client disconnected")
                    break

                # Read from stream
                messages = await redis_client.xreadgroup(
                    self.group_name,
                    self.consumer_name,
                    {self.stream_name: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )

                if messages:
                    for stream_name, entries in messages:
                        for entry_id, data in entries:
                            try:
                                # Decode bytes to strings
                                decoded = {}
                                for k, v in data.items():
                                    key = k.decode() if isinstance(k, bytes) else k
                                    val = v.decode() if isinstance(v, bytes) else v
                                    decoded[key] = val

                                # Acknowledge the message
                                await redis_client.xack(
                                    self.stream_name,
                                    self.group_name,
                                    entry_id,
                                )

                                event_type = decoded.get("event", "message")
                                yield SSEEvent(
                                    data=decoded,
                                    event=event_type,
                                    id=entry_id.decode()
                                    if isinstance(entry_id, bytes)
                                    else entry_id,
                                ).to_dict()

                            except Exception as e:
                                logger.exception(
                                    f"Error processing stream message: {e}"
                                )

                # Send heartbeat to keep connection alive
                current_time = asyncio.get_event_loop().time()
                if current_time - last_heartbeat >= self.heartbeat_interval:
                    yield SSEEvent(
                        data="",
                        event="heartbeat",
                    ).to_dict()
                    last_heartbeat = current_time

        except Exception as e:
            logger.exception(f"SSE stream error: {e}")
            yield SSEEvent(
                data={"error": str(e)},
                event="error",
            ).to_dict()

        finally:
            await redis_client.close()
