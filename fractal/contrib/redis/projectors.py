"""Redis event projectors for publishing events.

This module provides projectors that publish events to Redis:
- RedisEventBusProjector: Publishes to Redis Pub/Sub channels
- RedisStreamProjector: Publishes to Redis Streams for persistent messaging
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from json import JSONEncoder
from typing import TYPE_CHECKING, Any

import redis

from fractal.core.event_sourcing.event import BasicSendingEvent
from fractal.core.event_sourcing.event_projector import EventProjector
from fractal.core.event_sourcing.message import Message

if TYPE_CHECKING:
    from redis import Redis

logger = logging.getLogger("app")


class DateTimeEncoder(JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class RedisEventBusProjector(EventProjector):
    """Projector that publishes events to Redis Pub/Sub channels.

    Events are published to channels named after the event class,
    with an optional prefix for namespacing.

    Example:
        >>> projector = RedisEventBusProjector(
        ...     redis_url="redis://localhost:6379/0",
        ...     channel_prefix="myapp",
        ... )
        >>> projector.project("stream-123", UserCreatedEvent(...))
        # Publishes to channel: myapp:UserCreatedEvent

    Args:
        redis_url: Redis connection URL (e.g., redis://localhost:6379/0)
        channel_prefix: Optional prefix for channel names
        json_encoder: Custom JSON encoder class for serialization
    """

    def __init__(
        self,
        redis_url: str,
        channel_prefix: str = "",
        json_encoder: type[JSONEncoder] | None = None,
    ) -> None:
        self.redis_client: Redis = redis.from_url(redis_url)
        self.channel_prefix = channel_prefix
        self.json_encoder = json_encoder or DateTimeEncoder

    def _get_channel_name(self, event_name: str) -> str:
        """Get the full channel name for an event."""
        if self.channel_prefix:
            return f"{self.channel_prefix}:{event_name}"
        return event_name

    def project(self, id: str, event: BasicSendingEvent) -> None:
        """Publish an event to a Redis Pub/Sub channel.

        Args:
            id: The event stream identifier
            event: The domain event to publish
        """
        channel = self._get_channel_name(event.__class__.__name__)

        message = Message(
            id=id,
            occurred_on=datetime.now(timezone.utc),
            event=event.__class__.__name__,
            data=json.dumps(asdict(event), cls=self.json_encoder),
            object_id=event.object_id,
            aggregate_root_id=event.aggregate_root_id,
        )

        self.redis_client.publish(
            channel,
            json.dumps(asdict(message), cls=self.json_encoder),
        )
        logger.debug(f"Event published to Redis channel '{channel}': {message}")


class RedisStreamProjector(EventProjector):
    """Projector that publishes events to Redis Streams.

    Redis Streams provide persistent, ordered event storage with consumer
    group support. This is useful for reliable event delivery and replay.

    Example:
        >>> projector = RedisStreamProjector(
        ...     redis_url="redis://localhost:6379/0",
        ...     stream_name="events",
        ...     max_len=10000,
        ... )
        >>> projector.project("stream-123", UserCreatedEvent(...))
        # Adds to stream: events

    Args:
        redis_url: Redis connection URL
        stream_name: Name of the Redis stream
        max_len: Maximum stream length (older entries trimmed)
        json_encoder: Custom JSON encoder class for serialization
    """

    def __init__(
        self,
        redis_url: str,
        stream_name: str = "events",
        max_len: int | None = None,
        json_encoder: type[JSONEncoder] | None = None,
    ) -> None:
        self.redis_client: Redis = redis.from_url(redis_url)
        self.stream_name = stream_name
        self.max_len = max_len
        self.json_encoder = json_encoder or DateTimeEncoder

    def project(self, id: str, event: BasicSendingEvent) -> str:
        """Add an event to a Redis Stream.

        Args:
            id: The event stream identifier
            event: The domain event to publish

        Returns:
            The Redis stream entry ID
        """
        message = Message(
            id=id,
            occurred_on=datetime.now(timezone.utc),
            event=event.__class__.__name__,
            data=json.dumps(asdict(event), cls=self.json_encoder),
            object_id=event.object_id,
            aggregate_root_id=event.aggregate_root_id,
        )

        # Redis streams expect flat key-value pairs
        stream_entry = {
            "id": message.id,
            "occurred_on": message.occurred_on.isoformat(),
            "event": message.event,
            "data": message.data,
            "object_id": message.object_id,
            "aggregate_root_id": message.aggregate_root_id,
        }

        entry_id = self.redis_client.xadd(
            self.stream_name,
            stream_entry,
            maxlen=self.max_len,
        )

        logger.debug(
            f"Event added to Redis stream '{self.stream_name}' "
            f"with ID {entry_id}: {message}"
        )
        return entry_id
