"""Redis contrib module for Fractal Framework.

This module provides Redis-based implementations for:
- Event publishing via Redis Pub/Sub
- Event consuming via Redis Pub/Sub
- Event streaming via Redis Streams

Example usage:

    # Publishing events
    from fractal.contrib.redis import RedisEventBusProjector

    projector = RedisEventBusProjector(
        redis_url="redis://localhost:6379/0",
        channel_prefix="myapp",
    )
    publisher = EventPublisher([projector])
    publisher.publish_event(MyEvent(...))

    # Consuming events
    from fractal.contrib.redis import RedisEventBus

    event_bus = RedisEventBus(
        redis_url="redis://localhost:6379/0",
        command_bus=command_bus,
        channel_prefix="myapp",
        event_classes=[MyEvent],
    )
    event_bus.start()  # Blocking, runs in event loop
"""

from fractal.contrib.redis.event_bus import (
    RedisEventBus,
    RedisEventBusListener,
    RedisStreamConsumer,
)
from fractal.contrib.redis.projectors import (
    RedisEventBusProjector,
    RedisStreamProjector,
)

__all__ = [
    "RedisEventBus",
    "RedisEventBusListener",
    "RedisStreamConsumer",
    "RedisEventBusProjector",
    "RedisStreamProjector",
]
