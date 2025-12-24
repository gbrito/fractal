"""Redis event bus for consuming events.

This module provides an event bus that consumes events from Redis Pub/Sub
channels and dispatches them to command handlers.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import TYPE_CHECKING, Callable, List, Type

import redis

from fractal.core.command_bus.command_bus import CommandBus
from fractal.core.event_sourcing.event import ReceivingEvent
from fractal.core.event_sourcing.event_bus import EventBus
from fractal.core.event_sourcing.message import Message

if TYPE_CHECKING:
    from redis import Redis
    from redis.client import PubSub

logger = logging.getLogger("app")


class RedisEventBus(EventBus):
    """Event bus that consumes events from Redis Pub/Sub channels.

    This class subscribes to Redis channels for each event class and
    dispatches received events to the command bus.

    Example:
        >>> event_bus = RedisEventBus(
        ...     redis_url="redis://localhost:6379/0",
        ...     command_bus=command_bus,
        ...     channel_prefix="myapp",
        ...     event_classes=[UserCreatedEvent, OrderPlacedEvent],
        ... )
        >>> event_bus.start()  # Blocking

    Args:
        redis_url: Redis connection URL
        command_bus: The command bus for dispatching commands
        channel_prefix: Optional prefix for channel names
        event_classes: List of event classes to subscribe to
        use_thread: If True, run listener in background thread
    """

    def __init__(
        self,
        redis_url: str,
        command_bus: CommandBus,
        channel_prefix: str = "",
        event_classes: List[Type[ReceivingEvent]] = None,
        use_thread: bool = False,
    ) -> None:
        self.redis_url = redis_url
        self.command_bus = command_bus
        self.channel_prefix = channel_prefix
        self.event_classes = event_classes or []
        self.use_thread = use_thread
        self._listeners: list[RedisEventBusListener] = []

    def _get_channel_name(self, event_name: str) -> str:
        """Get the full channel name for an event."""
        if self.channel_prefix:
            return f"{self.channel_prefix}:{event_name}"
        return event_name

    def start(self) -> None:
        """Start listening for events.

        If use_thread is True, listeners run in background threads.
        Otherwise, this method blocks.
        """
        for event_class in self.event_classes:
            channel = self._get_channel_name(event_class.__name__)
            logger.info(f"Subscribing to Redis channel: {channel}")

            listener = RedisEventBusListener(
                redis_url=self.redis_url,
                command_bus=self.command_bus,
                event_class=event_class,
                channel=channel,
            )
            self._listeners.append(listener)

            if self.use_thread:
                thread = threading.Thread(target=listener.run, daemon=True)
                thread.start()
            else:
                listener.run()

    def stop(self) -> None:
        """Stop all listeners."""
        for listener in self._listeners:
            listener.stop()


class RedisEventBusListener:
    """Listener for a single Redis Pub/Sub channel.

    This class handles the actual subscription and message processing
    for a specific event type.

    Args:
        redis_url: Redis connection URL
        command_bus: The command bus for dispatching commands
        event_class: The event class to handle
        channel: The Redis channel to subscribe to
    """

    def __init__(
        self,
        redis_url: str,
        command_bus: CommandBus,
        event_class: Type[ReceivingEvent],
        channel: str,
    ) -> None:
        self.redis_url = redis_url
        self.command_bus = command_bus
        self.event_class = event_class
        self.channel = channel
        self._running = False
        self._pubsub: PubSub | None = None

    def run(self) -> None:
        """Start listening for messages on the channel.

        This method blocks until stop() is called.
        """
        logger.info(f"Starting Redis listener for channel: {self.channel}")

        client: Redis = redis.from_url(self.redis_url)
        self._pubsub = client.pubsub()
        self._pubsub.subscribe(self.channel)
        self._running = True

        try:
            for message in self._pubsub.listen():
                if not self._running:
                    break

                if message["type"] != "message":
                    continue

                try:
                    self._handle_message(message["data"])
                except Exception as e:
                    logger.exception(f"Error handling message: {e}")

        finally:
            self._pubsub.close()
            client.close()

    def stop(self) -> None:
        """Stop listening for messages."""
        self._running = False
        if self._pubsub:
            self._pubsub.unsubscribe()

    def _handle_message(self, data: bytes) -> None:
        """Process a received message.

        Args:
            data: The raw message data from Redis
        """
        try:
            payload = json.loads(data)
            message = Message(**payload)
            event = self.event_class(**json.loads(message.data))

            logger.debug(f"Received event from Redis: {event}")

            command = event.to_command()
            if command:
                self.command_bus.handle(command)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            raise


class RedisStreamConsumer:
    """Consumer for Redis Streams with consumer group support.

    This class provides reliable event consumption using Redis Streams
    consumer groups, with automatic acknowledgment and pending message
    recovery.

    Example:
        >>> consumer = RedisStreamConsumer(
        ...     redis_url="redis://localhost:6379/0",
        ...     stream_name="events",
        ...     group_name="myapp",
        ...     consumer_name="worker-1",
        ...     handler=process_event,
        ... )
        >>> consumer.start()  # Blocking

    Args:
        redis_url: Redis connection URL
        stream_name: Name of the Redis stream
        group_name: Consumer group name
        consumer_name: Unique name for this consumer
        handler: Callback function for processing events
        batch_size: Number of messages to fetch per read
        block_ms: Time to block waiting for new messages
    """

    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        handler: Callable[[dict], None],
        batch_size: int = 10,
        block_ms: int = 5000,
    ) -> None:
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.handler = handler
        self.batch_size = batch_size
        self.block_ms = block_ms
        self._running = False

    def start(self) -> None:
        """Start consuming messages from the stream.

        Creates the consumer group if it doesn't exist, then enters
        the main consumption loop.
        """
        client: Redis = redis.from_url(self.redis_url)
        self._running = True

        # Create consumer group if it doesn't exist
        try:
            client.xgroup_create(
                self.stream_name,
                self.group_name,
                id="0",
                mkstream=True,
            )
            logger.info(
                f"Created consumer group '{self.group_name}' "
                f"for stream '{self.stream_name}'"
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            logger.debug(f"Consumer group '{self.group_name}' already exists")

        try:
            # First, process any pending messages
            self._process_pending(client)

            # Then, consume new messages
            while self._running:
                messages = client.xreadgroup(
                    self.group_name,
                    self.consumer_name,
                    {self.stream_name: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )

                if not messages:
                    continue

                for stream, entries in messages:
                    for entry_id, data in entries:
                        try:
                            # Decode bytes to strings
                            decoded = {
                                k.decode() if isinstance(k, bytes) else k: v.decode()
                                if isinstance(v, bytes)
                                else v
                                for k, v in data.items()
                            }
                            self.handler(decoded)
                            client.xack(self.stream_name, self.group_name, entry_id)
                        except Exception as e:
                            logger.exception(
                                f"Error processing message {entry_id}: {e}"
                            )

        finally:
            client.close()

    def stop(self) -> None:
        """Stop the consumer."""
        self._running = False

    def _process_pending(self, client: Redis) -> None:
        """Process any pending messages that weren't acknowledged."""
        while self._running:
            pending = client.xreadgroup(
                self.group_name,
                self.consumer_name,
                {self.stream_name: "0"},
                count=self.batch_size,
            )

            if not pending or not pending[0][1]:
                break

            for stream, entries in pending:
                for entry_id, data in entries:
                    try:
                        decoded = {
                            k.decode() if isinstance(k, bytes) else k: v.decode()
                            if isinstance(v, bytes)
                            else v
                            for k, v in data.items()
                        }
                        self.handler(decoded)
                        client.xack(self.stream_name, self.group_name, entry_id)
                        logger.debug(f"Processed pending message: {entry_id}")
                    except Exception as e:
                        logger.exception(
                            f"Error processing pending message {entry_id}: {e}"
                        )
