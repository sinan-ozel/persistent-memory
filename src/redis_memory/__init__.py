__version__ = "0.2.2"

import json
import logging
import os
import threading
import time

import redis

logger = logging.getLogger("redis_memory")
logger.setLevel(os.environ.get("MEMORY_LOG_LEVEL", 'WARNING'))


class Memory:
    """A synchronized key-value store that uses Redis as a shared memory
    backend. If Redis is unavailable, values are cached locally and
    queued for later syncing.

    Environment Variables:
    ----------------------
    - REDIS_HOST:   Hostname of the Redis server (default: 'localhost')
    - REDIS_PORT:   Port of the Redis server (default: 6379)
    - REDIS_PREFIX: Prefix to use for Redis keys (default: 'memory:')

    Attributes:
    -----------
    _timeout : float
        Timeout for Redis operations in seconds (default: 0.5).
    _queue : list
        Queue of (key, value) tuples to be synced when Redis becomes available.
    _attributes : dict
        Local cache of attributes (always up-to-date with the last set values).

    Examples:
    ---------
    >>> os.environ['REDIS_HOST'] = 'localhost'
    >>> os.environ['REDIS_PORT'] = '6379'

    >>> mem1 = Memory()
    >>> mem1.foo = 42
    >>> mem2 = Memory()
    >>> print(mem2.foo)
    42

    >>> mem1.bar = {"a": 1}
    >>> print(mem2.bar)
    {'a': 1}
    """

    def __init__(self,
                 redis_hostname: str = 'redis',
                 redis_port: int = 6379,
                 redis_prefix: str = 'memory:'):
        """Initialize the Memory instance and flush any queued
        updates."""
        self._host = os.environ.get('REDIS_HOST', redis_hostname)
        self._port = int(os.environ.get('REDIS_PORT', redis_port))
        self._prefix = redis_prefix
        self._timeout = 0.5  # Seconds

        self._queue = []
        self._attributes = {}
        self._last_modified = {}  # Track last modified timestamps

        self._stop_event = threading.Event()
        self._thread = None

        self._is_connected_to_redis_at_least_once = False

        self.start_background_flush()

        self._load_from_redis()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure Redis is up and queue is flushed before exit."""
        # Wait for Redis to be available
        while True:
            try:
                self._connect()
                break
            except Exception:
                if not self._is_connected_to_redis_at_least_once:
                    return  # Do not try to flush the changes.
                time.sleep(1)

        # Flush any remaining queue
        if self._queue:
            self._flush_queue()

        self.stop_background_flush()

    def _connect(self):
        """Establish a new Redis connection.

        Returns:
            redis.Redis or None: A Redis client if connection works; otherwise None.
        """
        client = redis.Redis(
            host=self._host,
            port=self._port,
            socket_connect_timeout=self._timeout,
            socket_timeout=self._timeout,
        )
        client.ping()
        return client

    def _key(self, name):
        """Generate Redis key with prefix."""
        return f"{self._prefix}{name}"

    def _flush_queue(self):
        """Keep trying to connect to Redis and flush when connected."""
        while True:
            try:
                client = self._connect()
                break  # Exit loop if connection succeeds
            except Exception:
                time.sleep(1)  # Wait before retrying

        # Flush the entire queue
        while self._queue:
            key, value = self._queue.pop(0)
            if value is None:
                try:
                    client.delete(self._key(key))
                except Exception as e:
                    logger.exception("Failed to delete key %s: %s", key, e)
            else:
                try:
                    client.set(self._key(key), json.dumps(value))
                except Exception as e:
                    logger.exception("Failed to set key %s: %s", key, e)
            self._is_connected_to_redis_at_least_once = True

    def _background_flush_loop(self):
        while not self._stop_event.is_set():
            if self._queue:
                self._flush_queue()
            time.sleep(1)

    def start_background_flush(self):
        """Start the background thread to flush the queue
        periodically."""
        if self._thread is None or not self._thread.is_alive():
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._background_flush_loop,
                                            daemon=True)
            self._thread.start()

    def stop_background_flush(self):
        """Stop the background flushing thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def _load_from_redis(self):
        """Load all keys with the current prefix from Redis into local
        cache."""
        try:
            client = self._connect()
            pattern = f"{self._prefix}*"
            for key in client.scan_iter(match=pattern):
                name = key.decode().replace(self._prefix, '', 1)
                try:
                    raw = client.get(key)
                    obj = json.loads(raw)
                    if (isinstance(obj, dict) and "value" in obj
                            and "last_modified" in obj):
                        self._attributes[name] = obj["value"]
                        self._last_modified[name] = obj["last_modified"]
                    else:
                        self._attributes[name] = obj
                    self._is_connected_to_redis_at_least_once = True
                except Exception as e:
                    logger.warning("Failed to load key %s: %s", key, e)
        except Exception:
            logger.warning("Redis unavailable. Cannot preload attributes.")

    def __setattr__(self, name, value):
        """Set an attribute. Store in Redis if available, otherwise
        queue it.

        Raises:
            ValueError: If the value is not serializable.
        """
        if name.startswith('_'):
            super().__setattr__(name, value)
            return

        try:
            _ = json.dumps(value)
        except json.JSONDecodeError:
            logger.error("Cannot serialize value for attribute '%s'", name)
            raise

        self._attributes[name] = value
        timestamp = time.time_ns()
        self._last_modified[name] = timestamp
        payload = {"value": value, "last_modified": timestamp}

        try:
            client = self._connect()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
                OSError):
            logger.warning("Redis unavailable. Queuing %s = %s", name, value)
            self._queue.append((name, payload))
            return

        client.set(self._key(name), json.dumps(payload))
        self._is_connected_to_redis_at_least_once = True

    def __getattr__(self, name):
        """Get an attribute from Redis or fall back to local cache.

        Raises:
            AttributeError: If the attribute is not found.
        """
        if name.startswith('_'):
            return super().__getattribute__(name)

        try:
            client = self._connect()
        except Exception:
            logger.warning("Memory cannot access redis to retrieve %s.")
            value = self._attributes.get(name)  # Fall back to local cache
            if value is not None:
                return value
            raise AttributeError(f"'Memory' object has no attribute '{name}'")

        raw = client.get(self._key(name))
        if raw is not None:
            obj = json.loads(raw)
            if (isinstance(obj, dict) and "value" in obj
                    and "last_modified" in obj):
                value = obj["value"]
                self._attributes[name] = value
                self._last_modified[name] = obj["last_modified"]
            else:
                value = obj
                self._attributes[name] = value
            self._is_connected_to_redis_at_least_once = True
            return value

        raise AttributeError(f"'Memory' object has no attribute '{name}'")

    def __delattr__(self, name):
        """Delete an attribute from local cache and Redis (or queue
        deletion if Redis unavailable).

        Raises:
            AttributeError: If the attribute is not found.
        """
        if name.startswith('_'):
            super().__delattr__(name)
            return

        if name not in self._attributes:
            raise AttributeError(f"'Memory' object has no attribute '{name}'")

        # Remove from local cache
        del self._attributes[name]
        if name in self._last_modified:
            del self._last_modified[name]

        try:
            client = self._connect()
        except Exception:
            logger.warning("Memory cannot access redis to delete %s.")
            self._queue.append((name, None))

        try:
            client.delete(self._key(name))
            self._is_connected_to_redis_at_least_once = True
        except UnboundLocalError:
            pass


class ConversationMemory(Memory):
    """Memory subclass that namespaces keys by conversation ID.

    Args:
        conversation_id (str): Unique ID to isolate this conversation's memory.
        redis_hostname (str): Redis host (default 'redis').
        redis_port (int): Redis port (default 6379).
        redis_prefix (str): Base prefix for keys (default 'memory:').
    """

    def __init__(self,
                 conversation_id: str,
                 redis_hostname='redis',
                 redis_port=6379,
                 redis_prefix='memory:'):
        self._conversation_id = conversation_id
        super().__init__(redis_hostname=redis_hostname,
                         redis_port=redis_port,
                         redis_prefix=redis_prefix)

    def _key(self, name):
        # Override to include conversation_id in the Redis key
        return f"{self._prefix}{self._conversation_id}:{name}"
