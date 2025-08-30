import os
import pickle
import time

import pytest
import redis

from memory import Memory


@pytest.fixture(scope="function", autouse=True)
def setup_env_and_clear_redis():
    os.environ['REDIS_HOST'] = 'redis'
    os.environ['REDIS_PORT'] = '6379'
    mem = Memory()
    client = mem._connect()
    if client:
        keys = client.keys(f"{mem._prefix}*")
        if keys:
            client.delete(*keys)
    yield
    # Cleanup after each test
    mem = Memory()
    client = mem._connect()
    if client:
        keys = client.keys(f"{mem._prefix}*")
        if keys:
            client.delete(*keys)


def get_redis_value(key: str, prefix='memory:', host='redis', port=6379):
    client = redis.Redis(host=host, port=port)
    raw = client.get(f"{prefix}{key}")
    if raw is not None:
        return pickle.loads(raw)
    return None


def test_set_and_get_scalar():
    """Test setting and getting a scalar value across Memory instances."""
    mem1 = Memory()
    mem2 = Memory()
    mem1.answer = 42
    assert mem2.answer == 42
    assert get_redis_value("answer") == 42


def test_set_and_get_dict():
    """Test setting and getting a dictionary value in Memory."""
    mem = Memory()
    expected = {"theme": "dark", "volume": 0.75}
    mem.settings = expected
    assert mem.settings == expected
    assert get_redis_value("settings") == expected


def test_set_and_get_list():
    """Test setting and getting a list value in Memory."""
    mem = Memory()
    expected = [1, 2, 3]
    mem.items = expected
    assert mem.items == expected
    assert get_redis_value("items") == expected


def test_overwrite_existing_key():
    """Test that assigning a new value overwrites the existing Redis key."""
    mem = Memory()
    mem.key = "initial"
    assert get_redis_value("key") == "initial"
    mem.key = "updated"
    assert mem.key == "updated"
    assert get_redis_value("key") == "updated"


def test_missing_attribute_raises():
    """Test that accessing an unset attribute raises AttributeError."""
    mem = Memory()
    with pytest.raises(AttributeError):
        _ = mem.does_not_exist


def test_internal_attributes_not_stored():
    """Test that internal attributes are not stored in Redis."""
    mem = Memory()
    assert hasattr(mem, '_host')
    assert '_host' not in mem.__dict__.get('_attributes', {})


def test_multiple_instances_consistency():
    """Test that Memory instances share a consistent state."""
    m1 = Memory()
    m2 = Memory()
    m1.shared = {"a": 10}
    assert m2.shared == {"a": 10}


def test_attribute_persistence_across_instances():
    """Test that attributes persist across new Memory instances."""
    Memory().temp = "hello"
    assert Memory().temp == "hello"


def test_queue_flush_when_redis_restored(monkeypatch):
    """Test queueing when Redis is down and flushing when it's restored."""

    # Simulate bad Redis environment (Redis is down)
    monkeypatch.setenv('REDIS_HOST', 'nonexistent-host')
    monkeypatch.setenv('REDIS_PORT', '9999')

    mem = Memory()
    mem.foo = "bar"

    # Confirm value is queued
    assert ("foo", "bar") in mem._queue
    assert mem.foo == "bar"

    # Simulate Redis restored
    monkeypatch.setenv('REDIS_HOST', 'redis')
    monkeypatch.setenv('REDIS_PORT', '6379')

    mem_restored = Memory(redis_prefix=mem._prefix)
    mem_restored._queue = mem._queue.copy()
    mem_restored._flush_queue()

    client = redis.Redis(host='redis', port=6379)
    raw = client.get("memory:foo")
    assert raw is not None
    assert pickle.loads(raw) == "bar"


def test_queue_overwrite_before_redis_restored(monkeypatch):
    """Test that latest queued value is saved after Redis is restored."""

    monkeypatch.setenv('REDIS_HOST', 'nonexistent-host')
    monkeypatch.setenv('REDIS_PORT', '9999')

    mem = Memory()
    mem.config = {"a": 1}
    mem.config = {"a": 2}

    assert mem.config == {"a": 2}
    assert mem._queue[-1] == ("config", {"a": 2})

    # Redis comes back online
    monkeypatch.setenv('REDIS_HOST', 'redis')
    monkeypatch.setenv('REDIS_PORT', '6379')

    mem_restored = Memory(redis_prefix=mem._prefix)
    mem_restored._queue = mem._queue.copy()
    mem_restored._flush_queue()

    client = redis.Redis(host='redis', port=6379)
    raw = client.get("memory:config")
    assert raw is not None
    assert pickle.loads(raw) == {"a": 2}


@pytest.mark.depends(on=['test_set_and_get_scalar'])
def test_memory_with_custom_prefix():
    """Test that Memory with a custom prefix stores and retrieves data correctly."""
    custom_prefix = "custom:"
    mem = Memory(redis_prefix=custom_prefix)
    mem.status = "active"
    assert mem.status == "active"

    # Verify it's not saved under default prefix
    assert get_redis_value("status", prefix="memory:") is None

    # Verify it's saved under the custom prefix
    assert get_redis_value("status", prefix=custom_prefix) == "active"


def test_set_and_delete_attribute(monkeypatch):
    mem = Memory()
    mem.key = "value"

    # Delete attribute, should queue deletion since Redis unavailable
    del mem.key

    # Check attribute removed locally
    with pytest.raises(AttributeError):
        _ = mem.key


@pytest.mark.depends(on=['test_set_and_delete_attribute'])
def test_delete_attribute_queues_if_redis_down(monkeypatch):
    """Test that deleting an attribute while Redis is down queues the deletion properly."""

    # First, ensure Redis is up to set the value
    monkeypatch.setenv('REDIS_HOST', 'redis')
    monkeypatch.setenv('REDIS_PORT', '6379')
    mem = Memory()
    mem.queued_key = "queued value"

    # Now simulate Redis going down
    monkeypatch.setenv('REDIS_HOST', 'nonexistent-host')
    monkeypatch.setenv('REDIS_PORT', '9999')
    mem_down = Memory(redis_prefix=mem._prefix)
    mem_down._attributes = mem._attributes.copy()  # simulate state carry-over

    print(mem_down._attributes)
    # Delete attribute, should queue deletion
    del mem_down.queued_key
    print(mem_down._attributes)

    client = redis.Redis(host='redis', port=6379)
    print(client.keys())

    # Should raise since it's removed from local cache
    with pytest.raises(AttributeError):
        _ = mem_down.queued_key

    # Check deletion was queued (None marks deletion)
    assert ("queued_key", None) in mem_down._queue


@pytest.mark.depends(on=['test_set_and_delete_attribute',
                         'test_queue_flush_when_redis_restored',
                         'test_queue_overwrite_before_redis_restored'])
def test_background_flush_automatically(monkeypatch):
    """Test that the background flush loop automatically flushes queued data when Redis comes back online."""

    # Simulate Redis being down
    monkeypatch.setenv('REDIS_HOST', 'nonexistent-host')
    monkeypatch.setenv('REDIS_PORT', '9999')
    mem = Memory()

    # Set attribute (should be queued)
    mem.test_key = "queued_value"

    assert mem._attributes.get("test_key") == "queued_value"
    assert ("test_key", "queued_value") in mem._queue
    assert mem.test_key == "queued_value"

    # Redis comes back online
    monkeypatch.setenv('REDIS_HOST', 'redis')
    monkeypatch.setenv('REDIS_PORT', '6379')

    mem_restored = Memory(redis_prefix=mem._prefix)
    mem_restored._queue = mem._queue.copy()

    # Wait for background thread to pick up the connection and flush
    time.sleep(2)

    client = redis.Redis(host='redis', port=6379)
    print(client.keys())
    raw = client.get(mem._key("test_key"))
    assert raw is not None
    assert pickle.loads(raw) == "queued_value"

    # Clean up
    del mem.test_key


def test_memory_loads_existing_keys_on_init():
    """Test that Memory instance loads existing Redis keys on initialization."""
    # Set a key using one instance
    mem1 = Memory()
    mem1.user = {"name": "Alice", "role": "admin"}

    # Simulate new process / restart by creating a fresh instance
    mem2 = Memory()

    # Should have preloaded the same key from Redis
    assert "user" in mem2._attributes
    assert mem2.user == {"name": "Alice", "role": "admin"}


def test_basic_context_set_and_get():
    """
    Test that values set in a `with Memory()` context are correctly persisted
    and can be retrieved in a later context block.
    """
    with Memory() as memory:
        memory.test_key = "hello world"

    with Memory() as memory:
        assert memory.test_key == "hello world"


def test_context_lifecycle_and_del():
    """
    Test that Memory context persists values even after the object is deleted,
    as long as Redis remains available.
    """
    with Memory() as memory:
        memory.context = "Je me souviens."

    # Delete outside the context
    del memory

    # Reload and check persistence
    with Memory() as memory:
        assert memory.context == "Je me souviens."


def test_non_serializable_value():
    """
    Test that assigning a non-serializable value (e.g., a lambda function)
    to a Memory attribute raises a PicklingError.
    """
    with pytest.raises(AttributeError):
        with Memory() as memory:
            memory.bad = lambda x: x * 2  # Not serializable


@pytest.mark.depends(on=['test_set_and_delete_attribute'])
def test_works_if_redis_never_comes_alive(monkeypatch):
    """Test that deleting an attribute while Redis is down queues the deletion properly."""

    monkeypatch.setenv('REDIS_HOST', 'nonexistent-host')
    monkeypatch.setenv('REDIS_PORT', '9999')
    with Memory() as memory:
        memory.test_key = "hello world"
        assert memory.test_key == "hello world"
        del memory.test_key
        with pytest.raises(AttributeError):
            _ = memory.queued_key

    monkeypatch.setenv('REDIS_HOST', 'redis')
    monkeypatch.setenv('REDIS_PORT', '6379')
