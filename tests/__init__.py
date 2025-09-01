import os
import pickle
import time

import pytest
import redis

from redis_memory import Memory


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
