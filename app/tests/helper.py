import pytest
from unittest.mock import AsyncMock

from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler
import time

@pytest.fixture
def setup_handler():
    handler = AsyncMock()
    handler.memory = {}
    handler.expiration = {}
    handler.server = AsyncMock()
    handler.server.writers = []
    handler.server.numacks = 0
    handler.server.config = {
        "dir": "",
        "dbfilename": "dump.rdb",
        "save": "900 1 300 10 60 10000"  # Default Redis-like save configuration
    }
    handler.server.last_save_time = 0
    handler.server.memory = handler.memory  # For direct access in some commands
    handler.server.expiration = handler.expiration
    handler.server.streamstore = {}
    mock_writer = AsyncMock()
    handler.server.writers = [mock_writer]
    return handler


def get_keys_for_test(handler: 'AsyncRequestHandler', include_expired: bool = False) -> List[str]:
    keys = []
    for key in handler.memory.keys():
        if include_expired or (key not in handler.expiration or not handler.expiration[key] or handler.expiration[key] >= time.time()):
            keys.append(key)
    return keys

def get_key_value_for_test(handler: 'AsyncRequestHandler', key: str) -> Any:
    if key not in handler.memory:
        return None
    return handler.memory[key]

def get_key_expiry_for_test(handler: 'AsyncRequestHandler', key: str) -> None|int:
    if key not in handler.expiration:
        return None
    return handler.expiration[key]