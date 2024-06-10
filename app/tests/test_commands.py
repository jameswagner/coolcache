import time
import pytest
from unittest.mock import AsyncMock
import app.commands.commands as commands
from freezegun import freeze_time

@pytest.fixture
def setup_handler():
    handler = AsyncMock()
    handler.memory = {}
    handler.expiration = {}
    handler.server.writers = []
    handler.server.numacks = 0
    mock_writer = AsyncMock()
    handler.server.writers = [mock_writer]
    return handler

@pytest.mark.asyncio
async def test_ping_command(setup_handler):
    command = commands.PingCommand()
    response = await command.execute(setup_handler, ["PING"])
    assert response == "+PONG\r\n"

@pytest.mark.asyncio
async def test_echo_command(setup_handler):
    command = commands.EchoCommand()
    response = await command.execute(setup_handler, ["ECHO", "Hello"])
    assert response == "+Hello\r\n"

@pytest.mark.asyncio
async def test_set_command(setup_handler):
    command = commands.SetCommand()
    response = await command.execute(setup_handler, ["SET", "key", "value"])
    assert response == "+OK\r\n"
    assert len(setup_handler.memory) == 1
    assert len(setup_handler.expiration) == 1
    assert setup_handler.memory["key"] == "value"
    assert setup_handler.expiration["key"] is None
    assert setup_handler.server.numacks == 0
    setup_handler.server.writers[0].write.assert_called_once_with(b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')

@pytest.mark.asyncio
async def test_get_command(setup_handler):
    command = commands.GetCommand()
    setup_handler.memory["key"] = "value"
    response = await command.execute(setup_handler, ["GET", "key"])
    assert response == "$5\r\nvalue\r\n"

    setup_handler.memory["key"] = None
    response = await command.execute(setup_handler, ["GET", "key"])
    assert response == "$-1\r\n"


@pytest.mark.asyncio
@freeze_time("2023-06-09")
async def test_set_command_with_expiration(setup_handler):
    handler = setup_handler
    command = commands.SetCommand()
    response = await command.execute(handler, ["SET", "key", "value", "PX", "5000"])

    assert response == "+OK\r\n"
    assert handler.memory["key"] == "value"
    assert handler.expiration["key"] > time.time()
    assert len(handler.memory) == 1
    assert len(handler.expiration) == 1
    assert handler.server.numacks == 0
    handler.server.writers[0].write.assert_called_once_with(b'*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n5000\r\n')

    # Check that the value is accessible before expiration
    get_command = commands.GetCommand()
    response = await get_command.execute(handler, ["GET", "key"])
    assert response == "$5\r\nvalue\r\n"

    # Move time forward to simulate expiration
    with freeze_time("2023-06-09 00:05:01"):
        response = await get_command.execute(handler, ["GET", "key"])
        assert response == "$-1\r\n"
        assert "key" not in handler.memory
        assert "key" not in handler.expiration