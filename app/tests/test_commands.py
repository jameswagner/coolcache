
import pytest
from app.commands import commands, string_commands
from app.tests.helper import get_key_value_for_test, get_keys_for_test, setup_handler



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
async def test_del_command(setup_handler):
    # Set up initial data
    handler = setup_handler
    command = string_commands.SetCommand()
    await command.execute(handler, ["SET", "key1", "value1"])
    await command.execute(handler, ["SET", "key2", "value2"])
    await command.execute(handler, ["SET", "key3", "value3"])

    # Execute the command
    command = commands.DelCommand()
    response = await command.execute(handler, ["DEL", "key1", "key2"])

    # Check the response
    assert response == ":2\r\n"
    assert get_keys_for_test(handler) == ["key3"]
    assert get_key_value_for_test(handler, "key1") is None
    assert get_key_value_for_test(handler, "key2") is None
    assert get_key_value_for_test(handler, "key3") == "value3"
