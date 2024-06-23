import datetime
import time
from freezegun import freeze_time
import pytest
from app.commands import list_commands, string_commands
from app.tests.helper import setup_handler,  get_key_value_for_test, get_keys_for_test, get_key_expiry_for_test
from app.utils.constants import WRONG_TYPE_RESPONSE

@pytest.mark.asyncio
async def test_set_command(setup_handler):
    command = string_commands.SetCommand()
    response = await command.execute(setup_handler, ["SET", "key", "value"])
    assert response == "+OK\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key") == "value"
    assert get_key_expiry_for_test(setup_handler, "key") is None
    
@pytest.mark.asyncio
async def test_get_command(setup_handler):
    set_command = string_commands.SetCommand()
    await set_command.execute(setup_handler, ["SET", "key", "value"])
    
    command = string_commands.GetCommand()
    response = await command.execute(setup_handler, ["GET", "key"])
    assert response == "$5\r\nvalue\r\n"
    
    response = await command.execute(setup_handler, ["GET", "nonexistent"])
    assert response == "$-1\r\n"
    
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list", "value1"])
    response = await command.execute(setup_handler, ["GET", "list"])
    assert response == WRONG_TYPE_RESPONSE


@pytest.mark.asyncio
@freeze_time("2023-06-09 00:00:00 UTC")
async def test_set_command_with_expiration(setup_handler):
    handler = setup_handler
    command = string_commands.SetCommand()
    response = await command.execute(handler, ["SET", "key", "value", "PX", "5000"])
    assert response == "+OK\r\n"
    assert get_keys_for_test(handler) == ["key"]
    assert get_key_value_for_test(handler, "key") == "value"
    assert int(get_key_expiry_for_test(handler, "key")) == datetime.datetime(2023, 6, 9, 0, 0, 5).timestamp()

    # Check that the value is accessible before expiration
    get_command = string_commands.GetCommand()
    response = await get_command.execute(handler, ["GET", "key"])
    assert response == "$5\r\nvalue\r\n"

    # Move time forward to simulate expiration
    with freeze_time("2023-06-09 00:05:01"):
        response = await get_command.execute(handler, ["GET", "key"])
        assert response == "$-1\r\n"
        assert get_keys_for_test(handler) == []
    
    
@pytest.mark.asyncio
async def test_mset_command(setup_handler):
    command = string_commands.MSetCommand()
    response = await command.execute(setup_handler, ["MSET", "key1", "value1", "key2", "value2"])
    assert response == "+OK\r\n"
    assert get_keys_for_test(setup_handler) == ["key1", "key2"]
    assert get_key_value_for_test(setup_handler, "key1") == "value1"
    assert get_key_value_for_test(setup_handler, "key2") == "value2"

@pytest.mark.asyncio
async def test_mget_command(setup_handler):
    mset_command = string_commands.MSetCommand()
    await mset_command.execute(setup_handler, ["MSET", "key1", "value1", "key2", "value2"])

    command = string_commands.MGetCommand()
    response = await command.execute(setup_handler, ["MGET", "key1", "key2"])
    assert response == "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"

    response = await command.execute(setup_handler, ["MGET", "key1", "nonexistent"])
    print(response)
    assert response == "*2\r\n$6\r\nvalue1\r\n$-1\r\n"

    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list", "value1"])
    response = await command.execute(setup_handler, ["MGET", "key1", "list"])
    assert response == "*2\r\n$6\r\nvalue1\r\n$-1\r\n"
        
@pytest.mark.asyncio
async def test_incr_command(setup_handler):
    set_command = string_commands.SetCommand()
    await set_command.execute(setup_handler, ["SET", "counter", "10"])

    command = string_commands.IncrCommand()
    response = await command.execute(setup_handler, ["INCR", "counter"])
    assert response == ":11\r\n"
    assert get_keys_for_test(setup_handler) == ["counter"]
    assert get_key_value_for_test(setup_handler, "counter") == "11"

    response = await command.execute(setup_handler, ["INCR", "nonexistent"])
    assert response == ":1\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "nonexistent") == "1"

    response = await command.execute(setup_handler, ["INCR", "counter"])
    assert response == ":12\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "counter") == "12"
    
    await set_command.execute(setup_handler, ["SET", "non_int_value", "hello"])
    response = await command.execute(setup_handler, ["INCR", "non_int_value"])
    assert response == string_commands.NON_INT_ERROR
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent", "non_int_value"]
        
        

@pytest.mark.asyncio
async def test_incrby_command(setup_handler):
    set_command = string_commands.SetCommand()
    await set_command.execute(setup_handler, ["SET", "counter", "10"])

    command = string_commands.IncrByCommand()
    response = await command.execute(setup_handler, ["INCRBY", "counter", "5"])
    assert response == ":15\r\n"
    assert get_keys_for_test(setup_handler) == ["counter"]
    assert get_key_value_for_test(setup_handler, "counter") == "15"

    response = await command.execute(setup_handler, ["INCRBY", "nonexistent", "5"])
    assert response == ":5\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "nonexistent") == "5"

    response = await command.execute(setup_handler, ["INCRBY", "counter", "2"])
    assert response == ":17\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "counter") == "17"
    
    response = await command.execute(setup_handler, ["INCRBY", "counter", "non_int_increment"])
    assert response == string_commands.NON_INT_ERROR
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]

    await set_command.execute(setup_handler, ["SET", "non_int_value", "hello"])
    response = await command.execute(setup_handler, ["INCRBY", "non_int_value", "5"])
    assert response == string_commands.NON_INT_ERROR
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent", "non_int_value"]
    assert get_key_value_for_test(setup_handler, "non_int_value") == "hello"

@pytest.mark.asyncio
async def test_decr_command(setup_handler):
    set_command = string_commands.SetCommand()
    await set_command.execute(setup_handler, ["SET", "counter", "10"])

    command = string_commands.DecrCommand()
    response = await command.execute(setup_handler, ["DECR", "counter"])
    assert response == ":9\r\n"
    assert get_keys_for_test(setup_handler) == ["counter"]
    assert get_key_value_for_test(setup_handler, "counter") == "9"

    response = await command.execute(setup_handler, ["DECR", "nonexistent"])
    assert response == ":-1\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "nonexistent") == "-1"

    response = await command.execute(setup_handler, ["DECR", "counter"])
    assert response == ":8\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "counter") == "8"


@pytest.mark.asyncio
async def test_decrby_command(setup_handler):
    set_command = string_commands.SetCommand()
    await set_command.execute(setup_handler, ["SET", "counter", "10"])

    command = string_commands.DecrByCommand()
    response = await command.execute(setup_handler, ["DECRBY", "counter", "5"])
    
    assert get_keys_for_test(setup_handler) == ["counter"]
    assert get_key_value_for_test(setup_handler, "counter") == "5"
    assert response == ":5\r\n"

    response = await command.execute(setup_handler, ["DECRBY", "nonexistent", "5"])
    assert response == ":-5\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "nonexistent") == "-5"

    response = await command.execute(setup_handler, ["DECRBY", "counter", "2"])
    assert response == ":3\r\n"
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "counter") == "3"
    
    response = await command.execute(setup_handler, ["DECRBY", "counter", "non_int_increment"])
    assert response == string_commands.NON_INT_ERROR
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent"]

    await set_command.execute(setup_handler, ["SET", "non_int_value", "hello"])
    response = await command.execute(setup_handler, ["DECRBY", "non_int_value", "5"])
    assert response == string_commands.NON_INT_ERROR
    assert get_keys_for_test(setup_handler) == ["counter", "nonexistent", "non_int_value"]
    assert get_key_value_for_test(setup_handler, "non_int_value") == "hello"
    

@pytest.mark.asyncio
async def test_append_command(setup_handler):
    set_command = string_commands.SetCommand()
    await set_command.execute(setup_handler, ["SET", "key", "Hello"])

    command = string_commands.AppendCommand()
    response = await command.execute(setup_handler, ["APPEND", "key", " World"])
    assert response == ":11\r\n"

    response = await command.execute(setup_handler, ["APPEND", "nonexistent", " World"])
    assert response == ":6\r\n"

    response = await command.execute(setup_handler, ["APPEND", "key", "!"])
    assert response == ":12\r\n"

    assert get_keys_for_test(setup_handler) == ["key", "nonexistent"]
    assert get_key_value_for_test(setup_handler, "key") == "Hello World!"
    assert get_key_value_for_test(setup_handler, "nonexistent") == " World"
    
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list", "value1"])
    response = await command.execute(setup_handler, ["APPEND", "list", " World"])
    assert response == WRONG_TYPE_RESPONSE
    assert get_keys_for_test(setup_handler) == ["key", "nonexistent", "list"]
    assert get_key_value_for_test(setup_handler, "list") == ["value1"]
    
    