import pytest
from app.commands import string_commands
from app.commands.hash_map_commands import HGetAllCommand, HGetCommand, HSetCommand
from app.tests.helper import setup_handler,  get_key_value_for_test, get_keys_for_test, get_key_expiry_for_test
from app.utils.constants import EMPTY_ARRAY_RESPONSE, NIL_RESPONSE, WRONG_TYPE_RESPONSE

@pytest.mark.asyncio
async def test_hset_command(setup_handler):
    command = HSetCommand()
    
    #test creating a new hash map
    response = await command.execute(setup_handler, ["HSET", "key", "field1", "value1", "field2", "value2"])
    assert response == "+OK\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key") == {"field1": "value1", "field2": "value2"}
    
    #test overwriting in existing values
    response = await command.execute(setup_handler, ["HSET", "key", "field1", "value3", "field2", "value4"])
    assert response == "+OK\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key") == {"field1": "value3", "field2": "value4"}
    
    #test hset on a key that is not a hash map
    await string_commands.SetCommand().execute(setup_handler, ["SET", "key", "value"])
    response = await command.execute(setup_handler, ["HSET", "key", "field1", "value1"])
    assert response == WRONG_TYPE_RESPONSE
    assert get_key_value_for_test(setup_handler, "key") == "value"
    
@pytest.mark.asyncio
async def test_hgetall_command(setup_handler):
    set_command = HSetCommand()
    get_command = HGetAllCommand()
    
    #test getting all values from a hash map
    await set_command.execute(setup_handler, ["HSET", "key", "field1", "value1", "field2", "value2"])
    response = await get_command.execute(setup_handler, ["HGETALL", "key"])
    assert response == "*4\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n"
    
    #test getting all values from a hash map that doesn't exist
    response = await get_command.execute(setup_handler, ["HGETALL", "key2"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    #test getting all values from a hash map that is not a hash map
    await string_commands.SetCommand().execute(setup_handler, ["SET", "key", "value"])
    response = await get_command.execute(setup_handler, ["HGETALL", "key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_hget_command(setup_handler):
    set_command = HSetCommand()
    get_command = HGetCommand()
    
    #test getting a value= from a hash map
    await set_command.execute(setup_handler, ["HSET", "key", "field1", "value1", "field2", "value2"])
    response = await get_command.execute(setup_handler, ["HGET", "key", "field1"])
    assert response == "$6\r\nvalue1\r\n"
    
    #test getting a non existent value from a hash map
    response = await get_command.execute(setup_handler, ["HGET", "key", "field3"])
    assert response == NIL_RESPONSE
    
    #test getting a value from a hash map that doesn't exist
    response = await get_command.execute(setup_handler, ["HGET", "key2", "field1"])
    assert response == NIL_RESPONSE
    
    #test getting a value from a hash map that is not a hash map
    await string_commands.SetCommand().execute(setup_handler, ["SET", "key", "value"])
    response = await get_command.execute(setup_handler, ["HGET", "key", "field1"])
    assert response == WRONG_TYPE_RESPONSE