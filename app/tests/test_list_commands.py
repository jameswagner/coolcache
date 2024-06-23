import pytest
import app.commands.list_commands as list_commands
import app.commands.string_commands as string_commands
from app.tests.helper import setup_handler
from app.tests import helper
from app.utils.constants import NIL_RESPONSE, NON_INT_ERROR, WRONG_TYPE_RESPONSE, EMPTY_ARRAY_RESPONSE
from app.utils.encoding_utils import redis_array_to_list, redis_bulk_string_to_string

@pytest.mark.asyncio
async def test_lpush_command(setup_handler):
    command = list_commands.LPushCommand()
    
    #push to non-existing list    
    response = await command.execute(setup_handler, ["LPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    assert setup_handler.memory["key"] == ["value2", "value1"]
    
    #push to existing list
    response = await command.execute(setup_handler, ["LPUSH", "key", "value3"])
    assert response == ":3\r\n"
    assert setup_handler.memory["key"] == ["value3", "value2", "value1"]
    
    #push to element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LPUSH", "string_key", "value"])
    assert response == WRONG_TYPE_RESPONSE
    assert setup_handler.memory["string_key"] == "value"
    
@pytest.mark.asyncio
async def test_lpushx_command(setup_handler):
    command = list_commands.LPushXCommand()
    
    #push to non-existing list
    response = await command.execute(setup_handler, ["LPUSHX", "key", "value1", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    lrange_command = list_commands.LRangeCommand()
    response = await lrange_command.execute(setup_handler, ["LRANGE", "key", "0", "-1"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    #push to existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    response = await command.execute(setup_handler, ["LPUSHX", "key", "value3"])
    assert response == ":3\r\n"
    assert setup_handler.memory["key"] == ["value3", "value2", "value1"]
    
    #push to element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LPUSHX", "string_key", "value"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_rpop_command(setup_handler):
    command = list_commands.RPopCommand()
    
    #pop from non-existing list
    response = await command.execute(setup_handler, ["RPOP", "key"])
    assert response == NIL_RESPONSE
    
    #pop from existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    response = await command.execute(setup_handler, ["RPOP", "key"])
    assert response == "$6\r\nvalue1\r\n"
    assert setup_handler.memory["key"] == ["value2"]
    
    #pop from list with one element
    response = await command.execute(setup_handler, ["RPOP", "key"])
    assert response == "$6\r\nvalue2\r\n"
    assert setup_handler.memory.get("key") == None
    
    #pop from element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["RPOP", "string_key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_rpush_command(setup_handler):
    command = list_commands.RPushCommand()
    
    #push to non-existing list    
    response = await command.execute(setup_handler, ["RPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    assert setup_handler.memory["key"] == ["value1", "value2"]
    
    #push to existing list
    response = await command.execute(setup_handler, ["RPUSH", "key", "value3"])
    assert response == ":3\r\n"
    assert setup_handler.memory["key"] == ["value1", "value2", "value3"]
    
    #push to element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["RPUSH", "string_key", "value"])
    assert response == WRONG_TYPE_RESPONSE
    assert setup_handler.memory["string_key"] == "value"
    
@pytest.mark.asyncio
async def test_rpushx_command(setup_handler):
    command = list_commands.RPushXCommand()
    
    #push to non-existing list
    response = await command.execute(setup_handler, ["RPUSHX", "key", "value1", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    lrange_command = list_commands.LRangeCommand()
    response = await lrange_command.execute(setup_handler, ["LRANGE", "key", "0", "-1"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    #push to existing list
    rpush_command = list_commands.RPushCommand()
    response = await rpush_command.execute(setup_handler, ["RPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    response = await command.execute(setup_handler, ["RPUSHX", "key", "value3"])
    assert response == ":3\r\n"
    assert setup_handler.memory["key"] == ["value1", "value2", "value3"]
    
    #push to element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["RPUSHX", "string_key", "value"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_lrange_command(setup_handler):
    command = list_commands.LRangeCommand()
    
    #range from non-existing list
    response = await command.execute(setup_handler, ["LRANGE", "key", "0", "-1"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    #range from existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2", "value3"])
    assert response == ":3\r\n"
    response = await command.execute(setup_handler, ["LRANGE", "key", "0", "-1"])
    print(response)
    assert response == "*3\r\n$6\r\nvalue3\r\n$6\r\nvalue2\r\n$6\r\nvalue1\r\n"
    
    #range from existing list with start and stop
    response = await command.execute(setup_handler, ["LRANGE", "key", "0", "1"])
    assert response == "*2\r\n$6\r\nvalue3\r\n$6\r\nvalue2\r\n"
    
    #range from existing list with negative stop
    response = await command.execute(setup_handler, ["LRANGE", "key", "0", "-2"])
    assert response == "*2\r\n$6\r\nvalue3\r\n$6\r\nvalue2\r\n"
    
    #range from existing list with negative start and stop
    response = await command.execute(setup_handler, ["LRANGE", "key", "-1", "-2"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    #range from list with stop greater than length
    response = await command.execute(setup_handler, ["LRANGE", "key", "0", "10"])
    assert response == "*3\r\n$6\r\nvalue3\r\n$6\r\nvalue2\r\n$6\r\nvalue1\r\n"
    
    #range from list with start greater than stop
    response = await command.execute(setup_handler, ["LRANGE", "key", "2", "1"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    #range from element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LRANGE", "string_key", "0", "-1"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_lindex_command(setup_handler):
    command = list_commands.LIndexCommand()
    
    #index from non-existing list
    response = await command.execute(setup_handler, ["LINDEX", "key", "0"])
    assert response == NIL_RESPONSE
    
    #index from existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2", "value3"])
    assert response == ":3\r\n"
    response = await command.execute(setup_handler, ["LINDEX", "key", "0"])
    assert response == "$6\r\nvalue3\r\n"
    
    #index from existing list with negative index
    response = await command.execute(setup_handler, ["LINDEX", "key", "-1"])
    assert response == "$6\r\nvalue1\r\n"
    
    #index from existing list with index greater than length
    response = await command.execute(setup_handler, ["LINDEX", "key", "10"])
    assert response == NIL_RESPONSE
    
    #index from element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LINDEX", "string_key", "0"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_llen_command(setup_handler):
    command = list_commands.LLenCommand()
    
    #length of non-existing list
    response = await command.execute(setup_handler, ["LLEN", "key"])
    assert response == ":0\r\n"
    
    #length of existing list
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["LLEN", "key"])
    assert response == ":3\r\n"
    
    #length of element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LLEN", "string_key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_lpop_command(setup_handler):
    command = list_commands.LPopCommand()
    
    #pop from non-existing list
    response = await command.execute(setup_handler, ["LPOP", "key"])
    assert response == NIL_RESPONSE
    
    #pop from existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    response = await command.execute(setup_handler, ["LPOP", "key"])
    assert response == "$6\r\nvalue2\r\n"
    assert setup_handler.memory["key"] == ["value1"]
    
    #pop from list with one element
    response = await command.execute(setup_handler, ["LPOP", "key"])
    assert response == "$6\r\nvalue1\r\n"
    assert setup_handler.memory.get("key") == None
    
    #pop from element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LPOP", "string_key"])
    assert response == WRONG_TYPE_RESPONSE
    
    
@pytest.mark.asyncio
async def test_lset_command(setup_handler):
    command = list_commands.LSetCommand()
    
    #set to non-existing list
    response = await command.execute(setup_handler, ["LSET", "key", "0", "value"])
    assert response == NON_INT_ERROR
    
    #set to existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value1", "value2"])
    assert response == ":2\r\n"
    response = await command.execute(setup_handler, ["LSET", "key", "0", "value3"])
    assert response == "+OK\r\n"
    assert setup_handler.memory["key"] == ["value3", "value1"]
    
    #set to existing list with negative index
    response = await command.execute(setup_handler, ["LSET", "key", "-1", "value4"])
    assert response == "+OK\r\n"
    assert setup_handler.memory["key"] == ["value3", "value4"]
    
    #set to existing list with index greater than length
    response = await command.execute(setup_handler, ["LSET", "key", "10", "value5"])
    assert response == NON_INT_ERROR
    
    #set to element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LSET", "string_key", "0", "value"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_linsert_command(setup_handler):
    command = list_commands.LInsertCommand()
    
    #insert to non-existing list
    response = await command.execute(setup_handler, ["LINSERT", "key", "BEFORE", "value1", "value2"])
    assert response == NIL_RESPONSE
    
    #insert to existing list
    lpush_command = list_commands.LPushCommand()
    response = await lpush_command.execute(setup_handler, ["LPUSH", "key", "value3", "value1"])
    assert response == ":2\r\n"
    response = await command.execute(setup_handler, ["LINSERT", "key", "BEFORE", "value3", "value2"])
    assert response == ":3\r\n"
    assert setup_handler.memory["key"] == ["value1", "value2", "value3"]
    
    #insert after last element
    response = await command.execute(setup_handler, ["LINSERT", "key", "AFTER", "value3", "value4"])
    assert response == ":4\r\n"
    assert setup_handler.memory["key"] == ["value1", "value2", "value3", "value4"]
    
    #insert before first element
    response = await command.execute(setup_handler, ["LINSERT", "key", "BEFORE", "value1", "value5"])
    assert response == ":5\r\n"
    assert setup_handler.memory["key"] == ["value5", "value1", "value2", "value3", "value4"]
    
    #insert after non-existing pivot
    response = await command.execute(setup_handler, ["LINSERT", "key", "AFTER", "value6", "value7"])
    assert response == NIL_RESPONSE
    
    #insert before non-existing pivot
    response = await command.execute(setup_handler, ["LINSERT", "key", "BEFORE", "value6", "value7"])
    assert response == NIL_RESPONSE
    
    #insert to element that is not a list
    set_command = ["SET", "string_key", "value"]
    await string_commands.SetCommand().execute(setup_handler, set_command)
    response = await command.execute(setup_handler, ["LINSERT", "string_key", "AFTER", "value", "value"])
    assert response == WRONG_TYPE_RESPONSE