import pytest
import app.commands.set_commands as commands
import app.commands.list_commands as list_commands
from app.tests.helper import setup_handler
from app.tests import helper
from app.utils.constants import EMPTY_ARRAY_RESPONSE, WRONG_TYPE_RESPONSE
from app.utils.encoding_utils import redis_array_to_list, redis_bulk_string_to_string

@pytest.mark.asyncio
async def test_sadd_command(setup_handler):
    command = commands.SAddCommand()
    
    # Test adding a single value to an empty set
    response = await command.execute(setup_handler, ["SADD", "key", "value"])
    assert response == ":1\r\n"
    assert helper.get_keys_for_test(setup_handler) == ["key"]
    assert helper.get_key_value_for_test(setup_handler, "key") == {"value"}
    
    # Test adding multiple values to an existing set
    response = await command.execute(setup_handler, ["SADD", "key", "value1", "value2", "value3"])
    assert response == ":3\r\n"
    assert helper.get_keys_for_test(setup_handler) == ["key"]
    assert helper.get_key_value_for_test(setup_handler, "key") == {"value", "value1", "value2", "value3"}
    
    # Test adding a value that already exists in the set
    response = await command.execute(setup_handler, ["SADD", "key", "value1"])
    assert response == ":0\r\n"
    assert helper.get_keys_for_test(setup_handler) == ["key"]
    assert helper.get_key_value_for_test(setup_handler, "key") == {"value", "value1", "value2", "value3"}
    
    
    # Test adding values to a set that is not of type Set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SADD", "list_key", "value6"])
    assert response == WRONG_TYPE_RESPONSE
    assert helper.get_keys_for_test(setup_handler) == ["key", "list_key"]
    assert helper.get_key_value_for_test(setup_handler, "list_key") == ["value1", "value2", "value3"]
    
    
@pytest.mark.asyncio
async def test_smembers_command(setup_handler):
    command = commands.SMembersCommand()
    
    # Test getting members of an empty set
    response = await command.execute(setup_handler, ["SMEMBERS", "key"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting members of a set with values
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SMEMBERS", "key"])
    response_arr = sorted(redis_array_to_list(response))
    assert response_arr == ["value1", "value2", "value3"]
    
    # Test getting members of a set that is not of type Set
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SMEMBERS", "list_key"])
    assert response == WRONG_TYPE_RESPONSE

@pytest.mark.asyncio
async def test_srem_command(setup_handler):
    command = commands.SRemCommand()
    
    # Test removing a value from an empty set
    response = await command.execute(setup_handler, ["SREM", "key", "value"])
    assert response == ":0\r\n"
    
    # Test removing a value from a set
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SREM", "key", "value2"])
    assert response == ":1\r\n"
    assert helper.get_keys_for_test(setup_handler) == ["key"]
    assert helper.get_key_value_for_test(setup_handler, "key") == {"value1", "value3"}
    
    # Test removing a value that doesn't exist in the set
    response = await command.execute(setup_handler, ["SREM", "key", "value2"])
    assert response == ":0\r\n"
    assert helper.get_keys_for_test(setup_handler) == ["key"]
    assert helper.get_key_value_for_test(setup_handler, "key") == {"value1", "value3"}
    
    # Test removing values from a set that is not of type Set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SREM", "list_key", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    assert helper.get_keys_for_test(setup_handler) == ["key", "list_key"]
    assert helper.get_key_value_for_test(setup_handler, "list_key") == ["value1", "value2", "value3"]
    
@pytest.mark.asyncio
async def test_sismember_command(setup_handler):
    command = commands.SIsMemberCommand()
    
    # Test checking if a value is in an empty set
    response = await command.execute(setup_handler, ["SISMEMBER", "key", "value"])
    assert response == ":0\r\n"
    
    # Test checking if a value is in a set
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SISMEMBER", "key", "value2"])
    assert response == ":1\r\n"
    
    # Test checking if a value is not in a set
    response = await command.execute(setup_handler, ["SISMEMBER", "key", "value4"])
    assert response == ":0\r\n"
    
    # Test checking if a value is in a set that is not of type Set
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SISMEMBER", "list_key", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    
    
@pytest.mark.asyncio
async def test_scard_command(setup_handler):
    command = commands.SCardCommand()
    
    # Test getting the cardinality of an empty set
    response = await command.execute(setup_handler, ["SCARD", "key"])
    assert response == ":0\r\n"
    
    # Test getting the cardinality of a set
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SCARD", "key"])
    assert response == ":3\r\n"
    
    # Test getting the cardinality of a set that is not of type Set
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SCARD", "list_key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_spop_command(setup_handler):
    command = commands.SPopCommand()
    
    # Test popping a value from an empty set
    response = await command.execute(setup_handler, ["SPOP", "key"])
    assert response == "$-1\r\n"
    
    # Test popping a value from a set
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SPOP", "key"])
    expected_values = {"value1", "value2", "value3"}
    removed = redis_bulk_string_to_string(response)
    assert removed in expected_values
    assert helper.get_keys_for_test(setup_handler) == ["key"]
    assert helper.get_key_value_for_test(setup_handler, "key") == {"value1", "value2", "value3"} - {removed}
    
    # Test popping a value from a set that is not of type Set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SPOP", "list_key"])
    assert response == WRONG_TYPE_RESPONSE
    assert helper.get_keys_for_test(setup_handler) == ["key", "list_key"]
    assert helper.get_key_value_for_test(setup_handler, "list_key") == ["value1", "value2", "value3"]
    
    
@pytest.mark.asyncio
async def test_sdiff_command(setup_handler):
    command = commands.SDiffCommand()
    
    # Test getting the difference of two empty sets
    response = await command.execute(setup_handler, ["SDIFF", "key1", "key2"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the difference of two sets
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key1", "value1", "value2", "value3"])
    await sadd_command.execute(setup_handler, ["SADD", "key2", "value2", "value3", "value4"])
    response = await command.execute(setup_handler, ["SDIFF", "key1", "key2"])
    assert response == "*1\r\n$6\r\nvalue1\r\n"
    
    # Test getting the difference of two sets where one set is empty
    response = await command.execute(setup_handler, ["SDIFF", "key1", "key3"])
    response_arr = sorted(redis_array_to_list(response))
    assert response_arr == ["value1", "value2", "value3"]
    
    # Test getting the difference of three sets
    await sadd_command.execute(setup_handler, ["SADD", "key3", "value3", "value4", "value5"])
    response = await command.execute(setup_handler, ["SDIFF", "key1", "key2", "key3"])
    assert response == "*1\r\n$6\r\nvalue1\r\n"
    
    # Test getting the difference of two sets that are not of type Set
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SDIFF", "key1", "list_key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_sunion_command(setup_handler):
    command = commands.SUnionCommand()
    
    # Test getting the union of two empty sets
    response = await command.execute(setup_handler, ["SUNION", "key1", "key2"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the union of two sets
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key1", "value1", "value2", "value3"])
    await sadd_command.execute(setup_handler, ["SADD", "key2", "value2", "value3", "value4"])
    response = await command.execute(setup_handler, ["SUNION", "key1", "key2"])
    response_arr = sorted(redis_array_to_list(response))
    assert response_arr == ["value1", "value2", "value3", "value4"]

    # Test getting the union of two sets where one set is empty
    response = await command.execute(setup_handler, ["SUNION", "key1", "key3"])
    response_arr = sorted(redis_array_to_list(response))
    assert response_arr == ["value1", "value2", "value3"]
    
    # Test getting the union of three sets
    await sadd_command.execute(setup_handler, ["SADD", "key3", "value3", "value4", "value5"])
    response = await command.execute(setup_handler, ["SUNION", "key1", "key2", "key3"])
    response_arr = sorted(redis_array_to_list(response))
    assert response_arr == ["value1", "value2", "value3", "value4", "value5"]
    
    # Test getting the union of two sets that are not of type Set
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SUNION", "key1", "list_key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_sinter_command(setup_handler):
    command = commands.SInterCommand()
    
    # Test getting the intersection of two empty sets
    response = await command.execute(setup_handler, ["SINTER", "key1", "key2"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the intersection of two sets
    sadd_command = commands.SAddCommand()
    await sadd_command.execute(setup_handler, ["SADD", "key1", "value1", "value2", "value3"])
    await sadd_command.execute(setup_handler, ["SADD", "key2", "value2", "value3", "value4"])
    response = await command.execute(setup_handler, ["SINTER", "key1", "key2"])
    print(f"response {response}")
    response_arr = sorted(redis_array_to_list(response))
    print(f"response arr {response_arr}")
    assert response_arr == ["value2", "value3"]
    
    # Test getting the intersection of two sets where one set is empty
    response = await command.execute(setup_handler, ["SINTER", "key1", "key3"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the intersection of three sets
    await sadd_command.execute(setup_handler, ["SADD", "key3", "value3", "value4", "value5"])
    response = await command.execute(setup_handler, ["SINTER", "key1", "key2", "key3"])
    assert response == "*1\r\n$6\r\nvalue3\r\n"
    
    # Test getting the intersection of two sets that are not of type Set
    lpush_command = list_commands.LPushCommand()
    await lpush_command.execute(setup_handler, ["LPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["SINTER", "key1", "list_key"])
    assert response == WRONG_TYPE_RESPONSE
    
    
    