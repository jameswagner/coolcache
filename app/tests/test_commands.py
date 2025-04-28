import pytest
import os
import asyncio
import tempfile
from pathlib import Path
import time
from app.commands import commands, string_commands, hash_map_commands, list_commands, set_commands, sorted_set_commands
from app.tests.helper import get_key_value_for_test, get_keys_for_test, setup_handler
from app.utils.rdb_parser import parse_redis_file



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

@pytest.fixture
def temp_rdb_path():
    """Create a temporary file path for RDB tests."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.rdb') as temp_file:
        temp_path = temp_file.name
    
    yield temp_path
    
    # Clean up after the test
    if os.path.exists(temp_path):
        os.unlink(temp_path)

@pytest.mark.asyncio
async def test_save_command(setup_handler, temp_rdb_path):
    # Set up initial data
    handler = setup_handler
    
    # Update handler's server config
    handler.server.config["dir"] = os.path.dirname(temp_rdb_path)
    handler.server.config["dbfilename"] = os.path.basename(temp_rdb_path)
    
    # Add a string value
    string_cmd = string_commands.SetCommand()
    await string_cmd.execute(handler, ["SET", "string-key", "string-value"])
    
    # Add a list
    list_cmd = list_commands.LPushCommand()
    await list_cmd.execute(handler, ["LPUSH", "list-key", "item1", "item2", "item3"])
    
    # Add a set
    set_cmd = set_commands.SAddCommand()
    await set_cmd.execute(handler, ["SADD", "set-key", "member1", "member2", "member3"])
    
    # Add a hash
    hash_cmd = hash_map_commands.HSetCommand()
    await hash_cmd.execute(handler, ["HSET", "hash-key", "field1", "value1", "field2", "value2"])
    
    # Add a sorted set
    zset_cmd = sorted_set_commands.ZAddCommand()
    await zset_cmd.execute(handler, ["ZADD", "zset-key", "1.5", "member1", "2.5", "member2"])
    
    # Execute the SAVE command
    save_cmd = commands.SaveCommand()
    response = await save_cmd.execute(handler, ["SAVE"])
    
    # Check the response
    assert response == "+OK\r\n"
    
    # Verify the file exists
    assert os.path.exists(temp_rdb_path)
    
    # Parse the file to verify contents
    parsed_data, parsed_expires = parse_redis_file(temp_rdb_path)
    
    # Verify data was saved correctly
    assert "string-key" in parsed_data
    assert parsed_data["string-key"] == "string-value"
    
    assert "list-key" in parsed_data
    assert len(parsed_data["list-key"]) == 3
    assert "item1" in parsed_data["list-key"]
    assert "item2" in parsed_data["list-key"]
    assert "item3" in parsed_data["list-key"]
    
    assert "set-key" in parsed_data
    assert len(parsed_data["set-key"]) == 3
    assert "member1" in parsed_data["set-key"]
    assert "member2" in parsed_data["set-key"]
    assert "member3" in parsed_data["set-key"]
    
    assert "hash-key" in parsed_data
    assert len(parsed_data["hash-key"]) == 2
    assert parsed_data["hash-key"]["field1"] == "value1"
    assert parsed_data["hash-key"]["field2"] == "value2"
    
    assert "zset-key" in parsed_data
    assert len(parsed_data["zset-key"]) == 2
    # Note: zset is stored as a list of (member, score) tuples in our parser
    # Members might be returned as bytes, so we need to handle both cases
    zset_dict = {}
    for item in parsed_data["zset-key"]:
        if isinstance(item[0], bytes):
            zset_dict[item[0].decode('utf-8')] = item[1]
        else:
            zset_dict[item[0]] = item[1]
    
    assert "member1" in zset_dict
    assert "member2" in zset_dict
    assert zset_dict["member1"] == 1.5
    assert zset_dict["member2"] == 2.5

@pytest.mark.asyncio
async def test_bgsave_command(setup_handler, temp_rdb_path):
    # Set up initial data
    handler = setup_handler
    
    # Update handler's server config
    handler.server.config["dir"] = os.path.dirname(temp_rdb_path)
    handler.server.config["dbfilename"] = os.path.basename(temp_rdb_path)
    
    # Add some test data
    string_cmd = string_commands.SetCommand()
    await string_cmd.execute(handler, ["SET", "bg-key", "bg-value"])
    
    # Execute the BGSAVE command
    bgsave_cmd = commands.BGSaveCommand()
    response = await bgsave_cmd.execute(handler, ["BGSAVE"])
    
    # Check the response
    assert response == "+Background saving started\r\n"
    
    # Wait a moment for the background task to complete
    await asyncio.sleep(0.5)
    
    # Verify the file exists
    assert os.path.exists(temp_rdb_path)
    
    # Parse the file to verify contents
    parsed_data, parsed_expires = parse_redis_file(temp_rdb_path)
    
    # Verify data was saved correctly
    assert "bg-key" in parsed_data
    assert parsed_data["bg-key"] == "bg-value"

@pytest.mark.asyncio
async def test_save_with_expiry(setup_handler, temp_rdb_path):
    # Set up initial data
    handler = setup_handler
    
    # Update handler's server config
    handler.server.config["dir"] = os.path.dirname(temp_rdb_path)
    handler.server.config["dbfilename"] = os.path.basename(temp_rdb_path)
    
    # Add a value with expiry
    string_cmd = string_commands.SetCommand()
    await string_cmd.execute(handler, ["SET", "expiry-key", "expiry-value", "PX", "60000"])  # 60 seconds
    
    # Execute the SAVE command
    save_cmd = commands.SaveCommand()
    response = await save_cmd.execute(handler, ["SAVE"])
    
    # Check the response
    assert response == "+OK\r\n"
    
    # Parse the file to verify contents
    parsed_data, parsed_expires = parse_redis_file(temp_rdb_path)
    
    # Verify data and expiry were saved correctly
    assert "expiry-key" in parsed_data
    assert parsed_data["expiry-key"] == "expiry-value"
    assert "expiry-key" in parsed_expires
    # The expiry time should be in the future
    current_time = int(time.time())
    assert parsed_expires["expiry-key"] > current_time

@pytest.mark.asyncio
async def test_lastsave_command(setup_handler, temp_rdb_path):
    # Set up initial data
    handler = setup_handler
    
    # Update handler's server config
    handler.server.config["dir"] = os.path.dirname(temp_rdb_path)
    handler.server.config["dbfilename"] = os.path.basename(temp_rdb_path)
    
    # Add a value
    string_cmd = string_commands.SetCommand()
    await string_cmd.execute(handler, ["SET", "lastsave-key", "lastsave-value"])
    
    # Get initial last save time
    lastsave_cmd = commands.LastSaveCommand()
    initial_response = await lastsave_cmd.execute(handler, ["LASTSAVE"])
    
    # The initial timestamp might be 0 if no save has happened yet
    initial_timestamp_str = initial_response.strip()[1:-2]
    if initial_timestamp_str:
        initial_timestamp = int(initial_timestamp_str)
    else:
        initial_timestamp = 0
    
    # Execute the SAVE command
    save_cmd = commands.SaveCommand()
    await save_cmd.execute(handler, ["SAVE"])
    
    # Get updated last save time
    updated_response = await lastsave_cmd.execute(handler, ["LASTSAVE"])
    updated_timestamp = int(updated_response.strip()[1:-2])
    
    # The updated timestamp should be greater than or equal to the initial one
    assert updated_timestamp >= initial_timestamp

@pytest.mark.asyncio
async def test_config_save_command(setup_handler):
    # Set up handler
    handler = setup_handler
    
    # Execute CONFIG GET command for save
    config_cmd = commands.ConfigCommand()
    response = await config_cmd.execute(handler, ["CONFIG", "GET", "save"])
    
    # Check that it returns the default save configuration
    assert "save" in response
    assert "900 1 300 10 60 10000" in response
    
    # Set a new save configuration
    new_config = "60 1000"
    set_response = await config_cmd.execute(handler, ["CONFIG", "SET", "save", new_config])
    assert set_response == "+OK\r\n"
    
    # Verify the new configuration
    get_response = await config_cmd.execute(handler, ["CONFIG", "GET", "save"])
    assert new_config in get_response
