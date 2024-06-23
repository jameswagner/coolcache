import pytest
from app.utils.constants import EMPTY_ARRAY_RESPONSE, FLOAT_ERROR_MESSAGE, NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE
import app.commands.sorted_set_commands as commands
import app.commands.list_commands as list_commands
import app.commands.commands as general_commands
from app.tests.helper import setup_handler
from app.tests.helper import get_keys_for_test, get_key_value_for_test

@pytest.mark.asyncio
async def test_zadd_command(setup_handler):
    command = commands.ZAddCommand()
    
    # Test adding a single member with score to an empty sorted set
    response = await command.execute(setup_handler, ["ZADD", "key", "1", "member"])
    assert response == ":1\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key").as_list() == [(1.0, "member")]
    
    await general_commands.DelCommand().execute(setup_handler, ["DEL", "key"])
    
    # Test adding multiple members with scores to a sorted set
    response = await command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    assert response == ":3\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    vals = get_key_value_for_test(setup_handler, "key").as_list()
    print(vals)
    assert get_key_value_for_test(setup_handler, "key").as_list() ==  [(1.0, "member1"), (2.0, "member2"), (3.0, "member3")]
    
    # Test adding a member with score that already exists in the sorted set
    response = await command.execute(setup_handler, ["ZADD", "key", "5", "member1"])
    assert response == ":0\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key").as_list() ==  [(2, "member2"), (3, "member3"), (5, "member1")]
    
    # Test NX option - only add keys that do not already exist
    response = await command.execute(setup_handler, ["ZADD", "key", "NX", "1", "member1", "2", "member2", "4", "member4"])
    assert response == ":1\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key").as_list() ==  [(2, "member2"), (3, "member3"), (4, "member4"), (5, "member1")]
    
    # Test XX option - only update keys that already exist
    response = await command.execute(setup_handler, ["ZADD", "key", "XX", "-1", "member1", "-2", "member2", "5", "member5"])
    assert response == ":0\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key").as_list() ==  [(-2, "member2"), (-1, "member1"), (3, "member3"), (4, "member4")]
    
    # Test GT option - only update keys if the new score is greater than the old score, or member does not exist
    response = await command.execute(setup_handler, ["ZADD", "key", "GT", "1", "member1", "2", "member2", "2", "member3", "6", "member6"])
    assert response == ":1\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key").as_list() ==  [(1, "member1"), (2, "member2"), (3, "member3"), (4, "member4"), (6, "member6")]
    
    # Test LT option - only update keys if the new score is less than the old score, or member does not exist
    response = await command.execute(setup_handler, ["ZADD", "key", "LT", "3", "member1", "2", "member2", "2", "member3", "7", "member7"])
    assert response == ":1\r\n"
    assert get_keys_for_test(setup_handler) == ["key"]
    assert get_key_value_for_test(setup_handler, "key").as_list() ==  [(1, "member1"), (2, "member2"), (2, "member3"), (4, "member4"), (6, "member6"), (7, "member7")]
    
    # Test CH option - return the number of elements changed (added or updated)
    response = await command.execute(setup_handler, ["ZADD", "key", "CH", "0", "member1", "2", "member2", "3", "member3", "7", "member7", "8", "member8"])
    assert response == ":3\r\n"

    # Test adding members with scores to a sorted set that is not of type Sorted Set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZADD", "list_key", "1", "member6"])
    assert response == WRONG_TYPE_RESPONSE
    assert set(get_keys_for_test(setup_handler)) == set(["key", "list_key"])
    assert get_key_value_for_test(setup_handler, "list_key") == ["value1", "value2", "value3"]
    
@pytest.mark.asyncio
async def test_zcard_command(setup_handler):
    command = commands.ZCardCommand()
    
    # Test getting the cardinality of an empty sorted set
    response = await command.execute(setup_handler, ["ZCARD", "key"])
    assert response == ":0\r\n"
    
    # Test getting the cardinality of a sorted set with members
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZCARD", "key"])
    assert response == ":3\r\n"
    
    # Test getting the cardinality of a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZCARD", "list_key"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_zrank_command(setup_handler):
    command = commands.ZRankCommand()
    
    # Test getting the rank of a member in an empty sorted set
    response = await command.execute(setup_handler, ["ZRANK", "key", "member"])
    assert response == NOT_FOUND_RESPONSE
    
    # Test getting the rank of a member in a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZRANK", "key", "member2"])
    assert response == ":1\r\n"
    
    # Test getting the rank of a member that does not exist in a sorted set
    response = await command.execute(setup_handler, ["ZRANK", "key", "non_existent_member"])
    assert response == NOT_FOUND_RESPONSE
    
    # Test getting the rank of a member in a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZRANK", "list_key", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_zrange_command(setup_handler):
    command = commands.ZRangeCommand()
    
    # Test getting the range of members in an empty sorted set
    response = await command.execute(setup_handler, ["ZRANGE", "key", "0", "-1"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the range of members in a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZRANGE", "key", "0", "-1"])
    assert response == "*3\r\n$7\r\nmember1\r\n$7\r\nmember2\r\n$7\r\nmember3\r\n"
    
    # Test getting the range of members in a sorted set with a start and end index
    response = await command.execute(setup_handler, ["ZRANGE", "key", "1", "2"])
    assert response == "*2\r\n$7\r\nmember2\r\n$7\r\nmember3\r\n"
    
    # Test getting the range of members in a sorted set with a start and end index that are out of range
    response = await command.execute(setup_handler, ["ZRANGE", "key", "4", "5"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the range of members in a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZRANGE", "list_key", "0", "-1"])
    assert response == WRONG_TYPE_RESPONSE
    
    
@pytest.mark.asyncio
async def test_zscore_command(setup_handler):
    command = commands.ZScoreCommand()
    
    # Test getting the score of a member in an empty sorted set
    response = await command.execute(setup_handler, ["ZSCORE", "key", "member"])
    assert response == NOT_FOUND_RESPONSE
    
    # Test getting the score of a member in a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZSCORE", "key", "member2"])
    assert response == ":2\r\n" or response == ":2.0\r\n"
    
    # Test getting the score of a member that does not exist in a sorted set
    response = await command.execute(setup_handler, ["ZSCORE", "key", "non_existent_member"])
    assert response == NOT_FOUND_RESPONSE
    
    # Test getting the score of a member in a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZSCORE", "list_key", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_zrem_command(setup_handler):
    command = commands.ZRemCommand()
    
    # Test removing a member from an empty sorted set
    response = await command.execute(setup_handler, ["ZREM", "key", "member"])
    assert response == ":0\r\n"
    
    # Test removing a member from a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZREM", "key", "member2"])
    assert response == ":1\r\n"
    assert get_key_value_for_test(setup_handler, "key").as_list() == [(1, "member1"), (3, "member3")]
    
    # Test removing a member that does not exist from a sorted set
    response = await command.execute(setup_handler, ["ZREM", "key", "non_existent_member"])
    assert response == ":0\r\n"
    
    # Test removing a member from a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZREM", "list_key", "value2"])
    assert response == WRONG_TYPE_RESPONSE


@pytest.mark.asyncio
async def test_zrev_rank_command(setup_handler):
    command = commands.ZRevRankCommand()
    
    # Test getting the reverse rank of a member in an empty sorted set
    response = await command.execute(setup_handler, ["ZREVRANK", "key", "member"])
    assert response == NOT_FOUND_RESPONSE
    
    # Test getting the reverse rank of a member in a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZREVRANK", "key", "member2"])
    assert response == ":1\r\n"
    
    # Test getting the reverse rank of a member that does not exist in a sorted set
    response = await command.execute(setup_handler, ["ZREVRANK", "key", "non_existent_member"])
    assert response == NOT_FOUND_RESPONSE
    
    # Test getting the reverse rank of a member in a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZREVRANK", "list_key", "value2"])
    assert response == WRONG_TYPE_RESPONSE
    
@pytest.mark.asyncio
async def test_zcount_command(setup_handler):
    command = commands.ZCountCommand()
    
    # Test getting the count of members in an empty sorted set
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "-inf", "+inf"])
    assert response == ":0\r\n"
    
    # Test getting the count of members in a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "-inf", "+inf"])
    assert response == ":3\r\n"
    
    # Test getting the count of members in a sorted set with a min and max score
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "2", "3"])
    assert response == ":2\r\n"
    
    # Test getting the count of members in a sorted set with a min and max score that are out of range
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "4", "5"])
    assert response == ":0\r\n"
    
    # Test getting the count of members in a sorted set with a min and max score that are equal
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "2", "2"])
    assert response == ":1\r\n"
    
    # Test getting the count of members in a sorted set with a min score that is not a float
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "string", "2"])
    assert response == FLOAT_ERROR_MESSAGE
    
    # Test getting the count of members in a sorted set with a max score that is not a float
    response = await command.execute(setup_handler, ["ZCOUNT", "key", "2", "string"])
    assert response == FLOAT_ERROR_MESSAGE
    
    # Test getting the count of members in a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZCOUNT", "list_key", "-inf", "+inf"])
    assert response == WRONG_TYPE_RESPONSE
    
    
async def test_zrangebyscore_command(setup_handler):
    command = commands.ZRangeByScoreCommand()
    
    # Test getting the range of members in an empty sorted set
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "-inf", "+inf"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the range of members in a sorted set
    zadd_command = commands.ZAddCommand()
    await zadd_command.execute(setup_handler, ["ZADD", "key", "1", "member1", "2", "member2", "3", "member3"])
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "-inf", "+inf"])
    assert response == "*3\r\n$7\r\nmember1\r\n$7\r\nmember2\r\n$7\r\nmember3\r\n"
    
    # Test getting the range of members in a sorted set with a min and max score
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "2", "3"])
    assert response == "*2\r\n$7\r\nmember2\r\n$7\r\nmember3\r\n"
    
    # Test getting the range of members in a sorted set with a min and max score that are out of range
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "4", "5"])
    assert response == EMPTY_ARRAY_RESPONSE
    
    # Test getting the range of members in a sorted set with a min and max score that are equal
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "2", "2"])
    assert response == "*1\r\n$7\r\nmember2\r\n"
    
    # Test getting the range of members in a sorted set with a min score that is not a float
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "string", "2"])
    assert response == FLOAT_ERROR_MESSAGE
    
    # Test getting the range of members in a sorted set with a max score that is not a float
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "key", "2", "string"])
    assert response == FLOAT_ERROR_MESSAGE
    
    # Test getting the range of members in a key that is not a sorted set
    rpush_command = list_commands.RPushCommand()
    await rpush_command.execute(setup_handler, ["RPUSH", "list_key", "value1", "value2", "value3"])
    response = await command.execute(setup_handler, ["ZRANGEBYSCORE", "list_key", "-inf", "+inf"])
    assert response == WRONG_TYPE_RESPONSE
    
