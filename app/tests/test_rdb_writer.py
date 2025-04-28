import struct
import pytest
import tempfile
import os
from io import BytesIO
from pathlib import Path
import time

from app.utils.rdb_writer import (
    encode_length,
    encode_string,
    encode_set_value,
    encode_list_value,
    encode_hash_value,
    encode_zset_value,
    encode_value,
    encode_key_value_pair,
    encode_key_value_pair_with_expiry_ms,
    encode_key_value_pair_with_expiry_sec,
    write_header,
    write_database_selector,
    write_footer,
    write_redis_file,
    REDIS_MAGIC,
    RDB_VERSION_CURRENT,
    RDB_OPCODE_EOF,
    RDB_OPCODE_SELECTDB,
    RDB_OPCODE_EXPIRY_MS,
    RDB_OPCODE_EXPIRY_SEC,
    RDB_OPCODE_RESIZEDB,
    RDB_TYPE_STRING,
    RDB_TYPE_LIST,
    RDB_TYPE_SET,
    RDB_TYPE_HASH,
    RDB_TYPE_ZSET
)

# Import the Redis parser to verify our writer output
from app.utils.rdb_parser import parse_redis_file

def test_encode_length_small():
    """Test encoding a small length value (0-63)."""
    # Test with value 42
    result = encode_length(42)
    # Should be a single byte with value 42 (00101010)
    assert result == bytes([42])
    assert len(result) == 1

def test_encode_length_medium():
    """Test encoding a medium length value (64-16383)."""
    # Test with value 1000 (0x03E8)
    result = encode_length(1000)
    # Should be two bytes: 01000011 11101000
    high_bits = 0x40 | (1000 >> 8)  # 0x40 | 0x03 = 0x43
    low_bits = 1000 & 0xFF  # 0xE8
    assert result == bytes([high_bits, low_bits])
    assert len(result) == 2

def test_encode_length_large():
    """Test encoding a large length value (16384+)."""
    # Test with value 100000
    result = encode_length(100000)
    # Should be 5 bytes: 10000000 + 4 bytes (little endian)
    expected = b'\x80' + struct.pack("<I", 100000)
    assert result == expected
    assert len(result) == 5

def test_encode_length_negative():
    """Test that encoding a negative length raises ValueError."""
    with pytest.raises(ValueError):
        encode_length(-1)

def test_encode_string_str():
    """Test encoding a Python string."""
    test_string = "hello world"
    result = encode_string(test_string)
    
    # Expected: length byte (11) followed by UTF-8 bytes of the string
    expected_length = bytes([len(test_string)])  # 11 fits in a single byte
    expected = expected_length + test_string.encode('utf-8')
    
    assert result == expected
    assert result[0] == len(test_string)  # First byte should be the length
    assert result[1:] == test_string.encode('utf-8')  # Rest should be the string bytes

def test_encode_string_bytes():
    """Test encoding bytes directly."""
    test_bytes = b"hello world"
    result = encode_string(test_bytes)
    
    # Expected: length byte (11) followed by the bytes
    expected_length = bytes([len(test_bytes)])
    expected = expected_length + test_bytes
    
    assert result == expected
    assert result[0] == len(test_bytes)
    assert result[1:] == test_bytes

def test_encode_string_long():
    """Test encoding a long string that requires multi-byte length encoding."""
    # Create a string longer than 63 characters (requiring 2-byte length encoding)
    long_string = "x" * 1000
    result = encode_string(long_string)
    
    # Expected: 2 length bytes for 1000 followed by string bytes
    expected_length = encode_length(1000)
    expected = expected_length + long_string.encode('utf-8')
    
    assert result.startswith(expected_length)
    assert result[len(expected_length):] == long_string.encode('utf-8')

def test_encode_set_value_empty():
    """Test encoding an empty set."""
    test_set = set()
    result = encode_set_value(test_set)
    
    # Expected: just the length (0)
    expected = encode_length(0)
    assert result == expected

def test_encode_set_value_simple():
    """Test encoding a set with simple string elements."""
    test_set = {"item1", "item2", "item3"}
    result = encode_set_value(test_set)
    
    # Expected: length (3) followed by encoded strings
    # Order is not guaranteed in sets, so we need to check differently
    assert result.startswith(encode_length(len(test_set)))
    
    # The result should contain each encoded item
    result_bytes = result[len(encode_length(len(test_set))):]  # Skip the length prefix
    
    # We can't directly check the order, but we can verify each item is included
    for item in test_set:
        encoded_item = encode_string(item)
        # Find the encoded item in the result bytes
        assert encoded_item in result_bytes
    
    # Verify the total length is correct
    expected_total_length = len(encode_length(len(test_set)))
    for item in test_set:
        expected_total_length += len(encode_string(item))
    assert len(result) == expected_total_length

def test_encode_set_value_mixed_types():
    """Test encoding a set with mixed string and bytes elements."""
    test_set = {"string item", b"bytes item"}
    result = encode_set_value(test_set)
    
    # Verify length prefix
    assert result.startswith(encode_length(len(test_set)))
    
    # The result should contain each encoded item
    result_bytes = result[len(encode_length(len(test_set))):]  # Skip the length prefix
    
    # We can't directly check the order, but we can verify each item is included
    for item in test_set:
        encoded_item = encode_string(item)
        # Find the encoded item in the result bytes
        assert encoded_item in result_bytes
    
    # Verify the total length is correct
    expected_total_length = len(encode_length(len(test_set)))
    for item in test_set:
        expected_total_length += len(encode_string(item))
    assert len(result) == expected_total_length

def test_encode_list_value_empty():
    """Test encoding an empty list."""
    test_list = []
    result = encode_list_value(test_list)
    
    # Expected: just the length (0)
    expected = encode_length(0)
    assert result == expected

def test_encode_list_value_simple():
    """Test encoding a list with simple string elements."""
    test_list = ["item1", "item2", "item3"]
    result = encode_list_value(test_list)
    
    # Expected: length (3) followed by encoded strings in order
    expected = encode_length(len(test_list))
    for item in test_list:
        expected += encode_string(item)
    
    assert result == expected
    
    # Check components individually
    assert result.startswith(encode_length(len(test_list)))
    
    # Check the order of elements
    result_bytes = result[len(encode_length(len(test_list))):]
    position = 0
    for item in test_list:
        encoded_item = encode_string(item)
        assert result_bytes[position:position+len(encoded_item)] == encoded_item
        position += len(encoded_item)

def test_encode_list_value_mixed_types():
    """Test encoding a list with mixed string and bytes elements."""
    test_list = ["string item", b"bytes item"]
    result = encode_list_value(test_list)
    
    # Expected: length (2) followed by encoded strings in order
    expected = encode_length(len(test_list))
    for item in test_list:
        expected += encode_string(item)
    
    assert result == expected
    
    # Check components individually
    assert result.startswith(encode_length(len(test_list)))
    
    # Check the order of elements
    result_bytes = result[len(encode_length(len(test_list))):]
    position = 0
    for item in test_list:
        encoded_item = encode_string(item)
        assert result_bytes[position:position+len(encoded_item)] == encoded_item
        position += len(encoded_item)

def test_encode_hash_value_empty():
    """Test encoding an empty hash."""
    test_hash = {}
    result = encode_hash_value(test_hash)
    
    # Expected: just the length (0)
    expected = encode_length(0)
    assert result == expected

def test_encode_hash_value_simple():
    """Test encoding a hash with simple string keys and values."""
    test_hash = {"field1": "value1", "field2": "value2"}
    result = encode_hash_value(test_hash)
    
    # Expected: length (2) followed by encoded field-value pairs
    # Order is not guaranteed in dicts, so we need to check differently
    assert result.startswith(encode_length(len(test_hash)))
    
    # The result should contain each encoded field-value pair
    result_bytes = result[len(encode_length(len(test_hash))):]  # Skip the length prefix
    
    # Check that each field-value pair is included
    # This is a bit tricky because we need to find pairs rather than individual items
    for field, value in test_hash.items():
        encoded_field = encode_string(field)
        encoded_value = encode_string(value)
        
        # Find field in result
        field_pos = result_bytes.find(encoded_field)
        assert field_pos != -1, f"Field {field} not found in encoded hash"
        
        # Value should come right after field
        value_pos = field_pos + len(encoded_field)
        assert result_bytes[value_pos:value_pos+len(encoded_value)] == encoded_value, \
            f"Value {value} not found after field {field}"
    
    # Verify the total length is correct
    expected_total_length = len(encode_length(len(test_hash)))
    for field, value in test_hash.items():
        expected_total_length += len(encode_string(field))
        expected_total_length += len(encode_string(value))
    assert len(result) == expected_total_length

def test_encode_hash_value_mixed_types():
    """Test encoding a hash with mixed string and bytes keys and values."""
    test_hash = {"string_field": b"bytes_value", b"bytes_field": "string_value"}
    result = encode_hash_value(test_hash)
    
    # Verify length prefix
    assert result.startswith(encode_length(len(test_hash)))
    
    # The result should contain each encoded field-value pair
    result_bytes = result[len(encode_length(len(test_hash))):]  # Skip the length prefix
    
    # Check that each field-value pair is included
    for field, value in test_hash.items():
        encoded_field = encode_string(field)
        encoded_value = encode_string(value)
        
        # Find field in result
        field_pos = result_bytes.find(encoded_field)
        assert field_pos != -1, f"Field {field} not found in encoded hash"
        
        # Value should come right after field
        value_pos = field_pos + len(encoded_field)
        assert result_bytes[value_pos:value_pos+len(encoded_value)] == encoded_value, \
            f"Value {value} not found after field {field}"
    
    # Verify the total length is correct
    expected_total_length = len(encode_length(len(test_hash)))
    for field, value in test_hash.items():
        expected_total_length += len(encode_string(field))
        expected_total_length += len(encode_string(value))
    assert len(result) == expected_total_length

def test_encode_zset_value_empty():
    """Test encoding an empty sorted set."""
    test_zset = []
    result = encode_zset_value(test_zset)
    
    # Expected: just the length (0)
    expected = encode_length(0)
    assert result == expected

def test_encode_zset_value_simple():
    """Test encoding a sorted set with simple string members and float scores."""
    test_zset = [("member1", 1.5), ("member2", 2.5)]
    result = encode_zset_value(test_zset)
    
    # Expected: length (2) followed by encoded member-score pairs in order
    expected = encode_length(len(test_zset))
    for member, score in test_zset:
        expected += encode_string(member)
        expected += struct.pack('<d', score)
    
    assert result == expected
    
    # Check components individually
    assert result.startswith(encode_length(len(test_zset)))
    
    # Check the order of elements
    result_bytes = result[len(encode_length(len(test_zset))):]
    position = 0
    for member, score in test_zset:
        encoded_member = encode_string(member)
        encoded_score = struct.pack('<d', score)
        
        # Check member
        assert result_bytes[position:position+len(encoded_member)] == encoded_member
        position += len(encoded_member)
        
        # Check score (8 bytes double)
        assert result_bytes[position:position+8] == encoded_score
        position += 8

def test_encode_zset_value_mixed_types():
    """Test encoding a sorted set with mixed string and bytes members."""
    test_zset = [("string_member", 1.0), (b"bytes_member", 2.0)]
    result = encode_zset_value(test_zset)
    
    # Expected: length (2) followed by encoded member-score pairs in order
    expected = encode_length(len(test_zset))
    for member, score in test_zset:
        expected += encode_string(member)
        expected += struct.pack('<d', score)
    
    assert result == expected
    
    # Check components individually
    assert result.startswith(encode_length(len(test_zset)))
    
    # Check the order of elements
    result_bytes = result[len(encode_length(len(test_zset))):]
    position = 0
    for member, score in test_zset:
        encoded_member = encode_string(member)
        encoded_score = struct.pack('<d', score)
        
        # Check member
        assert result_bytes[position:position+len(encoded_member)] == encoded_member
        position += len(encoded_member)
        
        # Check score (8 bytes double)
        assert result_bytes[position:position+8] == encoded_score
        position += 8

def test_encode_value_string():
    """Test encoding a string value with type detection."""
    test_string = "test string"
    value_type, encoded_value = encode_value(test_string)
    
    assert value_type == RDB_TYPE_STRING
    assert encoded_value == encode_string(test_string)

def test_encode_value_list():
    """Test encoding a list value with type detection."""
    test_list = ["item1", "item2"]
    value_type, encoded_value = encode_value(test_list)
    
    assert value_type == RDB_TYPE_LIST
    assert encoded_value == encode_list_value(test_list)

def test_encode_value_zset():
    """Test encoding a sorted set value with type detection."""
    test_zset = [("member1", 1.5), ("member2", 2.5)]
    value_type, encoded_value = encode_value(test_zset)
    
    assert value_type == RDB_TYPE_ZSET
    assert encoded_value == encode_zset_value(test_zset)

def test_encode_value_set():
    """Test encoding a set value with type detection."""
    test_set = {"item1", "item2"}
    value_type, encoded_value = encode_value(test_set)
    
    assert value_type == RDB_TYPE_SET
    assert encoded_value == encode_set_value(test_set)

def test_encode_value_hash():
    """Test encoding a hash value with type detection."""
    test_hash = {"field1": "value1", "field2": "value2"}
    value_type, encoded_value = encode_value(test_hash)
    
    assert value_type == RDB_TYPE_HASH
    assert encoded_value == encode_hash_value(test_hash)

def test_encode_value_unsupported():
    """Test encoding an unsupported value type raises ValueError."""
    with pytest.raises(ValueError):
        encode_value(123)  # Plain integers are not supported

def test_encode_key_value_pair_string():
    """Test encoding a key-value pair with string value."""
    key = "test-key"
    value = "test-value"
    result = encode_key_value_pair(key, value)
    
    # Expected: value_type + encoded_key + encoded_value
    expected = RDB_TYPE_STRING + encode_string(key) + encode_string(value)
    assert result == expected

def test_encode_key_value_pair_list():
    """Test encoding a key-value pair with list value."""
    key = "list-key"
    value = ["item1", "item2"]
    result = encode_key_value_pair(key, value)
    
    # Expected: value_type + encoded_key + encoded_value
    expected = RDB_TYPE_LIST + encode_string(key) + encode_list_value(value)
    assert result == expected

def test_encode_key_value_pair_with_expiry_ms_string():
    """Test encoding a key-value pair with millisecond expiry and string value."""
    key = "expiring-key"
    value = "expiring-value"
    expiry_time_ms = 1623456789000  # Example timestamp in milliseconds
    
    result = encode_key_value_pair_with_expiry_ms(key, value, expiry_time_ms)
    
    # Expected: EXPIRY_MS + expiry_time + value_type + encoded_key + encoded_value
    expected = RDB_OPCODE_EXPIRY_MS
    expected += struct.pack('<Q', expiry_time_ms)
    expected += RDB_TYPE_STRING
    expected += encode_string(key)
    expected += encode_string(value)
    
    assert result == expected

def test_encode_key_value_pair_with_expiry_sec_string():
    """Test encoding a key-value pair with second expiry and string value."""
    key = "expiring-key"
    value = "expiring-value"
    expiry_time_sec = 1623456789  # Example timestamp in seconds
    
    result = encode_key_value_pair_with_expiry_sec(key, value, expiry_time_sec)
    
    # Expected: EXPIRY_SEC + expiry_time + value_type + encoded_key + encoded_value
    expected = RDB_OPCODE_EXPIRY_SEC
    expected += struct.pack('<I', expiry_time_sec)
    expected += RDB_TYPE_STRING
    expected += encode_string(key)
    expected += encode_string(value)
    
    assert result == expected

def test_write_header():
    """Test writing the RDB file header."""
    # Create a BytesIO to write to
    file = BytesIO()
    
    # Write the header
    write_header(file)
    
    # Check the result
    result = file.getvalue()
    expected = REDIS_MAGIC + RDB_VERSION_CURRENT
    assert result == expected

def test_write_database_selector():
    """Test writing a database selector."""
    # Create a BytesIO to write to
    file = BytesIO()
    
    # Write the database selector for DB 0
    write_database_selector(file, 0)
    
    # Check the result
    result = file.getvalue()
    expected = RDB_OPCODE_SELECTDB + bytes([0])
    assert result == expected

def test_write_database_selector_with_size():
    """Test writing a database selector with database size information."""
    # Create a BytesIO to write to
    file = BytesIO()
    
    # Write the database selector for DB 0 with 10 keys, 2 with expiry
    write_database_selector(file, 0, 10, 2)
    
    # Check the result
    result = file.getvalue()
    expected = RDB_OPCODE_SELECTDB + bytes([0]) + RDB_OPCODE_RESIZEDB + encode_length(10) + encode_length(2)
    assert result == expected

def test_write_footer():
    """Test writing the RDB file footer."""
    # Create a BytesIO to write to
    file = BytesIO()
    
    # Write the footer
    write_footer(file)
    
    # Check the result
    result = file.getvalue()
    expected = RDB_OPCODE_EOF
    assert result == expected

@pytest.fixture
def temp_rdb_path():
    """Create a temporary file path for RDB tests."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.rdb') as temp_file:
        temp_path = temp_file.name
    
    yield temp_path
    
    # Clean up after the test
    if os.path.exists(temp_path):
        os.unlink(temp_path)

def test_write_redis_file_simple(temp_rdb_path):
    """Test writing a simple Redis database to an RDB file."""
    # Create test data
    data = {
        "string-key": "string-value",
        "list-key": ["item1", "item2", "item3"],
        "set-key": {"member1", "member2", "member3"},
        "hash-key": {"field1": "value1", "field2": "value2"}
    }
    
    # Write the RDB file
    write_redis_file(temp_rdb_path, data)
    
    # Verify the file exists
    assert os.path.exists(temp_rdb_path)
    
    # Parse the file with our parser to verify the contents
    parsed_data, parsed_expires = parse_redis_file(temp_rdb_path)
    
    # Verify the parsed data
    assert "string-key" in parsed_data
    assert parsed_data["string-key"] == "string-value"
    
    assert "list-key" in parsed_data
    assert len(parsed_data["list-key"]) == 3
    assert parsed_data["list-key"][0] == "item1"
    assert parsed_data["list-key"][1] == "item2"
    assert parsed_data["list-key"][2] == "item3"
    
    assert "set-key" in parsed_data
    assert len(parsed_data["set-key"]) == 3
    assert "member1" in parsed_data["set-key"]
    assert "member2" in parsed_data["set-key"]
    assert "member3" in parsed_data["set-key"]
    
    assert "hash-key" in parsed_data
    assert len(parsed_data["hash-key"]) == 2
    assert parsed_data["hash-key"]["field1"] == "value1"
    assert parsed_data["hash-key"]["field2"] == "value2"
    
    # Verify there are no expiry times
    assert len(parsed_expires) == 0

def test_write_redis_file_with_expiry(temp_rdb_path):
    """Test writing a Redis database with expiry times to an RDB file."""
    # Create test data
    data = {
        "string-key": "string-value",
        "expiring-key-sec": "expires-in-seconds",
        "expiring-key-ms": "expires-in-milliseconds"
    }
    
    # Current time plus offsets
    current_time = int(time.time())
    expires = {
        "expiring-key-sec": current_time + 3600,  # 1 hour from now in seconds
        "expiring-key-ms": (current_time + 3600) * 1000  # 1 hour from now in milliseconds
    }
    
    # Write the RDB file
    write_redis_file(temp_rdb_path, data, expires)
    
    # Verify the file exists
    assert os.path.exists(temp_rdb_path)
    
    # Parse the file with our parser to verify the contents
    parsed_data, parsed_expires = parse_redis_file(temp_rdb_path)
    
    # Verify the parsed data
    assert "string-key" in parsed_data
    assert parsed_data["string-key"] == "string-value"
    
    assert "expiring-key-sec" in parsed_data
    assert parsed_data["expiring-key-sec"] == "expires-in-seconds"
    
    assert "expiring-key-ms" in parsed_data
    assert parsed_data["expiring-key-ms"] == "expires-in-milliseconds"
    
    # Verify expiry times
    assert "expiring-key-sec" in parsed_expires
    assert "expiring-key-ms" in parsed_expires
    assert parsed_expires["expiring-key-sec"] >= current_time + 3600 - 1  # Allow for some rounding
    assert parsed_expires["expiring-key-sec"] <= current_time + 3600 + 1
    assert parsed_expires["expiring-key-ms"] >= current_time + 3600 - 1  # Allow for conversion to seconds
    assert parsed_expires["expiring-key-ms"] <= current_time + 3600 + 1 