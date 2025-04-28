import os
import struct
import tempfile
import pytest
import time
from pathlib import Path
from io import BytesIO

from app.utils.rdb_parser import (
    parse_redis_file, 
    read_length, 
    read_string, 
    read_encoded_value,
    handle_key_value_pair, 
    handle_key_value_pair_with_expiry_ms,
    handle_key_value_pair_with_expiry_sec,
    handle_database_selector,
    decode_value,
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

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_dir_obj = tempfile.TemporaryDirectory()
    temp_path = Path(temp_dir_obj.name)
    yield temp_path
    temp_dir_obj.cleanup()

def test_read_length_small():
    """Test reading a small length value (0-63)."""
    # Test with value 42
    test_data = struct.pack('B', 42)
    file = BytesIO(test_data)
    
    result = read_length(file)
    assert result == 42

def test_read_length_medium():
    """Test reading a medium length value (64-16383)."""
    # Test with value 1000 (0x03E8)
    # Format: 01XXXXXX YYYYYYYY
    # For 1000 (decimal) = 0x03E8 (hex), 
    # We need 01000011 11101000 (binary)
    high_bits = 0x40 | (1000 >> 8)  # 0x40 | 0x03 = 0x43
    low_bits = 1000 & 0xFF  # 0xE8
    test_data = struct.pack('BB', high_bits, low_bits)
    file = BytesIO(test_data)
    
    result = read_length(file)
    assert result == 1000

def test_read_length_large():
    """Test reading a large length value (16384+)."""
    # Test with value 100000
    # Format: 10000000 + 4 bytes (little endian)
    test_data = bytes([0x80]) + struct.pack('<I', 100000)
    file = BytesIO(test_data)
    
    result = read_length(file)
    assert result == 100000

def test_read_string():
    """Test reading a string value."""
    test_string = b"hello world"
    # Format: length (11) followed by string bytes
    test_data = struct.pack('B', len(test_string)) + test_string
    file = BytesIO(test_data)
    
    result = read_string(file)
    assert result == test_string

def test_read_encoded_value_string():
    """Test reading an encoded string value."""
    test_string = b"test value"
    # Format: length (10) followed by string bytes
    test_data = struct.pack('B', len(test_string)) + test_string
    file = BytesIO(test_data)
    
    result = read_encoded_value(file, RDB_TYPE_STRING)
    assert result == test_string

def test_read_encoded_value_list():
    """Test reading an encoded list value."""
    # List with 2 elements: "item1" and "item2"
    element1 = b"item1"
    element2 = b"item2"
    
    # Build test data: list length (2) followed by elements with their lengths
    test_data = struct.pack('B', 2)  # List length
    test_data += struct.pack('B', len(element1)) + element1  # Element 1
    test_data += struct.pack('B', len(element2)) + element2  # Element 2
    
    file = BytesIO(test_data)
    
    result = read_encoded_value(file, RDB_TYPE_LIST)
    assert len(result) == 2
    assert result[0] == element1
    assert result[1] == element2

def test_read_encoded_value_set():
    """Test reading an encoded set value."""
    # Set with 2 elements: "item1" and "item2"
    element1 = b"item1"
    element2 = b"item2"
    
    # Build test data: set length (2) followed by elements with their lengths
    test_data = struct.pack('B', 2)  # Set length
    test_data += struct.pack('B', len(element1)) + element1  # Element 1
    test_data += struct.pack('B', len(element2)) + element2  # Element 2
    
    file = BytesIO(test_data)
    
    result = read_encoded_value(file, RDB_TYPE_SET)
    assert len(result) == 2
    assert element1 in result
    assert element2 in result

def test_read_encoded_value_hash():
    """Test reading an encoded hash value."""
    # Hash with 2 field-value pairs
    field1 = b"field1"
    value1 = b"value1"
    field2 = b"field2"
    value2 = b"value2"
    
    # Build test data
    test_data = struct.pack('B', 2)  # Hash length (2 pairs)
    # Field 1 and Value 1
    test_data += struct.pack('B', len(field1)) + field1
    test_data += struct.pack('B', len(value1)) + value1
    # Field 2 and Value 2
    test_data += struct.pack('B', len(field2)) + field2
    test_data += struct.pack('B', len(value2)) + value2
    
    file = BytesIO(test_data)
    
    result = read_encoded_value(file, RDB_TYPE_HASH)
    assert len(result) == 2
    assert result[field1] == value1
    assert result[field2] == value2

def test_read_encoded_value_zset():
    """Test reading an encoded sorted set value."""
    # Sorted set with 2 member-score pairs
    member1 = b"member1"
    score1 = 1.5
    member2 = b"member2"
    score2 = 2.5
    
    # Build test data
    test_data = struct.pack('B', 2)  # Zset length (2 pairs)
    # Member 1 and Score 1
    test_data += struct.pack('B', len(member1)) + member1
    test_data += struct.pack('<d', score1)
    # Member 2 and Score 2
    test_data += struct.pack('B', len(member2)) + member2
    test_data += struct.pack('<d', score2)
    
    file = BytesIO(test_data)
    
    result = read_encoded_value(file, RDB_TYPE_ZSET)
    assert len(result) == 2
    assert result[0][0] == member1
    assert result[0][1] == score1
    assert result[1][0] == member2
    assert result[1][1] == score2

def test_handle_key_value_pair():
    """Test handling a key-value pair without expiry."""
    key = b"test-key"
    value = b"test-value"
    
    # Build test data
    test_data = struct.pack('B', len(key)) + key  # Key
    test_data += struct.pack('B', len(value)) + value  # Value (as string)
    
    file = BytesIO(test_data)
    
    result_key, result_value, result_expiry = handle_key_value_pair(file, RDB_TYPE_STRING)
    assert result_key == key
    assert result_value == value
    assert result_expiry == 0  # No expiry

def test_handle_key_value_pair_with_expiry_ms():
    """Test handling a key-value pair with millisecond expiry."""
    # Create a timestamp 10 seconds in the future
    future_time_ms = int((time.time() + 10) * 1000)
    key = b"key-with-expiry"
    value = b"value-with-expiry"
    
    # Build test data
    test_data = struct.pack('<Q', future_time_ms)  # Expiry time in ms
    test_data += RDB_TYPE_STRING  # Type byte
    test_data += struct.pack('B', len(key)) + key  # Key
    test_data += struct.pack('B', len(value)) + value  # Value
    
    file = BytesIO(test_data)
    
    result_key, result_value, result_expiry = handle_key_value_pair_with_expiry_ms(file)
    assert result_key == key
    assert result_value == value
    # Expiry should be approximately 10 seconds in the future
    assert result_expiry > time.time()
    assert result_expiry < time.time() + 11

def test_handle_key_value_pair_with_expiry_sec():
    """Test handling a key-value pair with second expiry."""
    # Create a timestamp 10 seconds in the future
    future_time_sec = int(time.time() + 10)
    key = b"key-with-expiry"
    value = b"value-with-expiry"
    
    # Build test data
    test_data = struct.pack('<I', future_time_sec)  # Expiry time in seconds
    test_data += RDB_TYPE_STRING  # Type byte
    test_data += struct.pack('B', len(key)) + key  # Key
    test_data += struct.pack('B', len(value)) + value  # Value
    
    file = BytesIO(test_data)
    
    result_key, result_value, result_expiry = handle_key_value_pair_with_expiry_sec(file)
    assert result_key == key
    assert result_value == value
    # Expiry should be approximately 10 seconds in the future
    assert result_expiry > time.time()
    assert result_expiry < time.time() + 11

def test_handle_database_selector():
    """Test handling a database selector opcode."""
    # Database selector with database number 0
    db_number = 0
    
    # Build test data
    test_data = struct.pack('B', db_number)
    file = BytesIO(test_data)
    
    # This should not raise exceptions
    handle_database_selector(file)

def test_handle_database_selector_with_resizedb():
    """Test handling a database selector with RESIZEDB opcode."""
    # Database selector with database number 0 and RESIZEDB info
    db_number = 0
    db_size = 42  # Small enough to fit in 6 bits (00XXXXXX format)
    expire_size = 10  # Small enough to fit in 6 bits (00XXXXXX format)
    
    # Build test data
    test_data = struct.pack('B', db_number)  # DB number
    test_data += RDB_OPCODE_RESIZEDB  # RESIZEDB opcode
    # Use small length values that fit in a single byte with 00XXXXXX format
    test_data += struct.pack('B', db_size)  # Number of keys (encoded as 00101010)
    test_data += struct.pack('B', expire_size)  # Number of expires (encoded as 00001010)
    
    file = BytesIO(test_data)
    
    # This should not raise exceptions
    handle_database_selector(file)

def test_decode_value():
    """Test decoding values from bytes to Python types."""
    # Test decoding a string
    assert decode_value(b"test string") == "test string"
    
    # Test decoding a list
    assert decode_value([b"item1", b"item2"]) == ["item1", "item2"]
    
    # Test decoding a set
    assert decode_value({b"item1", b"item2"}) == {"item1", "item2"}
    
    # Test decoding a dict
    assert decode_value({b"key1": b"value1", b"key2": b"value2"}) == {"key1": "value1", "key2": "value2"}
    
    # Test decoding nested structures
    assert decode_value({b"key1": [b"item1", b"item2"], b"key2": {b"nested": b"value"}}) == {"key1": ["item1", "item2"], "key2": {"nested": "value"}}

def test_parse_redis_file_with_string(temp_dir):
    """Test parsing an RDB file with string values."""
    rdb_path = temp_dir / "test_strings.rdb"
    
    # Create a minimal RDB file with a string value
    with open(rdb_path, 'wb') as f:
        # Write header
        f.write(REDIS_MAGIC)
        f.write(RDB_VERSION_CURRENT)
        
        # Select database 0
        f.write(RDB_OPCODE_SELECTDB)
        f.write(struct.pack('B', 0))
        
        # Write string key-value pair
        key = b"string-key"
        value = b"string-value"
        f.write(RDB_TYPE_STRING)
        f.write(struct.pack('B', len(key)))
        f.write(key)
        f.write(struct.pack('B', len(value)))
        f.write(value)
        
        # Write EOF marker
        f.write(RDB_OPCODE_EOF)
    
    # Parse the file
    data, expires = parse_redis_file(str(rdb_path))
    
    # Check the results
    assert "string-key" in data
    assert data["string-key"] == "string-value"
    assert len(expires) == 0

def test_parse_redis_file_with_expiry(temp_dir):
    """Test parsing an RDB file with expiry time."""
    rdb_path = temp_dir / "test_expiry.rdb"
    
    # Create future expiry time (10 seconds from now)
    future_time_ms = int((time.time() + 10) * 1000)
    
    # Create a minimal RDB file with an expiring value
    with open(rdb_path, 'wb') as f:
        # Write header
        f.write(REDIS_MAGIC)
        f.write(RDB_VERSION_CURRENT)
        
        # Select database 0
        f.write(RDB_OPCODE_SELECTDB)
        f.write(struct.pack('B', 0))
        
        # Write expiry time
        f.write(RDB_OPCODE_EXPIRY_MS)
        f.write(struct.pack('<Q', future_time_ms))
        
        # Write string key-value pair
        key = b"expiring-key"
        value = b"expiring-value"
        f.write(RDB_TYPE_STRING)
        f.write(struct.pack('B', len(key)))
        f.write(key)
        f.write(struct.pack('B', len(value)))
        f.write(value)
        
        # Write EOF marker
        f.write(RDB_OPCODE_EOF)
    
    # Parse the file
    data, expires = parse_redis_file(str(rdb_path))
    
    # Check the results
    assert "expiring-key" in data
    assert data["expiring-key"] == "expiring-value"
    assert "expiring-key" in expires
    # Expiry should be approximately 10 seconds in the future
    assert expires["expiring-key"] > time.time()
    assert expires["expiring-key"] < time.time() + 11

def test_parse_redis_file_with_multiple_types(temp_dir):
    """Test parsing an RDB file with multiple data types."""
    rdb_path = temp_dir / "test_multiple.rdb"
    
    # Create a minimal RDB file with multiple data types
    with open(rdb_path, 'wb') as f:
        # Write header
        f.write(REDIS_MAGIC)
        f.write(RDB_VERSION_CURRENT)
        
        # Select database 0
        f.write(RDB_OPCODE_SELECTDB)
        f.write(struct.pack('B', 0))
        
        # Write string key-value pair
        string_key = b"string-key"
        string_value = b"string-value"
        f.write(RDB_TYPE_STRING)
        f.write(struct.pack('B', len(string_key)))
        f.write(string_key)
        f.write(struct.pack('B', len(string_value)))
        f.write(string_value)
        
        # Write list key-value pair
        list_key = b"list-key"
        f.write(RDB_TYPE_LIST)
        f.write(struct.pack('B', len(list_key)))
        f.write(list_key)
        # List with 2 elements
        f.write(struct.pack('B', 2))
        item1 = b"item1"
        f.write(struct.pack('B', len(item1)))
        f.write(item1)
        item2 = b"item2"
        f.write(struct.pack('B', len(item2)))
        f.write(item2)
        
        # Write hash key-value pair
        hash_key = b"hash-key"
        f.write(RDB_TYPE_HASH)
        f.write(struct.pack('B', len(hash_key)))
        f.write(hash_key)
        # Hash with 2 field-value pairs
        f.write(struct.pack('B', 2))
        field1 = b"field1"
        value1 = b"value1"
        f.write(struct.pack('B', len(field1)))
        f.write(field1)
        f.write(struct.pack('B', len(value1)))
        f.write(value1)
        field2 = b"field2"
        value2 = b"value2"
        f.write(struct.pack('B', len(field2)))
        f.write(field2)
        f.write(struct.pack('B', len(value2)))
        f.write(value2)
        
        # Write EOF marker
        f.write(RDB_OPCODE_EOF)
    
    # Parse the file
    data, expires = parse_redis_file(str(rdb_path))
    
    # Check the results
    assert len(data) == 3
    assert "string-key" in data
    assert data["string-key"] == "string-value"
    
    assert "list-key" in data
    assert len(data["list-key"]) == 2
    assert data["list-key"][0] == "item1"
    assert data["list-key"][1] == "item2"
    
    assert "hash-key" in data
    assert len(data["hash-key"]) == 2
    assert data["hash-key"]["field1"] == "value1"
    assert data["hash-key"]["field2"] == "value2" 