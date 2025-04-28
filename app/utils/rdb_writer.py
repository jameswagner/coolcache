import struct
import time
from typing import Any, BinaryIO, Dict, List, Set, Tuple, Optional, Union

# RDB Format Constants
REDIS_MAGIC = b'REDIS'
RDB_VERSION_CURRENT = b'0011'  # Version 7.2

# RDB Opcodes
RDB_OPCODE_EOF = b'\xFF'       # End of file marker
RDB_OPCODE_SELECTDB = b'\xFE'  # Database selector
RDB_OPCODE_EXPIRY_MS = b'\xFD' # Expiry time in milliseconds
RDB_OPCODE_EXPIRY_SEC = b'\xFC' # Expiry time in seconds
RDB_OPCODE_RESIZEDB = b'\xFB'  # Resize database (since Redis 7.0)

# RDB Types
RDB_TYPE_STRING = b'\x00'
RDB_TYPE_LIST = b'\x01'
RDB_TYPE_SET = b'\x02'
RDB_TYPE_HASH = b'\x03'
RDB_TYPE_ZSET = b'\x04'
RDB_TYPE_STREAM = b'\x06'  # Not fully implemented yet

def encode_length(length: int) -> bytes:
    """
    Encode an integer using Redis length encoding format.
    
    Redis uses a special format for length encoding:
    - 00XXXXXX for values 0-63
    - 01XXXXXX YYYYYYYY for values 64-16383
    - 10000000 XXXXXXXX YYYYYYYY ZZZZZZZZ AAAAAAAA for larger values
    
    Args:
        length: The integer to encode
        
    Returns:
        The encoded length as bytes
    """
    if length < 0:
        raise ValueError("Length cannot be negative")
    elif length <= 63:  # 6-bit length (0-63)
        return bytes([length])
    elif length <= 16383:  # 14-bit length (64-16383)
        high_bits = 0x40 | (length >> 8)  # 01XXXXXX
        low_bits = length & 0xFF  # YYYYYYYY
        return bytes([high_bits, low_bits])
    else:  # 32-bit length
        return b'\x80' + struct.pack("<I", length)

def encode_string(string_data: Union[str, bytes]) -> bytes:
    """
    Encode a string with its length prefix according to Redis RDB format.
    
    Args:
        string_data: The string to encode, can be str or bytes
        
    Returns:
        The encoded string with length prefix as bytes
    """
    # Convert string to bytes if needed
    if isinstance(string_data, str):
        string_bytes = string_data.encode('utf-8')
    else:
        string_bytes = string_data
    
    # Encode the length followed by the string data
    return encode_length(len(string_bytes)) + string_bytes

def encode_set_value(value: Set[Union[str, bytes]]) -> bytes:
    """
    Encode a set value according to Redis RDB format.
    
    Args:
        value: The set to encode, containing strings or bytes
        
    Returns:
        The encoded set value as bytes
    """
    # Start with the set length
    result = encode_length(len(value))
    
    # Add each element
    for item in value:
        result += encode_string(item)
    
    return result

def encode_list_value(value: List[Union[str, bytes]]) -> bytes:
    """
    Encode a list value according to Redis RDB format.
    
    Args:
        value: The list to encode, containing strings or bytes
        
    Returns:
        The encoded list value as bytes
    """
    # Start with the list length
    result = encode_length(len(value))
    
    # Add each element in order
    for item in value:
        result += encode_string(item)
    
    return result

def encode_hash_value(value: Dict[Union[str, bytes], Union[str, bytes]]) -> bytes:
    """
    Encode a hash value according to Redis RDB format.
    
    Args:
        value: The hash map to encode, with keys and values as strings or bytes
        
    Returns:
        The encoded hash value as bytes
    """
    # Start with the hash length (number of field-value pairs)
    result = encode_length(len(value))
    
    # Add each field-value pair
    for field, val in value.items():
        result += encode_string(field)  # Encode field
        result += encode_string(val)    # Encode value
    
    return result

def encode_zset_value(value: List[Tuple[Union[str, bytes], float]]) -> bytes:
    """
    Encode a sorted set value according to Redis RDB format.
    
    Args:
        value: The sorted set to encode, as a list of (member, score) pairs
              where members are strings or bytes and scores are floats
        
    Returns:
        The encoded sorted set value as bytes
    """
    # Start with the zset length (number of member-score pairs)
    result = encode_length(len(value))
    
    # Add each member-score pair
    for member, score in value:
        result += encode_string(member)        # Encode member
        result += struct.pack('<d', score)     # Encode score as 8-byte double
    
    return result

def encode_value(value: Any) -> Tuple[bytes, bytes]:
    """
    Encode a value based on its type and return the type marker and encoded value.
    
    Args:
        value: The value to encode, can be str, bytes, list, set, dict, or list of (member, score) tuples
        
    Returns:
        A tuple of (type_marker, encoded_value)
    """
    # Determine the type and encode accordingly
    if isinstance(value, (str, bytes)):
        # String value
        return RDB_TYPE_STRING, encode_string(value)
    elif isinstance(value, list):
        # Check if it's a list or a sorted set (list of (member, score) tuples)
        if all(isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], (int, float)) for item in value):
            # Sorted set
            return RDB_TYPE_ZSET, encode_zset_value(value)
        else:
            # Regular list
            return RDB_TYPE_LIST, encode_list_value(value)
    elif isinstance(value, set):
        # Set value
        return RDB_TYPE_SET, encode_set_value(value)
    elif isinstance(value, dict):
        # Hash value
        return RDB_TYPE_HASH, encode_hash_value(value)
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")

def encode_key_value_pair(key: Union[str, bytes], value: Any) -> bytes:
    """
    Encode a key-value pair without expiry time.
    
    Args:
        key: The key as string or bytes
        value: The value to encode (can be string, bytes, list, set, dict, etc.)
        
    Returns:
        The encoded key-value pair as bytes
    """
    # First determine the value type and get the encoded value
    value_type, encoded_value = encode_value(value)
    
    # The format is: value_type + key + value
    result = value_type
    result += encode_string(key)
    result += encoded_value
    
    return result

def encode_key_value_pair_with_expiry_ms(key: Union[str, bytes], value: Any, expiry_time_ms: int) -> bytes:
    """
    Encode a key-value pair with expiry time in milliseconds.
    
    Args:
        key: The key as string or bytes
        value: The value to encode (can be string, bytes, list, set, dict, etc.)
        expiry_time_ms: Expiry time in milliseconds since epoch
        
    Returns:
        The encoded key-value pair with expiry as bytes
    """
    # First determine the value type and get the encoded value
    value_type, encoded_value = encode_value(value)
    
    # The format is: EXPIRY_MS + expiry_time + value_type + key + value
    result = RDB_OPCODE_EXPIRY_MS
    result += struct.pack('<Q', expiry_time_ms)  # 8-byte unsigned int for ms
    result += value_type
    result += encode_string(key)
    result += encoded_value
    
    return result

def encode_key_value_pair_with_expiry_sec(key: Union[str, bytes], value: Any, expiry_time_sec: int) -> bytes:
    """
    Encode a key-value pair with expiry time in seconds.
    
    Args:
        key: The key as string or bytes
        value: The value to encode (can be string, bytes, list, set, dict, etc.)
        expiry_time_sec: Expiry time in seconds since epoch
        
    Returns:
        The encoded key-value pair with expiry as bytes
    """
    # First determine the value type and get the encoded value
    value_type, encoded_value = encode_value(value)
    
    # The format is: EXPIRY_SEC + expiry_time + value_type + key + value
    result = RDB_OPCODE_EXPIRY_SEC
    result += struct.pack('<I', expiry_time_sec)  # 4-byte unsigned int for seconds
    result += value_type
    result += encode_string(key)
    result += encoded_value
    
    return result

def write_header(file: BinaryIO) -> None:
    """
    Write the RDB file header.
    
    Args:
        file: Binary file object to write to
    """
    # Write the Redis magic string and version
    file.write(REDIS_MAGIC)
    file.write(RDB_VERSION_CURRENT)

def write_database_selector(file: BinaryIO, db_number: int, db_size: Optional[int] = None, expire_size: Optional[int] = None) -> None:
    """
    Write a database selector opcode and database size information if provided.
    
    Args:
        file: Binary file object to write to
        db_number: Database number to select (0-15)
        db_size: Optional, number of keys in the database
        expire_size: Optional, number of keys with expiry time
    """
    # Write the selector opcode and database number
    file.write(RDB_OPCODE_SELECTDB)
    file.write(bytes([db_number]))
    
    # If database size is provided, write RESIZEDB opcode and sizes
    if db_size is not None:
        file.write(RDB_OPCODE_RESIZEDB)
        file.write(encode_length(db_size))
        file.write(encode_length(expire_size or 0))  # Default to 0 expires if not specified

def write_footer(file: BinaryIO) -> None:
    """
    Write the RDB file footer (EOF marker).
    
    Args:
        file: Binary file object to write to
    """
    file.write(RDB_OPCODE_EOF)

def write_redis_file(
    filename: str, 
    data: Dict[str, Any], 
    expires: Optional[Dict[str, Union[int, float]]] = None,
    db_number: int = 0
) -> None:
    """
    Write a Redis database to an RDB file.
    
    Args:
        filename: The path to write the RDB file to
        data: Dictionary containing the Redis key-value pairs
        expires: Optional dictionary mapping keys to their expiry times (seconds since epoch)
        db_number: Database number (0-15, default is 0)
    """
    if expires is None:
        expires = {}
    
    with open(filename, 'wb') as f:
        # Write the file header
        write_header(f)
        
        # Write database selector with database sizes
        write_database_selector(f, db_number, len(data), len(expires))
        
        # Write each key-value pair
        for key, value in data.items():
            if key in expires:
                expiry_time = expires[key]
                # Determine if we should use seconds or milliseconds
                # If expiry time is large enough, it's probably in milliseconds already
                if expiry_time > 1000000000000:  # threshold for ms (around year 2286)
                    f.write(encode_key_value_pair_with_expiry_ms(key, value, int(expiry_time)))
                else:
                    # Convert to seconds if it's not already
                    expiry_sec = int(expiry_time)
                    f.write(encode_key_value_pair_with_expiry_sec(key, value, expiry_sec))
            else:
                # No expiry
                f.write(encode_key_value_pair(key, value))
        
        # Write the footer
        write_footer(f) 