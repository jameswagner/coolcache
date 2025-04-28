import time
import struct
from typing import Any, BinaryIO, Dict, List, Set, Tuple, Optional

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

def read_length(file: BinaryIO) -> int:
    """
    Read a length-encoded integer from the RDB file.
    
    Redis uses a special format for length encoding:
    - Bytes starting with 00XXXXXX encode values 0-63
    - Bytes starting with 01XXXXXX YYYYYYYY encode values 64-16383
    - Bytes starting with 10000000 XXXXXXXX YYYYYYYY ZZZZZZZZ AAAAAAAA encode larger values
    """
    first_byte = file.read(1)
    if not first_byte:
        raise EOFError("Unexpected end of file while reading length")
    
    first_byte_int = ord(first_byte)
    if (first_byte_int & 0xC0) == 0:  # 00XXXXXX
        # 6-bit length (0-63)
        return first_byte_int & 0x3F
    elif (first_byte_int & 0xC0) == 0x40:  # 01XXXXXX
        # 14-bit length (64-16383)
        second_byte = file.read(1)
        if not second_byte:
            raise EOFError("Unexpected end of file while reading length")
        second_byte_int = ord(second_byte)
        return ((first_byte_int & 0x3F) << 8) | second_byte_int
    elif first_byte_int == 0x80:  # 10000000
        # 32-bit length
        length_bytes = file.read(4)
        if len(length_bytes) < 4:
            raise EOFError("Unexpected end of file while reading length")
        return struct.unpack("<I", length_bytes)[0]
    else:
        # Invalid encoding
        raise ValueError(f"Invalid length encoding: {first_byte_int:02x}")

def read_string(file: BinaryIO) -> bytes:
    """Read a string value from the RDB file."""
    length = read_length(file)
    string_data = file.read(length)
    if len(string_data) < length:
        raise EOFError("Unexpected end of file while reading string")
    return string_data

def read_encoded_value(file: BinaryIO, value_type: bytes) -> Any:
    """
    Read an encoded value from the RDB file based on its type.
    
    Args:
        file: Binary file object to read from.
        value_type: Type of value to read.
        
    Returns:
        The decoded value.
    """
    if value_type == RDB_TYPE_STRING:
        # String value
        return read_string(file)
    elif value_type == RDB_TYPE_LIST:
        # List value
        length = read_length(file)
        value = []
        for _ in range(length):
            element = read_string(file)
            value.append(element)
        return value
    elif value_type == RDB_TYPE_SET:
        # Set value
        length = read_length(file)
        value = set()
        for _ in range(length):
            element = read_string(file)
            value.add(element)
        return value
    elif value_type == RDB_TYPE_HASH:
        # Hash value
        length = read_length(file)
        value = {}
        for _ in range(length):
            field = read_string(file)
            val = read_string(file)
            value[field] = val
        return value
    elif value_type == RDB_TYPE_ZSET:
        # Sorted set value
        length = read_length(file)
        value = []
        for _ in range(length):
            member = read_string(file)
            score_bytes = file.read(8)
            if len(score_bytes) < 8:
                raise EOFError("Unexpected end of file while reading zset score")
            score = struct.unpack("<d", score_bytes)[0]
            value.append((member, score))
        return value
    elif value_type == RDB_TYPE_STREAM:
        # Stream value - simplified handling for now
        # In a real implementation, we'd need to parse the full stream structure
        # For now, just return a placeholder
        # Skip the stream data
        length = read_length(file)  # Number of entries
        # Skip each entry
        for _ in range(length):
            # Skip entry ID
            file.read(16)
            # Skip fields
            fields_count = read_length(file)
            for _ in range(fields_count):
                # Skip field and value
                field_len = read_length(file)
                file.read(field_len)
                value_len = read_length(file)
                file.read(value_len)
        return b"stream-placeholder"
    else:
        # Unknown value type
        print(f"Unknown value type: {value_type}")
        return None

def handle_database_selector(file: BinaryIO) -> None:
    """
    Handle the SELECT DB opcode in the RDB file.
    
    Args:
        file: Binary file object to read from.
    """
    db_number = ord(file.read(1))
    
    # We need to check for RESIZEDB without peek()
    # Save the current position
    current_pos = file.tell()
    
    # Read the next byte
    next_byte = file.read(1)
    
    # If it's the RESIZEDB opcode, handle it
    if next_byte == RDB_OPCODE_RESIZEDB:
        # Read the database size information
        # First the number of keys
        _ = read_length(file)
        # Then the number of expires
        _ = read_length(file)
    else:
        # If it's not RESIZEDB, we need to go back
        file.seek(current_pos)

def handle_key_value_pair_with_expiry_ms(file: BinaryIO) -> Tuple[Optional[bytes], Optional[Any], Optional[float]]:
    """
    Handle a key-value pair with expiry time in milliseconds.
    
    Args:
        file: Binary file object to read from.
        
    Returns:
        Tuple of (key, value, expiry_time).
    """
    try:
        expiry_bytes = file.read(8)
        if len(expiry_bytes) < 8:
            raise EOFError("Unexpected end of file while reading expiry time")
        
        expiry_time = struct.unpack("<Q", expiry_bytes)[0] / 1000.0  # Convert ms to seconds
        value_type = file.read(1)
        if not value_type:
            raise EOFError("Unexpected end of file while reading value type")
        
        key = read_string(file)
        value = read_encoded_value(file, value_type)
        
        # Check if the key has expired
        if expiry_time > 0 and expiry_time < time.time():
            return None, None, 0
            
        return key, value, expiry_time
    except Exception as e:
        print(f"Error reading key-value pair with ms expiry: {e}")
        return None, None, 0

def handle_key_value_pair_with_expiry_sec(file: BinaryIO) -> Tuple[Optional[bytes], Optional[Any], Optional[float]]:
    """
    Handle a key-value pair with expiry time in seconds.
    
    Args:
        file: Binary file object to read from.
        
    Returns:
        Tuple of (key, value, expiry_time).
    """
    try:
        expiry_bytes = file.read(4)
        if len(expiry_bytes) < 4:
            raise EOFError("Unexpected end of file while reading expiry time")
        
        expiry_time = struct.unpack("<I", expiry_bytes)[0]  # Seconds since epoch
        value_type = file.read(1)
        if not value_type:
            raise EOFError("Unexpected end of file while reading value type")
        
        key = read_string(file)
        value = read_encoded_value(file, value_type)
        
        # Check if the key has expired
        if expiry_time > 0 and expiry_time < time.time():
            return None, None, 0
            
        return key, value, expiry_time
    except Exception as e:
        print(f"Error reading key-value pair with sec expiry: {e}")
        return None, None, 0

def handle_key_value_pair(file: BinaryIO, value_type: bytes) -> Tuple[Optional[bytes], Optional[Any], float]:
    """
    Handle a key-value pair without expiry time.
    
    Args:
        file: Binary file object to read from.
        value_type: Type of value to read.
        
    Returns:
        Tuple of (key, value, expiry_time).
    """
    try:
        key = read_string(file)
        value = read_encoded_value(file, value_type)
        return key, value, 0  # No expiry
    except Exception as e:
        print(f"Error reading key-value pair: {e}")
        return None, None, 0

def parse_redis_file(file_path: str) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """
    Parse a Redis RDB file.
    
    Args:
        file_path: Path to the RDB file.
        
    Returns:
        Tuple of (hash_map, expiry_times).
    """
    hash_map = {}
    expiry_times = {}

    try:
        with open(file_path, "rb") as file:
            # Read magic string and version
            magic_string = file.read(len(REDIS_MAGIC))
            if magic_string != REDIS_MAGIC:
                raise ValueError(f"Invalid RDB file: magic string mismatch. Expected {REDIS_MAGIC}, got {magic_string}")
                
            rdb_version = file.read(len(RDB_VERSION_CURRENT))
            # We accept any version, but log if it's not the expected one
            if rdb_version != RDB_VERSION_CURRENT:
                print(f"Warning: RDB version is {rdb_version}, expected {RDB_VERSION_CURRENT}")
            
            # Main parsing loop
            while True:
                opcode = file.read(1)
                if not opcode:
                    break  # End of file
                
                if opcode == RDB_OPCODE_EOF:
                    # End of file marker
                    break
                elif opcode == RDB_OPCODE_SELECTDB:
                    # Database selector
                    handle_database_selector(file)
                elif opcode == RDB_OPCODE_EXPIRY_MS:
                    # Expiry time in milliseconds
                    key, value, expiry_time = handle_key_value_pair_with_expiry_ms(file)
                    if key is not None and value is not None:
                        try:
                            hash_map[key.decode('utf-8')] = decode_value(value)
                            if expiry_time > 0:
                                expiry_times[key.decode('utf-8')] = expiry_time
                        except Exception as e:
                            print(f"Error decoding key or value: {e}")
                elif opcode == RDB_OPCODE_EXPIRY_SEC:
                    # Expiry time in seconds
                    key, value, expiry_time = handle_key_value_pair_with_expiry_sec(file)
                    if key is not None and value is not None:
                        try:
                            hash_map[key.decode('utf-8')] = decode_value(value)
                            if expiry_time > 0:
                                expiry_times[key.decode('utf-8')] = expiry_time
                        except Exception as e:
                            print(f"Error decoding key or value: {e}")
                else:
                    # Assume it's a value type
                    key, value, _ = handle_key_value_pair(file, opcode)
                    if key is not None and value is not None:
                        try:
                            hash_map[key.decode('utf-8')] = decode_value(value)
                        except Exception as e:
                            print(f"Error decoding key or value: {e}")

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error parsing RDB file: {e}")

    return hash_map, expiry_times

def decode_value(value: Any) -> Any:
    """
    Decode a value from bytes to appropriate Python type.
    
    Args:
        value: Value to decode.
        
    Returns:
        Decoded value.
    """
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # Return as bytes if not valid UTF-8
            return value
    elif isinstance(value, list):
        # Decode each element in the list
        return [decode_value(item) for item in value]
    elif isinstance(value, set):
        # Decode each element in the set
        return {decode_value(item) for item in value}
    elif isinstance(value, dict):
        # Decode each key and value in the dict
        return {decode_value(k): decode_value(v) for k, v in value.items()}
    else:
        return value