import random
import string
from typing import List, Any, Iterable

from app.utils.constants import EMPTY_ARRAY_RESPONSE, NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE


def generate_redis_array(string: str = "", lst: List[str] = []) -> str:
    redis_array = []
    if(string):
        redis_array.append(as_bulk_string(string))
    if(lst != None):
        if(len(lst) == 0):
            return EMPTY_ARRAY_RESPONSE
        redis_array.append(f"*{len(lst)}\r\n")
        for element in lst:
            if element == WRONG_TYPE_RESPONSE:
                element = None
            redis_array.append(as_bulk_string(element))
    return ''.join(redis_array)

def as_array(items: Iterable[Any]) -> str:
    """
    Convert a Python collection to a Redis array.
    
    Args:
        items: Collection of items to convert to a Redis array
        
    Returns:
        Redis protocol array string
    """
    if not items:
        return EMPTY_ARRAY_RESPONSE
        
    result = [f"*{len(list(items))}\r\n"]
    
    for item in items:
        if item is None:
            result.append("$-1\r\n")
        else:
            str_item = str(item)
            result.append(f"${len(str_item)}\r\n{str_item}\r\n")
            
    return ''.join(result)

def as_bulk_string(payload: str) -> str:
    if not payload or payload == NOT_FOUND_RESPONSE:
        return NOT_FOUND_RESPONSE
    if payload == WRONG_TYPE_RESPONSE:
        return WRONG_TYPE_RESPONSE
    return f"${len(payload)}\r\n{payload}\r\n"

def redis_bulk_string_to_string(data: str) -> str:
    if data == NOT_FOUND_RESPONSE:
        return NOT_FOUND_RESPONSE
    if data == WRONG_TYPE_RESPONSE:
        return WRONG_TYPE_RESPONSE
    return data.split('\r\n')[1]

def redis_array_to_list(data: str) -> List[str]:
    if data == NOT_FOUND_RESPONSE:
        return NOT_FOUND_RESPONSE
    if data == WRONG_TYPE_RESPONSE:
        return WRONG_TYPE_RESPONSE
    elements = data.split('\r\n')
    print(f"Elements: {elements}")
    if elements[0] == '*0':
        return []
    print(elements[2::2])
    return elements[2::2]

def encode_redis_protocol(data: List[str]) -> bytes:
    encoded_data = []
    encoded_data.append(f"*{len(data)}\r\n")
    
    for element in data:
        encoded_data.append(f"${len(element)}\r\n{element}\r\n")
    
    return ''.join(encoded_data).encode()

def parse_redis_protocol(data: bytes):
    try:
        data = data[data.index(b'*'):]
        parts = data.split(b'\r\n')
        commands = []
        lengths = []  # New array to store the lengths of the substrings used for commands
        offset = 0  # Variable to keep track of the offset
        index = 0
        while index < len(parts) - 1:
            if parts[index] and parts[index][0] == ord(b'*'):
                num_elements = int(parts[index][1:])
                offset += len(str(num_elements)) + 3  # Add the length of the number of elements and the length of the * and \r\n characters
                index += 1
                elements = []
                for _ in range(num_elements):
                    if parts[index] and parts[index][0] == ord(b'$'):
                        element_length = int(parts[index][1:])
                        offset += len(str(element_length)) + 3  # Add the length of the element length and the length of the $ and \r\n characters
                        index += 1
                        element = parts[index].decode('utf-8')
                        elements.append(element)
                        index += 1
                        offset += element_length + 2 
                commands.append(elements)
                lengths.append(offset)  # Store the offset as the length of the substring used for the command
                offset = 0
            else:
                index += 1

        return commands, lengths  # Return the commands and lengths arrays
    except (IndexError, ValueError):
        return [], []  # Return empty arrays if there was an error

    
def generate_random_string(length: int) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def parse_element(data: bytes, index: int):
    if data[index] == ord(b'+'):
        return parse_simple_string(data, index)
    if data[index] == ord(b'-'):
        return parse_error(data, index)
    if data[index] == ord(b':'):
        return parse_integer(data, index)
    if data[index] == ord(b'$'):
        return parse_bulk_string(data, index)
    if data[index] == ord(b'*'):
        return parse_array(data, index)
    return None, index


def parse_error(data: bytes, index: int):
    end = data.index(b'\r\n', index)
    return Exception(data[index + 1:end].decode()), end + 2
    
def parse_integer(data: bytes, index: int):
    end = data.index(b'\r\n', index)
    return int(data[index + 1:end]), end + 2

def parse_bulk_string(data: bytes, index: int):
    end = data.index(b'\r\n', index)
    length = int(data[index + 1:end])
    if length == -1:
        return "(nil)", end + 2
    start = end + 2
    end = start + length
    return data[start:end].decode(), end + 2

def parse_simple_string(data: bytes, index: int):
    end = data.index(b'\r\n', index)
    return data[index + 1:end].decode(), end + 2

def parse_array(data: bytes, index: int):
    end = data.index(b'\r\n', index)
    length = int(data[index + 1:end])
    if length == -1:
        return None, end + 2
    start = end + 2
    elements = []
    for _ in range(length):
        element, start = parse_element(data, start)
        elements.append(element)
    return elements, start