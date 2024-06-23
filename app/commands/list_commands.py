from app.utils.constants import  NON_INT_ERROR, SYNTAX_ERROR, WRONG_TYPE_RESPONSE, NIL_RESPONSE, WRONG_TYPE_RESPONSE
from app.commands.commands import RedisCommand
from typing import List, TYPE_CHECKING

from app.utils.encoding_utils import generate_redis_array

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler

def get_list_from_memory(handler: 'AsyncRequestHandler', key: str) -> List[str]:
    if key not in handler.memory:
        return []
    elif not isinstance(handler.memory[key], list):
        return WRONG_TYPE_RESPONSE
    return handler.memory[key]

class LInsertCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        position = command[2].lower()
        pivot = command[3]
        value = command[4]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if pivot not in existing_list:
            return NIL_RESPONSE
        if position == "before":
            existing_list.insert(existing_list.index(pivot), value)
        elif position == "after":
            existing_list.insert(existing_list.index(pivot) + 1, value)
        else:
            return SYNTAX_ERROR
        return f":{len(existing_list)}\r\n"

class LPopCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if not existing_list:
            return NIL_RESPONSE
        value = existing_list.pop(0)
        if not existing_list:
            handler.memory.pop(key)
        return f"${len(value)}\r\n{value}\r\n"
    
class LPushXCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if not existing_list:
            return WRONG_TYPE_RESPONSE
        values = command[2:][::-1]
        values.extend(existing_list)
        handler.memory[key] = values
        return f":{len(handler.memory[key])}\r\n"
    
class RPushXCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if not existing_list:
            return WRONG_TYPE_RESPONSE
        values = command[2:]
        existing_list.extend(values)
        return f":{len(handler.memory[key])}\r\n"

class RPopCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if not existing_list:
            return NIL_RESPONSE
        value = existing_list.pop(-1)
        if not existing_list:
            handler.memory.pop(key)

        return f"${len(value)}\r\n{value}\r\n"

class LPushCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        values = command[2:][::-1]
        values.extend(existing_list)
        handler.memory[key] = values
        return f":{len(handler.memory[key])}\r\n"
    
class LRangeCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        start = int(command[2])
        stop = int(command[3])
        values = handler.memory.get(key, [])
        if start < 0:
            start = len(values) + start
        if stop < 0:
            stop = len(values) + stop
        start = max(0, start)
        stop = max(0, stop)
        start = min(len(values) - 1, start)
        stop = min(len(values) - 1, stop)
        return generate_redis_array(lst= values[start:stop+1])

class LLenCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        return f":{len(existing_list)}\r\n"


class LIndexCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        index = command[2]
        try:
            index = int(index)
        except ValueError:
            return NON_INT_ERROR
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if index < 0:
            index = len(existing_list) + index
        if index < 0 or index >= len(existing_list):
            return NIL_RESPONSE
        return f"${len(existing_list[index])}\r\n{existing_list[index]}\r\n"
    
class LSetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        index = command[2]
        try: 
            index = int(index)
        except ValueError:
            return NON_INT_ERROR
        value = command[3]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if index < 0:
            index = len(existing_list) + index
        if index < 0 or index >= len(existing_list):
            return NON_INT_ERROR
        existing_list[index] = value
        return "+OK\r\n"


class RPushCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_list = get_list_from_memory(handler, key)
        if existing_list == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        values = command[2:]
        existing_list.extend(values)
        handler.memory[key] = existing_list
        return f":{len(handler.memory[key])}\r\n"