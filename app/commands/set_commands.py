from typing import List, Set, TYPE_CHECKING
from app.commands.commands import RedisCommand
from app.utils import encoding_utils
from app.utils.constants import WRONG_TYPE_RESPONSE

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler

def get_set_from_memory(handler: 'AsyncRequestHandler', key: str) -> Set[str]:
    if key not in handler.memory:
        return set()
    elif not isinstance(handler.memory[key], set):
        return WRONG_TYPE_RESPONSE
    return handler.memory[key]


class SAddCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        added = 0
        for value in command[2:]:
            if value not in existing_set:
                existing_set.add(value)
                added += 1
        handler.memory[key] = existing_set
        print(f"Memory: {handler.memory}")
        return f":{added}\r\n"
    
class SMembersCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        return encoding_utils.generate_redis_array(lst=list(existing_set))
    
class SRemCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        removed = 0
        for value in command[2:]:
            if value in existing_set:
                existing_set.remove(value)
                removed += 1
        return f":{removed}\r\n"
    
class SIsMemberCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        return f":{int(command[2] in existing_set)}\r\n"
    
class SCardCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        return f":{len(existing_set)}\r\n"
    
class SDiffCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key1 = command[1]
        key2 = command[2]
        existing_set1 = get_set_from_memory(handler, key1)
        if existing_set1 == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        existing_set2 = get_set_from_memory(handler, key2)
        if existing_set2 == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        diff = existing_set1 - existing_set2
        return encoding_utils.generate_redis_array(lst=list(diff))
    
class SUnionCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        sets = []
        for key in command[1:]:
            existing_set = get_set_from_memory(handler, key)
            if existing_set == WRONG_TYPE_RESPONSE:
                return WRONG_TYPE_RESPONSE
            sets.append(existing_set)
        union = set.union(*sets)
        return encoding_utils.generate_redis_array(lst=list(union))


class SInterCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        sets = []
        for key in command[1:]:
            existing_set = get_set_from_memory(handler, key)
            if existing_set == WRONG_TYPE_RESPONSE:
                return WRONG_TYPE_RESPONSE
            sets.append(existing_set)
        intersection = set.intersection(*sets)
        print(f"Sets: {sets}")
        print(f"Intersection: {intersection}")
        return encoding_utils.generate_redis_array(lst=list(intersection))
    
class SPopCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if not existing_set:
            return "$-1\r\n"
        value = existing_set.pop()
        print(f"MEMORY {handler.memory[key]}")
        return f"${len(value)}\r\n{value}\r\n"

class SMoveCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        source_key = command[1]
        dest_key = command[2]
        value = command[3]
        source_set = get_set_from_memory(handler, source_key)
        if source_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if value not in source_set:
            return ":0\r\n"
        dest_set = get_set_from_memory(handler, dest_key)
        if dest_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        source_set.remove(value)
        dest_set.add(value)
        return ":1\r\n"