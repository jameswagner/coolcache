from typing import TYPE_CHECKING, Dict, List, Set
from app.utils import encoding_utils
from app.utils.constants import NIL_RESPONSE, WRONG_TYPE_RESPONSE
from app.commands.commands import RedisCommand

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler
    

def get_hash_map_from_memory(handler: 'AsyncRequestHandler', key: str) -> Dict:
    if key not in handler.memory:
        return {}
    elif not isinstance(handler.memory[key], dict):
        return WRONG_TYPE_RESPONSE
    return handler.memory[key]


class HGetAllCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_hash_map = get_hash_map_from_memory(handler, key)
        if existing_hash_map == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        response = []
        for k, v in existing_hash_map.items():
            response.append(k)
            response.append(v)
        return encoding_utils.generate_redis_array(lst=response)
    
class HGetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        field = command[2]
        existing_hash_map = get_hash_map_from_memory(handler, key)
        if existing_hash_map == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if field not in existing_hash_map:
            return NIL_RESPONSE
        return encoding_utils.as_bulk_string(existing_hash_map[field])

class HSetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_hash_map = get_hash_map_from_memory(handler, key)
        if existing_hash_map == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        for i in range(2, len(command), 2):
            existing_hash_map[command[i]] = command[i + 1]
        handler.memory[key] = existing_hash_map
        return "+OK\r\n"
