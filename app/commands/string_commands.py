import time
from typing import TYPE_CHECKING, List, Dict, Any
from app.commands.commands import RedisCommand
from app.utils import encoding_utils
from app.utils.constants import NON_INT_ERROR, NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE, NIL_RESPONSE, FLOAT_ERROR_MESSAGE

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler

async def get_value(handler: 'AsyncRequestHandler', key: str) -> None|str:
    if handler.expiration.get(key, None) and handler.expiration[key] < time.time():
        handler.memory.pop(key, None)
        handler.expiration.pop(key, None)
        return None
    else:
        value = handler.memory.get(key, None)
        if value is None:
            return NOT_FOUND_RESPONSE
        elif not isinstance(value, str):
            return WRONG_TYPE_RESPONSE
        else:
            return value

class GetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        value = await get_value(handler, key)
        return encoding_utils.as_bulk_string(value)
        

class MGetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        keys = command[1:]
        values = []
        for key in keys:
            value = await get_value(handler, key)
            if value == WRONG_TYPE_RESPONSE or value == NOT_FOUND_RESPONSE:
                value = None
            values.append(value)
        return encoding_utils.generate_redis_array(lst=values)
    
    
class SetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if len(command) < 3:
            return "-ERR wrong number of arguments for 'set' command\r\n"
        
        key = command[1]
        value = command[2]
        expiry_ms = None
        
        # Check for optional PX parameter
        i = 3
        while i < len(command):
            if command[i].upper() == "PX" and i + 1 < len(command):
                try:
                    expiry_ms = int(command[i + 1])
                except ValueError:
                    return "-ERR value is not an integer or out of range\r\n"
                i += 2
            else:
                i += 1
        
        handler.memory[key] = value
        
        if expiry_ms is not None:
            handler.expiration[key] = time.time() + (expiry_ms / 1000.0)
        else:
            handler.expiration.pop(key, None)  # Remove any existing expiration
        
        # Register the change with the server for auto-save
        handler.server.register_change()
        
        # Handle replication to replicas
        handler.server.numacks = 0  
        for writer in handler.server.writers:
            print(f"writing CMD {command} to writer: {writer.get_extra_info('peername')}")
            writer.write(encoding_utils.encode_redis_protocol(command))
            await writer.drain()
        
        return "+OK\r\n"
    
class MSetCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if len(command) % 2 != 1:
            return "-ERR wrong number of arguments for MSET\r\n"
        
        for i in range(1, len(command), 2):
            key = command[i]
            value = command[i+1]
            set_command = SetCommand()
            await set_command.execute(handler, ["SET", key, value])
        return "+OK\r\n"

class IncrByCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        increment = command[2]
        value = await get_value(handler, key)
        if value == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if value == NOT_FOUND_RESPONSE:
            value = "0"
        try:
            increment = int(increment)
            value = int(value)
        except ValueError:
            return NON_INT_ERROR
        handler.memory[key] = str(int(value) + int(increment))
        return f":{handler.memory[key]}\r\n"
        
class IncrCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        incr_by_command = IncrByCommand()
        return await incr_by_command.execute(handler, ["INCRBY", command[1], "1"])
    
    
class DecrByCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        decrement = command[2]
        return await IncrByCommand().execute(handler, ["INCRBY", key, f"-{decrement}"])
        
class DecrCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        incr_by_command = IncrByCommand()
        return await incr_by_command.execute(handler, ["INCRBY", command[1], "-1"])
    
    
class AppendCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        value = command[2]
        existing_value = await get_value(handler, key)
        print(existing_value)
        if existing_value == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        if existing_value == NOT_FOUND_RESPONSE:
            handler.memory[key] = value
        else:
            handler.memory[key] += value
        return f":{len(handler.memory[key])}\r\n"