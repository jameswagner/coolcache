from abc import ABC, abstractmethod
import asyncio
import os
import time
from typing import List
import app.utils.encoding_utils as encoding_utils
from app.utils.rdb_writer import write_rdb_file

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler

class RedisCommand(ABC):
    @abstractmethod
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        pass

class KeysCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        keys = handler.server.get_keys_array()
        return keys

class TypeCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        if key in handler.memory and (not handler.expiration.get(key) or handler.expiration[key] >= time.time()):
            value = handler.memory[key]
            if isinstance(value, list):
                return "+list\r\n"
            elif isinstance(value, str):
                return "+string\r\n"
            elif isinstance(value, set):
                return "+set\r\n"
            else:
                return "+none\r\n"
        elif key in handler.server.streamstore:
            return "+stream\r\n"
        else:
            return "+none\r\n"

class ConfigCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if len(command) < 2:
            return "-ERR wrong number of arguments for 'config' command\r\n"
        
        subcommand = command[1].upper()
        
        if subcommand == "GET":
            if len(command) < 3:
                return "-ERR wrong number of arguments for 'config get' command\r\n"
            
            config_params = command[2:]
            response = []
            for param in config_params:
                response.append(param)
                if param in handler.server.config:
                    value = handler.server.config[param]
                    response.append(value)
                else:
                    response.append("(nil)")
            return encoding_utils.as_array(response)
            
        elif subcommand == "SET":
            if len(command) < 4:
                return "-ERR wrong number of arguments for 'config set' command\r\n"
            
            param = command[2]
            value = command[3]
            
            # Update the configuration
            handler.server.config[param] = value
            
            # Special handling for save configuration
            if param == "save":
                handler.server._parse_save_config()
                
            return "+OK\r\n"
            
        else:
            return f"-ERR unknown subcommand '{subcommand}' for CONFIG command\r\n"

class WaitCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        max_wait_ms = int(command[2])
        num_replicas = int(command[1])
        for writer in handler.server.writers:
            writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
            await writer.drain()
        
        start_time = time.time()
        while handler.server.numacks < num_replicas and (time.time() - start_time) < (max_wait_ms / 1000):
            print(f"NUMACKS: {handler.server.numacks} num_replicas: {num_replicas} max_wait_ms: {max_wait_ms} time: {time.time()} start_time: {start_time}")
            await asyncio.sleep(0.1)
        print("SENDING BACK", handler.server.numacks)
        return f":{handler.server.numacks}\r\n"

class PingCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        return "+PONG\r\n"

class ReplConfCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        writer = handler.writer
        if len(command) > 2 and command[1] == "listening-port":
            handler.server.writers.append(writer)
        elif len(command) > 2 and command[1] == "GETACK":
            response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(handler.offset))}\r\n{handler.offset}\r\n"
            print(f"REPLCONF ACK: {response}")
            return response
        elif len(command) > 2 and command[1] == "ACK":
            print("Incrementing num acks")
            handler.server.numacks += 1
            return ""
        return "+OK\r\n"

class PSyncCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        binary_data = bytes.fromhex(rdb_hex)
        header = f"${len(binary_data)}\r\n"
        handler.writer.write(response.encode())
        handler.writer.write(header.encode())
        handler.writer.write(binary_data)
        await handler.writer.drain()
        handler.server.numacks += 1
        return ""

class InfoCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        if command[1].lower() == "replication":
            if handler.replica_server is None:
                master_replid = encoding_utils.generate_random_string(40)
                master_repl_offset = "0"
                payload = f"role:master\nmaster_replid:{master_replid}\nmaster_repl_offset:{master_repl_offset}"
                response = encoding_utils.as_bulk_string(payload)
                return response
            else:
                return "+role:slave\r\n"
        else:
            return "-ERR unknown INFO section\r\n"

class EchoCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        return f"+{command[1]}\r\n"

    
class DelCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        print("DELETING KEYS", command)
        keys = command[1:]
        count = 0
        for key in keys:
            if key in handler.memory:
                handler.memory.pop(key, None)
                handler.expiration.pop(key, None)
                count += 1
                print(f"DELETING KEY {key}")
            else:
                print(f"KEY {key} NOT FOUND")
        return f":{count}\r\n"
    
class SaveCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        """Execute the SAVE command which creates an RDB file with the current dataset."""
        # Get config values
        dir_path = handler.server.config.get("dir", "")
        filename = handler.server.config.get("dbfilename", "dump.rdb")
        
        # If dir is not set, use current directory
        if not dir_path:
            dir_path = os.getcwd()
            
        # Create full file path
        filepath = os.path.join(dir_path, filename)
        
        # Write RDB file
        try:
            write_rdb_file(
                filepath,
                handler.memory,
                handler.expiration
            )
            
            # Update last save time
            handler.server.last_save_time = int(time.time())
            return "+OK\r\n"
        except Exception as e:
            print(f"Error saving RDB file: {e}")
            return f"-ERR Failed to save RDB file: {str(e)}\r\n"

class BGSaveCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        """Execute the BGSAVE command which saves the DB in background."""
        # Get config values
        dir_path = handler.server.config.get("dir", "")
        filename = handler.server.config.get("dbfilename", "dump.rdb")
        
        # If dir is not set, use current directory
        if not dir_path:
            dir_path = os.getcwd()
            
        # Create full file path
        filepath = os.path.join(dir_path, filename)
        
        # Create task to save in background
        asyncio.create_task(self._save_in_background(
            handler,
            handler.memory.copy(),
            handler.expiration.copy(),
            filepath
        ))
        
        return "+Background saving started\r\n"
    
    async def _save_in_background(self, handler, memory, expiration, filepath):
        """Save the RDB file in a background task."""
        # Use an executor to perform the blocking file operation
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: write_rdb_file(filepath, memory, expiration)
            )
            
            # Update last save time
            handler.server.last_save_time = int(time.time())
            print(f"Background save completed successfully")
        except Exception as e:
            print(f"Error in background save: {e}")

class LastSaveCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        """Return the UNIX timestamp of the last successful save."""
        return f":{handler.server.last_save_time}\r\n"

class FlushAllCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        handler.memory.clear()
        handler.expiration.clear()
        return "+OK\r\n"
    
class UnknownCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        return "-ERR unknown command\r\n"