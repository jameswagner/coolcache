import asyncio
import logging
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Tuple

from app.AsyncHandler import AsyncRequestHandler
from app.utils.rdb_parser import parse_redis_file
from app.utils.rdb_writer import write_rdb_file
import app.utils.encoding_utils as encoding_utils


class AsyncServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, replica_server: str = None, replica_port: int = None, dir: str = '', dbfilename: str = ''):
        self.host = host
        self.port = port
        self.replica_server = replica_server
        self.replica_port = replica_port
        self.memory = {}
        self.expiration = {}
        self.streamstore = {}
        self.writers = []
        self.inner_server = None
        self.numacks = 0
        self.config = {
            "dir": dir, 
            "dbfilename": dbfilename,
            "save": "900 1 300 10 60 10000"  # Default Redis-like save configuration
        }
        self.last_save_time = 0  # Unix timestamp of the last successful save
        self.changes_since_last_save = 0
        self.save_schedule = []  # List of (seconds, changes) pairs
        self._parse_save_config()
        self._auto_save_task = None

    def _parse_save_config(self) -> None:
        """Parse the save configuration and set up the save schedule."""
        save_config = self.config.get("save", "")
        if not save_config:
            # No auto save
            self.save_schedule = []
            return
        
        values = save_config.split()
        if len(values) % 2 != 0:
            logging.warning("Invalid save configuration: %s", save_config)
            return
        
        self.save_schedule = []
        for i in range(0, len(values), 2):
            seconds = int(values[i])
            changes = int(values[i + 1])
            self.save_schedule.append((seconds, changes))

    def register_change(self) -> None:
        """Register a change to the database and trigger auto-save if needed."""
        self.changes_since_last_save += 1
        self._check_save_conditions()

    def _check_save_conditions(self) -> None:
        """Check if auto-save conditions are met and trigger a save if needed."""
        if not self.save_schedule or not self.last_save_time:
            return
        
        current_time = time.time()
        time_since_last_save = current_time - self.last_save_time
        
        for seconds, changes in self.save_schedule:
            if (time_since_last_save >= seconds and 
                self.changes_since_last_save >= changes):
                self._trigger_auto_save()
                break

    def _trigger_auto_save(self) -> None:
        """Trigger an automatic background save."""
        if self._auto_save_task and not self._auto_save_task.done():
            # A save is already in progress
            return
        
        dir_path = self.config.get("dir", "")
        filename = self.config.get("dbfilename", "dump.rdb")
        
        # If dir is not set, use current directory
        if not dir_path:
            dir_path = os.getcwd()
            
        # Create full file path
        filepath = os.path.join(dir_path, filename)
        
        # Start background save
        self._auto_save_task = asyncio.create_task(self._auto_save(filepath))

    async def _auto_save(self, filepath: str) -> None:
        """Perform an automatic background save."""
        logging.info("Auto-saving RDB to %s", filepath)
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(
            None,
            lambda: write_rdb_file(
                self.memory.copy(),
                self.expiration.copy(),
                self.streamstore.copy(),
                filepath
            )
        )
        
        if success:
            self.last_save_time = int(time.time())
            self.changes_since_last_save = 0
            logging.info("Auto-save completed successfully")
        else:
            logging.error("Auto-save failed")

    @classmethod
    async def create(cls, host: str = "127.0.0.1", port: int = 6379, replica_server: str = None, replica_port: int = None, dir: str = '', dbfilename: str = ''):
        instance = cls(host, port, replica_server, replica_port, dir, dbfilename)
        if(dir and dbfilename):
            instance.memory, instance.expiration = parse_redis_file(Path(dir) / dbfilename)
            instance.last_save_time = time.time()  # Set last_save_time if we loaded from a file
        instance.inner_server = await instance.start()
        
        if replica_server is not None and replica_port is not None:
            reader, writer = await asyncio.open_connection(replica_server, replica_port)
            response = await instance.send_ping(reader, writer)
            if response != "+PONG\r\n":
                raise ValueError("Failed to receive PONG from replica server")
            

            await instance.send_replconf_command(reader, writer, port)
            await instance.send_additional_replconf_command(reader, writer)
            await instance.send_psync_command(reader, writer)
            await asyncio.create_task(instance.accept_connections(reader, writer))
            #psync_response = await reader.read(1024)
            
            #writer.close()
            #await writer.wait_closed()
        async with instance.inner_server as server:
            print("SERVER STARTED")
            await server.serve_forever()
            
        return instance

    async def send_replconf_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int) -> None:
        replconf_command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + str(port) + "\r\n"
        writer.write(replconf_command.encode())
        await writer.drain()
        replconf_response = await reader.read(1024)
        if replconf_response.decode() != "+OK\r\n":
            raise ValueError("Failed to receive +OK response from REPLCONF command")

    async def send_additional_replconf_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        replconf_command_additional = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        writer.write(replconf_command_additional.encode())
        await writer.drain()
        replconf_response_additional = await reader.read(1024)
        if replconf_response_additional.decode() != "+OK\r\n":
            raise ValueError("Failed to receive +OK response from additional REPLCONF command")

    async def send_psync_command(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        writer.write(psync_command.encode())
        await writer.drain()
        #psync_response = await reader.read(1024)
        #if not psync_response.startswith(b"+FULLRESYNC"):
            #raise ValueError("Failed to receive +FULLRESYNC response from PSYNC command")

    async def send_ping(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> str:
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await reader.read(1024)
        return response.decode()

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.accept_connections, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        logging.info(f"Server started at http://{addr[0]}:{addr[1]}")
        return server

    async def accept_connections(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Connected by {addr}")
        request_handler = AsyncRequestHandler(reader, writer, self)
        await request_handler.process_request()
        
    def get_keys_array(self):
        """Get array of all keys in the memory."""
        if self.config.get("dir") and self.config.get("dbfilename"):
            try:
                hash_map, _ = parse_redis_file(Path(self.config["dir"]) / self.config["dbfilename"])
                encoded_keys = encoding_utils.as_array(hash_map.keys())
                return encoded_keys
            except Exception as e:
                logging.error(f"Error reading RDB file: {e}")
                return encoding_utils.as_array(self.memory.keys())
        else:
            return encoding_utils.as_array(self.memory.keys())



 

