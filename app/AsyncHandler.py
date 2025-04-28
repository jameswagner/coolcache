import asyncio
import logging


from typing import TYPE_CHECKING

from app.commands import hash_map_commands, list_commands, set_commands, sorted_set_commands, stream_commands, string_commands, commands
import app.utils.encoding_utils as encoding_utils
if TYPE_CHECKING:
    from .AsyncServer import AsyncServer

class AsyncRequestHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: 'AsyncServer'):
        self.reader = reader
        self.writer = writer
        self.server = server
        self.memory = server.memory
        self.expiration = server.expiration
        self.replica_server = server.replica_server
        self.replica_port = server.replica_port
        self.offset = 0
        self.command_map = {
            "PING": commands.PingCommand(),
            "ECHO": commands.EchoCommand(),
            "SET": string_commands.SetCommand(),
            "GET": string_commands.GetCommand(),
            "INFO": commands.InfoCommand(),
            "REPLCONF": commands.ReplConfCommand(),
            "PSYNC": commands.PSyncCommand(),
            "WAIT": commands.WaitCommand(),
            "CONFIG": commands.ConfigCommand(),
            "KEYS": commands.KeysCommand(),
            "TYPE": commands.TypeCommand(),
            "XADD": stream_commands.XAddCommand(),
            "XRANGE": stream_commands.XRangeCommand(),
            "XREAD": stream_commands.XReadCommand(),
            "LPUSH": list_commands.LPushCommand(),
            "RPUSH": list_commands.RPushCommand(),
            "LPOP": list_commands.LPopCommand(),
            "RPOP": list_commands.RPopCommand(),
            "LLEN": list_commands.LLenCommand(),
            "LINDEX": list_commands.LIndexCommand(),
            "LINSERT": list_commands.LInsertCommand(),
            "LPUSHX": list_commands.LPushXCommand(),
            "RPUSHX": list_commands.RPushXCommand(),
            "LRANGE": list_commands.LRangeCommand(),
            "LSET": list_commands.LSetCommand(),
            "DEL": commands.DelCommand(),
            "SADD": set_commands.SAddCommand(),
            "SCARD": set_commands.SCardCommand(),
            "SISMEMBER": set_commands.SIsMemberCommand(),
            "SMEMBERS": set_commands.SMembersCommand(),
            "SREM": set_commands.SRemCommand(),
            "SPOP": set_commands.SPopCommand(),
            "SUNION": set_commands.SUnionCommand(),
            "SINTER": set_commands.SInterCommand(),
            "SDIFF": set_commands.SDiffCommand(),
            "SMOVE": set_commands.SMoveCommand(),
            "MSET": string_commands.MSetCommand(),
            "MGET": string_commands.MGetCommand(),
            "INCR": string_commands.IncrCommand(),
            "DECR": string_commands.DecrCommand(),
            "INCRBY": string_commands.IncrByCommand(),
            "DECRBY": string_commands.DecrByCommand(),
            "APPEND": string_commands.AppendCommand(),
            "HSET": hash_map_commands.HSetCommand(),
            "HGET": hash_map_commands.HGetCommand(),
            "HGETALL": hash_map_commands.HGetAllCommand(),
            "ZADD": sorted_set_commands.ZAddCommand(),
            "ZREM": sorted_set_commands.ZRemCommand(),
            "ZRANGE": sorted_set_commands.ZRangeCommand(),
            "ZRANGEBYSCORE": sorted_set_commands.ZRangeByScoreCommand(),
            "ZRANK": sorted_set_commands.ZRankCommand(),
            "ZREVRANK": sorted_set_commands.ZRevRankCommand(),
            "ZSCORE": sorted_set_commands.ZScoreCommand(),
            "ZCARD": sorted_set_commands.ZCardCommand(),
            "ZCOUNT": sorted_set_commands.ZCountCommand(),
            "SAVE": commands.SaveCommand(),
            "BGSAVE": commands.BGSaveCommand(),
            "FLUSHALL": commands.FlushAllCommand(),
            "LASTSAVE": commands.LastSaveCommand(),
        }

    async def process_request(self) -> None:
        while True:
            request = await self.reader.read(1024)
            if not request:
                break
            logging.info(f"Request: {request}")
            await self.handle_request(request)

    async def handle_request(self, request: bytes) -> None:
        command_list, lengths = encoding_utils.parse_redis_protocol(request)
        
        if not command_list:
            logging.info("Received invalid data")
            return

        for index, cmd in enumerate(command_list):
            cmd_name = cmd[0].upper()  # Command names are case-insensitive
            command_class = self.command_map.get(cmd_name, commands.UnknownCommand())

            if cmd_name in self.command_map:
                response = await command_class.execute(self, cmd)
            else:
                response = await commands.UnknownCommand.execute(self, cmd)

            if self.replica_server is not None and self.writer.get_extra_info("peername")[1] == self.replica_port:
                if response.startswith("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK"):
                    self.writer.write(response.encode())
                    await self.writer.drain()
                self.offset += lengths[index]
            else:
                if response:
                    print(f"sending response: {response} to {self.writer.get_extra_info('peername')} command: {cmd}")
                    self.writer.write(response.encode())
                    await self.writer.drain()
                self.offset += lengths[index]