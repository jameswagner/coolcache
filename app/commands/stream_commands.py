from app.commands.commands import RedisCommand
from app.utils import stream_utils
from typing import List, TYPE_CHECKING


if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler
    
class XAddCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        stream_key = command[1]
        stream_id = command[2]
        stream_id = stream_utils.generate_stream_id(stream_key, stream_id, handler.server)
        err_message = stream_utils.validate_stream_id(stream_key, stream_id, handler.server)
        if err_message:
            return err_message
        if stream_key not in handler.server.streamstore:
            handler.server.streamstore[stream_key] = {}
        stream_id_parts = stream_id.split("-")
        entry_number = int(stream_id_parts[0])
        sequence_number = int(stream_id_parts[1])
        if entry_number not in handler.server.streamstore[stream_key]:
            handler.server.streamstore[stream_key][entry_number] = {}

        handler.server.streamstore[stream_key][entry_number][sequence_number] = command[3:]
        return f"${len(stream_id)}\r\n{stream_id}\r\n"
    

class XReadCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        stream_keys, stream_ids = None, None
        if command[1].lower() == "block":
            stream_keys, stream_ids = await stream_utils.block_read(int(command[2]), command, handler.server)        
            
        if not stream_keys or not stream_ids:
            stream_keys, stream_ids = stream_utils._get_stream_keys_and_ids(command, handler.server)
        
        ret_string = f"*{len(stream_keys)}\r\n"
        for stream_key, stream_id in zip(stream_keys, stream_ids):
            ret_string += stream_utils.get_one_xread_response(stream_key, stream_id, handler.server)
        return ret_string
    
class XRangeCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        stream_key = command[1]
        lower, upper = command[2], command[3]
        if lower == "-":
            lower = "0-0"

        none_string = "+none\r\n"
        if stream_key not in handler.server.streamstore:
            print(f"Stream key '{stream_key}' not found in streamstore")
            return none_string

        streamstore = handler.server.streamstore[stream_key]

        keys = list(streamstore.keys())
        
        if upper == "+":
            upper = f"{keys[-1]}-{list(streamstore[keys[-1]].keys())[-1]}"
        
        lower_outer, lower_inner = int(lower.split("-")[0]), int(lower.split("-")[1])
        upper_outer, upper_inner = int(upper.split("-")[0]), int(upper.split("-")[1])
        
        start_index, end_index = stream_utils.find_outer_indices(keys, lower_outer, upper_outer)
        print(f"Start index: {start_index}, End index: {end_index}")
        if start_index == -1 or end_index == -1 or start_index >= len(keys) or end_index < 0 or start_index > end_index:
            print("Invalid range indices")
            return none_string
        
        streamstore_start_index = stream_utils.find_inner_start_index(streamstore, keys, start_index, lower_outer, lower_inner)
        streamstore_end_index = stream_utils.find_inner_end_index(streamstore, keys, end_index, upper_outer, upper_inner)
        print(f"Streamstore start index: {streamstore_start_index}, Streamstore end index: {streamstore_end_index}")
        if streamstore_start_index == -1 or streamstore_end_index == -1:
            print("Invalid inner indices")
            return none_string

        elements = stream_utils.extract_elements(streamstore, keys, start_index, end_index, streamstore_start_index, streamstore_end_index)
        ret_string = f"*{len(elements)}\r\n"
        for key, value in elements.items():
            ret_string += f"*2\r\n${len(key)}\r\n{key}\r\n{handler.server.as_array(value)}"
        print(f"Ret string: {ret_string}")
        return ret_string