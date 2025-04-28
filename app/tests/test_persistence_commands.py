import os
import time
import asyncio
import unittest
import tempfile
from pathlib import Path

from app.AsyncServer import AsyncServer
from app.AsyncHandler import AsyncRequestHandler
from app.commands.commands import SaveCommand, BGSaveCommand, LastSaveCommand
from app.utils.rdb_parser import parse_redis_file


class MockStreamReader:
    def __init__(self):
        pass
    
    async def read(self, _):
        return b""


class MockStreamWriter:
    def __init__(self):
        self.buffer = b""
        
    def write(self, data):
        self.buffer += data
        
    async def drain(self):
        pass
    
    def get_extra_info(self, _):
        return ("127.0.0.1", 12345)


class TestPersistenceCommands(unittest.TestCase):
    """Test the persistence-related commands."""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        self.server = AsyncServer(
            host="127.0.0.1",
            port=6379,
            dir=str(self.temp_path),
            dbfilename="test.rdb"
        )
        
        self.reader = MockStreamReader()
        self.writer = MockStreamWriter()
        self.handler = AsyncRequestHandler(self.reader, self.writer, self.server)
        
        # Add some test data
        self.server.memory["test_key"] = "test_value"
        self.server.memory["list_key"] = ["item1", "item2"]
        self.server.expiration["test_key"] = time.time() + 3600  # Expire in 1 hour
        
    def tearDown(self):
        self.temp_dir.cleanup()
        self.loop.close()
    
    def test_save_command(self):
        """Test that the SAVE command creates an RDB file."""
        save_cmd = SaveCommand()
        self.loop.run_until_complete(save_cmd.execute(self.handler, ["SAVE"]))
        
        # Check that file was created
        rdb_path = self.temp_path / "test.rdb"
        self.assertTrue(rdb_path.exists())
        
        # Check that last_save_time was updated
        self.assertGreater(self.server.last_save_time, 0)
        
        # Check that we can read the data back
        data, expiry = parse_redis_file(rdb_path)
        self.assertEqual(data.get("test_key"), "test_value")
    
    def test_bgsave_command(self):
        """Test that the BGSAVE command creates an RDB file in the background."""
        bgsave_cmd = BGSaveCommand()
        self.loop.run_until_complete(bgsave_cmd.execute(self.handler, ["BGSAVE"]))
        
        # Give it a moment to complete the background task
        self.loop.run_until_complete(asyncio.sleep(0.5))
        
        # Check that file was created
        rdb_path = self.temp_path / "test.rdb"
        self.assertTrue(rdb_path.exists())
        
        # Check that last_save_time was updated
        self.assertGreater(self.server.last_save_time, 0)
    
    def test_lastsave_command(self):
        """Test that the LASTSAVE command returns the last save time."""
        # First save to set last_save_time
        save_cmd = SaveCommand()
        self.loop.run_until_complete(save_cmd.execute(self.handler, ["SAVE"]))
        
        initial_save_time = self.server.last_save_time
        
        # Now check LASTSAVE
        lastsave_cmd = LastSaveCommand()
        result = self.loop.run_until_complete(
            lastsave_cmd.execute(self.handler, ["LASTSAVE"])
        )
        
        # Parse the integer from the Redis protocol format
        result_time = int(result.strip('\r\n').lstrip(':'))
        
        self.assertEqual(result_time, initial_save_time)


if __name__ == "__main__":
    unittest.main() 