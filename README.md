# CoolCache
This project aims to implement a Redis-like server in Python, providing a subset of Redis functionality. It allows clients to interact with the server using the Redis protocol and supports various Redis commands for data manipulation and retrieval. I gratefully acknowledge CodeCrafters for providing the framework for setting up this cache, and I aim to continue adding features to this and exploring more about caching and Redis functionality. 

## Features

Basic key-value operations (SET, GET)
Expiration of keys using the PX option in the SET command
Handling of expired keys in the GET command
Pub/Sub functionality (PUBLISH, SUBSCRIBE)
Partial implementation of Redis Streams (XADD, XRANGE, XREAD)
Replication of data from a master server
Complete RDB file parsing and writing to load and save datasets
Support for most functions with HashMaps, Lists, Sets, and Sorted Sets
RDB persistence (SAVE, BGSAVE, automatic background saving)

## Getting Started

## Installation

### Clone the repository:
`git clone https://github.com/jameswagner/coolcache.git`

### Change into the project directory:
`cd coolcache`

### Install the required dependencies:
`pip install -r requirements.txt`

### Install: 
`pip install .`

## Usage

### Server
To start the Coolcache server, run the following command:
`python .\app\main.py --port 6379`

This will start the server on the default port 6379. You can specify a different port using the `--port` option.

Additional server options:
- `--dir <directory>` - Specify the directory where RDB files are stored
- `--dbfilename <filename>` - Specify the name of the RDB file
- `--replicaof <host> <port>` - Make the server a replica of another CoolCache instance

### Client
To connect to the server, you can use the coolcache client library in Python or the command line.

#### Command line
To use the command line, the port and hostname arguments should be specified, or the 
`COOLCACHE_HOST` and `COOLCACHE_PORT` environment variables should be set.

eg to set a variable
`coolcache-cli --host localhost --port 6379 SET foo bar`
or if the environment variables are already set: 
`coolcache-cli SET foo bar`

to open a shell, enter `coolcache` without any command (only the `--port` and `--host` if the environment variables are not set)

#### Python
The Python functions impemented are in app/client/coolcache_client.py
you can execute an import: 
`from app.client.coolcache_client import CoolClient`

The initialize a client instance with your port and username:
`client = CoolClient("localhost", 6379)`

and call functions on this client:
`client.set_value("foo", "bar")`

### Persistence

CoolCache supports RDB persistence for data durability. The implementation includes:

- Complete RDB file parsing to load data from existing Redis RDB files
- Full RDB file writing capability with support for all Redis data types (strings, lists, sets, hashes, sorted sets)
- Proper handling of expiry times in both seconds and milliseconds

You can use the following commands to manage persistence:

- `SAVE` - Synchronously save the dataset to disk (blocking operation)
- `BGSAVE` - Save the dataset to disk in the background
- `LASTSAVE` - Get the UNIX timestamp of the last successful save operation
- `CONFIG GET save` - View the current auto-save configuration
- `CONFIG SET save "<seconds> <changes>"` - Set auto-save configuration (e.g., "900 1 300 10 60 10000")

By default, CoolCache will automatically save the dataset when a certain number of changes have been made within a specific time period, following Redis's default pattern:

- After 900 seconds (15 minutes) if at least 1 change was made
- After 300 seconds (5 minutes) if at least 10 changes were made
- After 60 seconds if at least 10000 changes were made

## TODO
Here are the top Redis functionalities that need to be implemented:

Complete implementation of Redis Streams (XDEL, XLEN, XINFO, etc.)
Implement AOF persistence (logging every write operation)
Transactions (MULTI, EXEC, DISCARD)
Lua scripting (EVAL, EVALSHA, SCRIPT)
Cluster support (distributing data across multiple nodes)