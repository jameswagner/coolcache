# CoolCache
This project aims to implement a Redis-like server in Python, providing a subset of Redis functionality. It allows clients to interact with the server using the Redis protocol and supports various Redis commands for data manipulation and retrieval. I gratefully acknowledge CodeCrafters for providing the framework for setting up this cache, and I aim to continue adding features to this and exploring more about caching and Redis functionality. 

## Features

Basic key-value operations (SET, GET)
Expiration of keys using the PX option in the SET command
Handling of expired keys in the GET command
Pub/Sub functionality (PUBLISH, SUBSCRIBE)
Partial implementation of Redis Streams (XADD, XRANGE, XREAD)
Replication of data from a master server
RDB file parsing to load the initial dataset
Support for most functions with HashMaps, Lists, Sets, and Sorted Sets

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

## TODO
Here are the top 5 Redis functionalities that need to be implemented:

Complete implementation of Redis Streams (XDEL, XLEN, XINFO, etc.)
Persistence (saving the dataset to disk using RDB or AOF)
Transactions (MULTI, EXEC, DISCARD)
Lua scripting (EVAL, EVALSHA, SCRIPT)
Cluster support (distributing data across multiple nodes)