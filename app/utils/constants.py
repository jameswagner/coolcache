NOT_FOUND_RESPONSE = "$-1\r\n"
NIL_RESPONSE = "+nil\r\n"
SYNTAX_ERROR = "-ERR syntax error\r\n"
WRONG_TYPE_RESPONSE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
NON_INT_ERROR = "-ERR value is not an integer or out of range\r\n"
EMPTY_ARRAY_RESPONSE = "+(empty array)"
OUT_OF_RANGE_RESPONSE = "+ERR index out of range"
FLOAT_ERROR_MESSAGE = "-ERR value is not a valid float\r\n"

# RDB Constants
REDIS_RDB_MAGIC = b'REDIS'
REDIS_VERSION = b'0011'  # Redis version 7.2
TYPE_STRING = 0
TYPE_LIST = 1
TYPE_SET = 2
TYPE_HASH = 3
TYPE_ZSET = 4
TYPE_STREAM = 6