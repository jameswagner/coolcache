import re
import socket

from app.utils.encoding_utils import generate_redis_array, parse_element, parse_redis_protocol


class CoolClient:
    def __init__(self, host="localhost", port=6379):
        self.host = host
        self.port = port
        print(f'Connecting to {host}:{port}')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def close(self):
        self.sock.close()
        
    def send_command(self, command):
        command_list = re.split(r'\s+', command.strip())
        redis_command = generate_redis_array(string="", lst=command_list)
        self.sock.sendall(redis_command.encode())
        response = self.sock.recv(1024)
        response = parse_element(response, 0)
        return response[0]

    def keys(self):
        command = 'KEYS\n'
        response = self.send_command(command)
        return response

    def type(self, key):
        command = f'TYPE {key}\n'
        response = self.send_command(command)
        return response

    def config(self, *params):
        command = f'CONFIG {" ".join(params)}\n'
        response = self.send_command(command)
        return response

    def wait(self, num_replicas, max_wait_ms):
        command = f'WAIT {num_replicas} {max_wait_ms}\n'
        response = self.send_command(command)
        return response

    def ping(self):
        command = 'PING\n'
        response = self.send_command(command)
        return response

    def replconf(self, *params):
        command = f'REPLCONF {" ".join(params)}\n'
        response = self.send_command(command)
        return response

    def psync(self, replication_id, offset):
        command = f'PSYNC {replication_id} {offset}\n'
        response = self.send_command(command)
        return response

    def info(self, section):
        command = f'INFO {section}\n'
        response = self.send_command(command)
        return response

    def echo(self, message):
        command = f'ECHO {message}\n'
        response = self.send_command(command)
        return response

    def set_value(self, key, value, *options):
        command = f'SET {key} {value}'
        if options:
            command += f' {" ".join(options)}'
        command += '\n'
        response = self.send_command(command)
        return response

    def get_value(self, key):
        command = f'GET {key}\n'
        response = self.send_command(command)
        return response

    def xadd(self, stream_key, stream_id, *field_value_pairs):
        command = f'XADD {stream_key} {stream_id} {" ".join(field_value_pairs)}\n'
        response = self.send_command(command)
        return response

    def xrange(self, stream_key, lower, upper):
        command = f'XRANGE {stream_key} {lower} {upper}\n'
        response = self.send_command(command)
        return response

    def xread(self, stream_keys, stream_ids, blocking=False, block_interval=0, only_new=False):
        command = 'XREAD'
        if blocking:
            command += f' BLOCK {block_interval}'
        for stream_key in stream_keys:
            command += f' {stream_key}'
        if blocking and only_new:
            command += ' $'
        else:
            for stream_id in stream_ids:
                command += f' {stream_id}'
        command += '\n'
        response = self.send_command(command)
        return response
