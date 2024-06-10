import sys
import os
import argparse
from coolcache_client import CoolClient

def repl_shell(client: CoolClient):
    while True:
        command = input('coolcache> ')
        if command.lower() == 'quit':
            break
        response = client.send_command(command + '\n')
        print(response)


def create_client():
    parser = argparse.ArgumentParser(description='CoolCache CLI')
    parser.add_argument('command', nargs='*', help='Command and its arguments')
    parser.add_argument('--host', help='CoolCache server host')
    parser.add_argument('--port', type=int, help='CoolCache server port')

    args = parser.parse_args()

    host = args.host or os.environ.get('COOLCACHE_HOST')
    port = args.port or os.environ.get('COOLCACHE_PORT')

    if not host or not port:
        print('Error: CoolCache host and port must be specified either as command-line arguments or environment variables.')
        sys.exit(1)

    client = CoolClient(host, port)
    return client, args


def main():

    client, args = create_client()

    if not args.command:
        repl_shell(client)
    else:
        command = ' '.join(args.command) + '\n'
        response = client.send_command(command)
        print(response)

    client.close()

if __name__ == '__main__':
    main()