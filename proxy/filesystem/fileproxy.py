import argparse
import os
import socket
import sys
from datetime import datetime
import threading
import time
from typing import NoReturn
from dotenv import load_dotenv

from agent.log import Log, Record
from networking import AddRecordNetworkMessage, GetLogPositionNetworkMessage, NetworkConnection, NetworkMessage, NetworkMessageDeserializer, SendingConnection
from proxy.filesystem.contentmanager import FileLogContentManager
from proxy.logproxy import LogProxyMessageDeserializer
from proxy.filesystem.logfilewatcher import FileRecordCollector

if sys.platform == 'linux' or sys.platform == 'linux2':
    from proxy.filesystem.unix.unixfilewatcher import UnixFileWatcherManager as PlatformFileWatcher
#elif sys.platform == 'win32':
#    pass
else:
    raise ValueError(f'platform "{sys.platform}" not supported')



load_dotenv('.env')

class ClientConnection(SendingConnection):
    """Wrapper for a connection to the agent."""

    def __init__(self, deserializer: NetworkMessageDeserializer, address: str, port: int):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.connect((address, port))
        self.__connection = NetworkConnection(deserializer, sock, address, port)

    def receive_messages(self) -> None:
        """Processes incoming messages."""
        while self.__connection.receive():
            pass

    def send(self, message: NetworkMessage) -> None:
        """
        Sends message.
        This method is thread-safe.
        """
        self.__connection.send(message)


class RecordSender(FileRecordCollector):
    """
    Implementation of FileRecordCollector immediately sending records agent listening on specified address.
    """

    def __init__(self, client: SendingConnection):
        self.__client = client

    def request_initialize(self, filename: str) -> None:
        log = Log(filename)
        message = GetLogPositionNetworkMessage(log)
        self.__client.send(message)
        print(f'Request log position for {filename}')

    def on_record_added(self, filename: str, timestamp: datetime, file_pos: int, contents: str) -> None:
        log = Log(filename)
        record = Record(log, contents, timestamp)
        message = AddRecordNetworkMessage(record)
        self.__client.send(message)
        print(f'{filename}: [{timestamp}] offset:{file_pos} {contents}')

    def on_empty_write(self, filename: str, timestamp: datetime, file_pos: int) -> None:
        print(f'WARNING: empty write to "{filename}" [{timestamp}] offset:{file_pos}')

    def on_file_disappeared(self, filename: str, timestamp: datetime, file_pos: int) -> None:
        print(f'WARNING: file "{filename}" disappeared [{timestamp}] offset:{file_pos}')

def listen_watches(watcher: PlatformFileWatcher) -> NoReturn:
    try:
        watcher.listen()
    except Exception as e:
        print(e)
        os._exit(1)


parser = argparse.ArgumentParser()
parser.add_argument('--agentAddr', '-a', type=str)
parser.add_argument('--agentPort', '-p', type=int)
parser.add_argument('--watch', '-w', type=str, nargs='+')
args = parser.parse_args()


address = args.agentAddr or os.environ['AGENT_ADDR']
port = args.agentPort or int(os.environ['AGENT_PORT'])
watched = args.watch or os.environ['FILEPROXY_WATCHED_PATHS'].split(';')

print(f'Agent host - {address}:{port}')
print(f'Paths to watch: [ {str.join(", ", watched)} ]')

deserializer = LogProxyMessageDeserializer()
client = ClientConnection(deserializer, address, port)
collector = RecordSender(client)
with PlatformFileWatcher(collector) as watcher:
    content_manager = FileLogContentManager(watcher, client)
    deserializer.set_content_manager(content_manager)
    deserializer.set_watch_manager(watcher)

    for path in watched:
        watcher.begin_watch(path)
    
    listen_thread = threading.Thread(target=listen_watches, args=[ watcher ])
    listen_thread.start()
    
    try:
        while True:
            client.receive_messages()
            time.sleep(10 / 1000)
    except Exception as e:
        print(e)
        os._exit(1)
