import socket

from agent.collector import LogCollector
from agent.proxyconnection import ProxyConnection


class AgentServer:
    """
    Class for accepting and handling connections from Log Proxies.
    """

    def __init__(self, collector: LogCollector, address: str, port: int):
        self.__collector = collector
        self.__sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__sock.bind((address, port))
        self.__sock.listen()

    def accept_new(self) -> ProxyConnection:
        """Waits for incoming proxy connection. Then returns a new ProxyConnection wrapper this this connection."""
        connection, addr = self.__sock.accept()
        return ProxyConnection(self.__collector, connection, addr[0], addr[1])
