import os, time
from typing import NoReturn

from agent.log import Log
from agent.proxyconnection import ContentRequest, ProxyConnection



class AgentContentRequest:
    """
    Class for aggregating ContentRequests sent to all connected log proxies.
    """

    def __init__(self, requests: list[tuple[ProxyConnection, ContentRequest]]):
        self.__requests = requests
        self.__lead = None

    def get_lead(self) -> ContentRequest:
        """
        Returns 'leading' ContentRequest.
        Lead is the first ContentRequest that returned found status. Other ContentRequests are dropped.
        """
        if self.__lead is None:
            self.__try_resolve_lead()
        return self.__lead

    def is_dead(self) -> bool:
        """Returns if all ContentRequests are dead - none of them found specified log."""
        return self.__lead is None and len(self.__requests) == 0

    def __try_resolve_lead(self) -> None:
        """Runs logic to check if any request found specified log. If so, drops all other requests."""
        for i in reversed(range(len(self.__requests))):
            (_, req) = self.__requests[i]
            s = req.get_status()
            if s == ContentRequest.Status.RECEIVING or s == ContentRequest.Status.CLOSED:
                self.__select_lead(req)
                break
            elif s == ContentRequest.Status.NOT_FOUND or s == ContentRequest.Status.DROPPED:
                del self.__requests[i]

    def __select_lead(self, lead: ContentRequest) -> None:
        """Selects specified request as leading. Drops all others."""
        self.__lead = lead
        for (con, req) in self.__requests:
            if req != lead:
                con.drop_content_request(req)
        self.__requests.clear()


class LfbAgent:
    """
    Class representing working agent, containing common functionality for agent submodules.
    """

    def __init__(self):
        self.__connections = list[ProxyConnection]()

    def handle_clients(self) -> NoReturn:
        """
        Runs agent's event loop.
        This method never returns.
        """
        try:
            while True:
                for con in self.__connections:
                    try:
                        con.receive_messages()
                    except ConnectionError:
                        print(f'Connection with {con.get_address()}:{con.get_port()} lost')
                        self.__connections.remove(con)
                time.sleep(10 / 1000)
        except Exception as e:
            print(e)
            os._exit(1)

    def add_client(self, client: ProxyConnection) -> None:
        """Registers a new client for handling in event loop."""
        self.__connections.append(client)

    def request_log_content(self, log: Log, begin_record: int, end_record: int) -> AgentContentRequest:
        """Creates and sends request for specified log content."""
        requests = list[tuple[ProxyConnection, ContentRequest]]()
        for con in self.__connections:
            req = con.request_content(log, begin_record, end_record)
            requests.append((con, req))
        agentreq = AgentContentRequest(requests)
        return agentreq