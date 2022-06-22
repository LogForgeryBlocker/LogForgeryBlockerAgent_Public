from queue import Empty, SimpleQueue


from agent.collector import LogCollector
from networking import *


class ContentRequest:
    """
    Class representing request for log contents sent to specified log proxy.
    """

    class Status(Enum):
        PENDING = 0,
        """Request is pending - no status message have been received back yet."""
        RECEIVING = 1,
        """Request found the log - records are being received at the moment."""
        CLOSED = 2,
        """Request is closed - all records were received."""
        NOT_FOUND = 3,
        """Request is closed - log was not found."""
        DROPPED = 4,
        """Request is dropped - no records will be received."""

    def __init__(self, request_id: int, log: Log, begin_record: int, end_record: int):
        self.__request_id = request_id
        self.__log = log
        self.__begin_record = begin_record
        self.__next_record_ind = begin_record
        self.__end_record = end_record
        self.__status = ContentRequest.Status.PENDING
        self.__content = SimpleQueue[str]()
        self.__mutex = Lock()

    def get_id(self) -> int:
        """Returns id for this request."""
        return self.__request_id

    def get_status(self) -> Status:
        """Returns current status of this request"""
        with self.__mutex:
            return self.__status

    def pop_record(self) -> str:
        """Returns next record from received record queue. Returns None if queue is empty."""
        if self.__content.empty():
            return None
        try:
            return self.__content.get(False)
        except Empty:
            return None

    def get_next_record_index(self) -> int:
        """Returns absolute index of next received record."""
        return self.__next_record_ind

    def got_all_requested_records(self) -> bool:
        """Returns whether all records that were requested, have already been received."""
        return self.__next_record_ind == self.__end_record + 1

    def is_finished(self) -> bool:
        """Returns whether all records that were requested, have already been received and popped from the record queue."""
        return self.got_all_requested_records() and self.__content.empty()

    def add_record(self, record: str) -> None:
        """Adds a new record to the record queue."""
        self.__content.put(record)
        self.__next_record_ind += 1

    def set_status(self, status: Status) -> None:
        """Sets a new request status."""
        with self.__mutex:
            self.__status = status


class ContentRequestHandler:
    """
    Class providing functionality for creating, handling, and closing requests for log contents.
    """

    def __init__(self):
        self.__requests = dict[int, ContentRequest]()
        self.__request_mutex = Lock()
        self.__next_request_id = 0

    def create_request(self, log: Log, begin_record: int, end_record: int) -> ContentRequest:
        """Creates a new content request for record range in specified log."""
        with self.__request_mutex:
            id = self.__get_next_request_id()
            req = ContentRequest(id, log, begin_record, end_record)
            self.__requests[id] = req
            return req

    def add_records(self, request_id: int, begin_record: int, records: Iterable[str]) -> None:
        """Adds specified records to the request with specified id."""
        with self.__request_mutex:
            req = self.__requests.get(request_id)
            if req is None:
                return
            if req.got_all_requested_records():
                raise ValueError('got next records for a request, although all requested records have already been sent')
            if req.get_next_record_index() != begin_record:
                raise ValueError(f'invalid begin_record, expected:{req.get_next_record_index()} got:{begin_record}')
            for record in records:
                req.add_record(record)

    def set_status(self, request_id: int, status: ContentRequest.Status) -> None:
        """Sets status of the request with specified id."""
        with self.__request_mutex:
            req = self.__requests.get(request_id)
            if req is None:
                return
            req.set_status(status)
            if status == ContentRequest.Status.NOT_FOUND or status == ContentRequest.Status.CLOSED or status == ContentRequest.Status.DROPPED:
                del self.__requests[request_id]

    def __get_next_request_id(self) -> int:
        """Returns a new unique request id."""
        id = self.__next_request_id
        self.__next_request_id += 1
        return id


class AgentMessageDeserializer(NetworkMessageDeserializer):
    """
    NetworkMessageDeserializer implementation for parsing network messages from Log Proxy to Agent.
    """

    def __init__(self, log_collector: LogCollector, content_requestor: ContentRequestHandler):
        self.__log_collector = log_collector
        self.__content_requestor = content_requestor

    def deserialize(self, message: RawNetworkMessage) -> NetworkMessage:
        match message.get_type():

            case NetworkMessageType.ADD_RECORD.value:
                pmessage = AddRecordNetworkMessage.from_raw(message)
                self.__log_collector.collect_record(pmessage.record)
            case NetworkMessageType.GET_LOG_POSITION.value:
                pmessage = GetLogPositionNetworkMessage.from_raw(message)
                position = self.__log_collector.get_log_position(pmessage.log)
                resp = LogPositionResponseNetworkMessage(pmessage.log, position)
                return resp

            case NetworkMessageType.LOG_CONTENT_STATUS.value:
                pmessage = LogContentStatusNetworkMessage.from_raw(message)
                match pmessage.status:
                    case LogContentStatusNetworkMessage.Status.FOUND_AND_BEGIN_SEND:
                        self.__content_requestor.set_status(pmessage.request_id, ContentRequest.Status.RECEIVING)
                    case LogContentStatusNetworkMessage.Status.NOT_FOUND:
                        self.__content_requestor.set_status(pmessage.request_id, ContentRequest.Status.NOT_FOUND)
                    case LogContentStatusNetworkMessage.Status.END_SEND:
                        self.__content_requestor.set_status(pmessage.request_id, ContentRequest.Status.CLOSED)
            case NetworkMessageType.LOG_CONTENT_DATA.value:
                pmessage = LogContentDataNetworkMessage.from_raw(message)
                self.__content_requestor.add_records(pmessage.request_id, pmessage.begin_record, pmessage.contents)

            case _:
                raise ValueError('incorrect message type')


class ProxyConnection:
    """
    Wrapper for a connection to the log proxy.
    """

    def __init__(self, collector: LogCollector, connection: socket.socket, address: str, port: int):
        self.__content_requestor = ContentRequestHandler()
        self.__deserializer = AgentMessageDeserializer(collector, self.__content_requestor)
        self.__connection = NetworkConnection(self.__deserializer, connection, address, port)

    def get_address(self) -> str:
        """Returns IP address of the connected log proxy."""
        return self.__connection.get_address()

    def get_port(self) -> int:
        """Returns port of the connected log proxy."""
        return self.__connection.get_port()

    def receive_messages(self) -> None:
        """Processes pending incoming messages on this proxy connection."""
        while self.__connection.receive():
            pass

    def request_content(self, log: Log, begin_record: int, end_record: int) -> ContentRequest:
        """
        Creates and sends a request for record content range in specified log.
        This method is thread-safe.
        """
        req = self.__content_requestor.create_request(log, begin_record, end_record)
        reqmsg = GetLogContentNetworkMessage(log, req.get_id(), begin_record, end_record)
        self.__connection.send(reqmsg)
        return req

    def drop_content_request(self, request: ContentRequest) -> None:
        """
        Drops specified content request.
        This method is thread-safe.
        """
        self.__content_requestor.set_status(request.get_id(), ContentRequest.Status.DROPPED)
