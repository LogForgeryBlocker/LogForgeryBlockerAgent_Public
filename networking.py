import socket
from abc import ABC, abstractmethod
from enum import Enum
from threading import Lock
from typing import Iterable
from google.protobuf.timestamp_pb2 import Timestamp

from agent.log import Log, Record
from lsocket import BufferingSocket
from protobuf import agentcom_pb2


class NetworkMessage(ABC):
    """
    Base class for network messages.
    """

    @abstractmethod
    def get_type(self) -> int:
        pass

    @abstractmethod
    def get_contents(self) -> bytearray:
        pass

    def serialize(self) -> bytearray:
        """Serializes this message to buffer."""
        type = self.get_type()
        contents = self.get_contents()
        length = len(contents)
        if type < 0 or type > 255:
            raise ValueError('type is out of range')

        buffer = bytearray(5 + len(contents))
        buffer[0] = int.to_bytes(type, 1, byteorder='big', signed=False)[0]
        buffer[1:5] = int.to_bytes(length, 4, byteorder='big', signed=False)
        buffer[5:] = contents
        return buffer


class NetworkMessageMeta:
    def __init__(self, message_header: bytearray):
        self.type = int.from_bytes(message_header[0:1], byteorder='big', signed=False)
        self.length = int.from_bytes(message_header[1:5], byteorder='big', signed=False)


class RawNetworkMessage(NetworkMessage):
    """
    NetworkMessage implementation being only container for storing raw message properties, before passing them for deserialization.
    """

    def __init__(self, meta: NetworkMessageMeta, contents: bytearray):
        if len(contents) < meta.length:
            raise ValueError('invalid contents length')
        self.__meta = meta
        self.__contents = contents

    def get_type(self) -> int:
        return self.__meta.type

    def get_contents(self) -> bytearray:
        return self.__contents[:self.__meta.length]


class NetworkMessageDeserializer(ABC):
    """
    Contract for injecting network message deserializing method.
    """

    @abstractmethod
    def deserialize(self, message: RawNetworkMessage) -> NetworkMessage:
        """Processes received message."""
        pass


class SendingConnection(ABC):
    @abstractmethod
    def send(self, message: NetworkMessage) -> None:
        """
        Serializes message to bytearray and sends it (or queues it to be sent) to the socket.
        This method is thread safe.
        """
        pass


class NetworkConnection(SendingConnection):
    """
    Socket wrapper for sending and receiving network messages.
    """

    def __init__(self, deserializer: NetworkMessageDeserializer, connection: socket.socket, address: str, port: int):
        """
        Parameters:
            deserializer    : NetworkMessageSerializer - message deserialization logic
            connection      : BufferingSocket - underlaying socket for sending/receiving data
            addr            - address of client
        """
        self.__deserializer = deserializer
        self.__socket = BufferingSocket(connection, 4096)
        self.__current_message: RawNetworkMessage = None
        self.__address = address
        self.__port = port
        self.__send_mutex = Lock()

    def get_address(self) -> str:
        return self.__address

    def get_port(self) -> int:
        return self.__port

    def receive(self) -> bool:
        """
        Attempts to read next message from socket.

        When message read is completed, it is passed to NetworkConnection.deserializer for further processing.

        Returns True if current message read has ended during this call (there may be another message on socket which is ready to be read), False otherwise. 
        """
        if self.__current_message is None:
            self.__socket.begin_read(5)
            if not self.__socket.read(False):
                return False
            buff = self.__socket.get_buffer()
            meta = NetworkMessageMeta(buff)
            self.__socket.begin_read(meta.length)
            self.__current_message = RawNetworkMessage(meta, self.__socket.get_buffer())

        if self.__socket.read():
            resp = self.__deserializer.deserialize(self.__current_message)
            if resp is not None:
                self.send(resp)
            self.__current_message = None
            return True
        return False

    def send(self, message: NetworkMessage) -> None:
        buffer = message.serialize()
        try:
            self.__send_mutex.acquire()
            self.__socket.write(buffer)
        finally:
            self.__send_mutex.release()


class NetworkMessageType(Enum):
    ADD_RECORD = 1
    GET_LOG_POSITION = 2
    LOG_POSITION_RESPONSE = 3
    GET_LOG_CONTENT = 4
    LOG_CONTENT_STATUS = 5
    LOG_CONTENT_DATA = 6


class DeserializationHelper:
    @staticmethod
    def parse_log(raw: agentcom_pb2.Log) -> Log:
        return Log(raw.name)

    @staticmethod
    def parse_record(raw: agentcom_pb2.Record) -> Record:
        return Record(DeserializationHelper.parse_log(raw.log), raw.message, raw.timestamp.ToDatetime())


class AddRecordNetworkMessage(NetworkMessage):
    def __init__(self, record : Record):
        self.record = record

    @staticmethod
    def from_raw(message: RawNetworkMessage) -> 'AddRecordNetworkMessage':
        if message.get_type() != NetworkMessageType.ADD_RECORD.value:
            raise ValueError('invalid message type')
        srecord = agentcom_pb2.Record()
        srecord.ParseFromString(message.get_contents())
        record = DeserializationHelper.parse_record(srecord)
        ret = AddRecordNetworkMessage(record)
        return ret

    def get_type(self) -> int:
        return NetworkMessageType.ADD_RECORD.value

    def get_contents(self) -> bytearray:
        log = agentcom_pb2.Log(name=self.record.log.log_name)
        timestamp = Timestamp()
        timestamp.FromDatetime(self.record.timestamp)

        srecord = agentcom_pb2.Record(log=log, timestamp=timestamp, message=self.record.data)
        contents = srecord.SerializeToString()
        return contents


class GetLogPositionNetworkMessage(NetworkMessage):
    def __init__(self, log: Log):
        self.log = log

    @staticmethod
    def from_raw(message: RawNetworkMessage) -> 'GetLogPositionNetworkMessage':
        if message.get_type() != NetworkMessageType.GET_LOG_POSITION.value:
            raise ValueError('invalid message type')
        slog = agentcom_pb2.Log()
        slog.ParseFromString(message.get_contents())
        log = DeserializationHelper.parse_log(slog)
        ret = GetLogPositionNetworkMessage(log)
        return ret

    def get_type(self) -> int:
        return NetworkMessageType.GET_LOG_POSITION.value

    def get_contents(self) -> bytearray:
        slog = agentcom_pb2.Log(name=self.log.log_name)
        contents = slog.SerializeToString()
        return contents


class LogPositionResponseNetworkMessage(NetworkMessage):
    def __init__(self, log: Log, position: int):
        self.log = log
        self.position = position

    @staticmethod
    def from_raw(message: RawNetworkMessage) -> 'LogPositionResponseNetworkMessage':
        if message.get_type() != NetworkMessageType.LOG_POSITION_RESPONSE.value:
            raise ValueError('invalid message type')
        slogPosition = agentcom_pb2.LogPosition()
        slogPosition.ParseFromString(message.get_contents())
        log = DeserializationHelper.parse_log(slogPosition.log)
        ret = LogPositionResponseNetworkMessage(log, slogPosition.position)
        return ret

    def get_type(self) -> int:
        return NetworkMessageType.LOG_POSITION_RESPONSE.value

    def get_contents(self) -> bytearray:
        slog = agentcom_pb2.Log(name=self.log.log_name)
        slogPosition = agentcom_pb2.LogPosition(log=slog, position=self.position)
        contents = slogPosition.SerializeToString()
        return contents


class GetLogContentNetworkMessage(NetworkMessage):
    def __init__(self, log: Log, request_id: int, begin_record: int, end_record: int):
        self.log = log
        self.request_id = request_id
        self.begin_record = begin_record
        self.end_record = end_record

    @staticmethod
    def from_raw(message: RawNetworkMessage) -> 'GetLogContentNetworkMessage':
        if message.get_type() != NetworkMessageType.GET_LOG_CONTENT.value:
            raise ValueError('invalid message type')
        srequest = agentcom_pb2.LogContentRequest()
        srequest.ParseFromString(message.get_contents())
        log = DeserializationHelper.parse_log(srequest.log)
        ret = GetLogContentNetworkMessage(log, srequest.request_id, srequest.begin_record, srequest.end_record)
        return ret

    def get_type(self) -> int:
        return NetworkMessageType.GET_LOG_CONTENT.value

    def get_contents(self) -> bytearray:
        slog = agentcom_pb2.Log(name=self.log.log_name)
        srequest = agentcom_pb2.LogContentRequest(log=slog, request_id=self.request_id, begin_record=self.begin_record, end_record=self.end_record)
        contents = srequest.SerializeToString()
        return contents


class LogContentStatusNetworkMessage(NetworkMessage):
    class Status(Enum):
        FOUND_AND_BEGIN_SEND = 0
        END_SEND = 1
        NOT_FOUND = -1

    def __init__(self, request_id: int, status: Status):
        self.request_id = request_id
        self.status = status

    @staticmethod
    def from_raw(message: RawNetworkMessage) -> 'LogContentStatusNetworkMessage':
        if message.get_type() != NetworkMessageType.LOG_CONTENT_STATUS.value:
            raise ValueError('invalid message type')
        sresponse = agentcom_pb2.LogContentResponse()
        sresponse.ParseFromString(message.get_contents())
        ret = LogContentStatusNetworkMessage(sresponse.request_id, LogContentStatusNetworkMessage.Status(sresponse.status))
        return ret

    def get_type(self) -> int:
        return NetworkMessageType.LOG_CONTENT_STATUS.value

    def get_contents(self) -> bytearray:
        sresponse = agentcom_pb2.LogContentResponse(request_id=self.request_id, status=self.status.value)
        contents = sresponse.SerializeToString()
        return contents


class LogContentDataNetworkMessage(NetworkMessage):
    def __init__(self, request_id: int, begin_record: int, end_record: int, contents: Iterable[str]):
        if begin_record > end_record:
            raise ValueError('begin_record cannot be greater than end_record')
        self.request_id = request_id
        self.begin_record = begin_record
        self.end_record = end_record
        self.contents = contents

    @staticmethod
    def from_raw(message: RawNetworkMessage) -> 'LogContentDataNetworkMessage':
        if message.get_type() != NetworkMessageType.LOG_CONTENT_DATA.value:
            raise ValueError('invalid message type')
        sdata = agentcom_pb2.LogContentData()
        sdata.ParseFromString(message.get_contents())
        if sdata.begin_record > sdata.end_record or sdata.end_record - sdata.begin_record + 1 != len(sdata.contents):
            raise ValueError('invalid begin/end record indices')
        ret = LogContentDataNetworkMessage(sdata.request_id, sdata.begin_record, sdata.end_record, sdata.contents)
        return ret

    def get_type(self) -> int:
        return NetworkMessageType.LOG_CONTENT_DATA.value

    def get_contents(self) -> bytearray:
        sdata = agentcom_pb2.LogContentData(request_id=self.request_id, begin_record=self.begin_record, end_record=self.end_record)
        sdata.contents.extend(self.contents)
        msg_contents = sdata.SerializeToString()
        return msg_contents
