import itertools
from typing import Iterable

from networking import LogContentDataNetworkMessage, LogContentStatusNetworkMessage, SendingConnection
from proxy.logproxy import LogContentManager, LogWatchManager



BATCH_SIZE = 20
"""Maximum record count for one LogContentData message."""


class LogStream:
    def __init__(self, path: str, begin_record: int):
        self.__file = open(path, 'r')
        for i in range(begin_record):
            next(self.__file)
        
    def read_records(self, max_count: int) -> list[str]:
        return list[str](itertools.islice(self.__file, 0, max_count))


class FileLogContentManager(LogContentManager):
    def __init__(self, watch_manager: LogWatchManager, connection: SendingConnection):
        self.__watch_manager = watch_manager
        self.__connection = connection

    def handle_log_content_request(self, log_name: str, request_id: int, begin_record: int, end_record: int) -> None:
        if not self.__watch_manager.contains_watch(log_name):
            self.__send_response(request_id, LogContentStatusNetworkMessage.Status.NOT_FOUND)
            return

        self.__send_response(request_id, LogContentStatusNetworkMessage.Status.FOUND_AND_BEGIN_SEND)

        stream = LogStream(log_name, begin_record)
        record = begin_record
        while record <= end_record:
            to_send = min(end_record - record + 1, BATCH_SIZE)
            records = stream.read_records(to_send)
            rec_count = len(records)

            if rec_count == 0:
                break

            self.__send_data(request_id, record, record + rec_count - 1, records)
            record += rec_count

        self.__send_response(request_id, LogContentStatusNetworkMessage.Status.END_SEND)

    def __send_response(self, request_id: int, status: int) -> None:
        message = LogContentStatusNetworkMessage(request_id, status)
        self.__connection.send(message)

    def __send_data(self, request_id: int, begin_record: int, end_record: int, records: Iterable[str]) -> None:
        message = LogContentDataNetworkMessage(request_id, begin_record, end_record, [r.rstrip() for r in records])
        self.__connection.send(message)
