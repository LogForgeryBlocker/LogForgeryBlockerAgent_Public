import datetime
import hashlib
from typing import Dict
from dotenv import load_dotenv
from threading import RLock

load_dotenv('.env')


class Log:
    """
    Log instance.

    Attributes:
        log_name    : str       - name of this log
        log_id      : str       - ID returned by the backend
    """

    def __init__(self, log_name: str, log_id: str = None):
        self.log_name = log_name
        self.log_id = log_id

    def __str__(self) -> str:
        return f'<Log log_name=\'{self.log_name}\'>'

    def add_log_id(self, log_id: str):
        self.log_id = log_id

    def to_dict(self):
        return {"name": self.log_name}


class Record:
    """
    Represents one Log record.
    Attributes:
        log         : Log       - owner of this record
        data        : str       - text value of this record
        timestamp   : datetime  - time of this record
    """

    def __init__(self, log: Log, data: str, timestamp: datetime):
        self.log = log
        self.data = data
        self.timestamp = timestamp

    def __str__(self):
        return f'<LogRecord log={self.log} data=\'{self.data}\' timestamp={self.timestamp}>'


class Snapshot:
    """
    Subset of Log, containing records for specified period.
    To change the hashing function, it is sufficient to replace the hash_fun method.

    Attributes:
        log         : Log        - reference to Log object, which is owner of this Snapshot
        cum_hash    : Str        - cumulative hash for all the records.
        first_line   : Int        - the position of the first line in the snapshot
        line_count   : Int        - the number of hashed lines
        mutex       : Luck       - mutex used to handle concurrent requests
    """

    def __init__(self, log: Log, first_line: int = 0):
        self.log = log
        self.cum_hash = Snapshot.hash_fun('')
        self.first_line = first_line
        self.line_count = 0
        self.mutex = RLock()

    def add_record(self, record: Record) -> None:
        """
        Adds specified record to this Snapshot.
        """
        with self.mutex:
            self.cum_hash = Snapshot.hash_fun(self.cum_hash + record.data)
            self.line_count = self.line_count + 1

    def get_next_line(self) -> int:
        """
        Returns last (exclusive) line for current snapshot.
        """
        with self.mutex:
            return self.first_line + self.line_count

    def upload_prep(self):
        with self.mutex:
            data = self.to_dict()
            self.reset()
            return data

    def reset(self):
        with self.mutex:
            self.first_line, self.line_count = self.get_next_line(), 0
            self.cum_hash = Snapshot.hash_fun('')

    def to_dict(self) -> Dict[str, str]:
        with self.mutex:
            return {"firstLine": self.first_line,
                    "lastLine": self.get_next_line() - 1,
                    "logId": self.log.log_id,
                    "fingerprint": self.get_hash()}

    def get_hash(self) -> str:
        return str(self)

    def __str__(self):
        return self.cum_hash

    @staticmethod
    def hash_fun(data: str) -> str:
        return str(hashlib.sha256(data.encode()).hexdigest())

    @staticmethod
    def parse_from_backend(backend_data: dict):
        log = Log(backend_data['name'], backend_data['id'])

        return Snapshot(log, backend_data['records'])
