from abc import ABC, abstractmethod

from agent.log import Log, Record, Snapshot
from agent.backend_connector import post_log, post_snapshot, get_logs


class LogCollector(ABC):
    """
    Contract for injecting log collection method.
    """

    @abstractmethod
    def collect_record(self, record: Record) -> None:
        pass

    @abstractmethod
    def upload_records(self) -> None:
        pass

    @abstractmethod
    def get_logs(self):
        pass

    @abstractmethod
    def log_size(self) -> int:
        pass

    @abstractmethod
    def get_log_position(self, log: Log) -> int:
        pass


class ListLogCollector(LogCollector):
    """
    Simple LogCollector implementation just adding all records into list.
    """

    def __init__(self):
        self.snapshots: dict[str, Snapshot] = get_logs()

    def collect_record(self, record: Record) -> None:
        snapshot = self.snapshots.get(record.log.log_name)

        if snapshot is None:
            snapshot = post_log(record.log.log_name)
            self.snapshots[record.log.log_name] = snapshot

        snapshot.add_record(record)

    def upload_records(self) -> None:
        for snapshot in self.snapshots.values():
            post_snapshot(snapshot)

    def get_logs(self):
        logs = []

        for snapshot in self.snapshots.values():
            logs.append(snapshot.log)

        return logs

    def log_size(self) -> int:
        ret = 0

        for snapshot in self.snapshots.values():
            ret = ret + snapshot.line_count

        return ret

    def get_log_position(self, log: Log) -> int:
        snapshot = self.snapshots.get(log.log_name)

        if snapshot is None:
            return 0

        return snapshot.get_next_line()
