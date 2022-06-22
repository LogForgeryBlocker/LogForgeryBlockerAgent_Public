import io
import os
from abc import ABC, abstractmethod
from datetime import datetime

from proxy.logproxy import LogWatchManager



class FileRecordCollector(ABC):
    """Base class for collecting log records."""

    @abstractmethod
    def request_initialize(self, filename: str) -> None:
        pass

    @abstractmethod
    def on_record_added(self, filename: str, timestamp: datetime, file_pos: int, contents: str) -> None:
        """Called when record was added to log."""
        pass

    @abstractmethod
    def on_empty_write(self, filename: str, timestamp: datetime, file_pos: int) -> None:
        """Called when empty write to log was detected."""
        pass

    @abstractmethod
    def on_file_disappeared(self, filename: str, timestamp: datetime, file_pos: int) -> None:
        """Called when watched file log has disappeared."""
        pass


class LogFileWatcher(ABC):
    """
    Base log file watcher class.
    """

    def setup(self, filename: str, collector: FileRecordCollector):
        """
        Sets up watcher for specified file.
        Parameters:
            filename    : str   - file to watch,
            collector   : FileRecordCollector - collector to use for events,
        """
        self.__filename = filename
        self.__collector = collector
        self.__file_stream = open(filename, 'r')
        self.__prev_line = str()
        self.__initialized = False
        self.__to_skip = 0
        self.__collector.request_initialize(filename)

    def is_initialized(self) -> bool:
        return self.__initialized

    def initialize(self, start_line : int):
        """Initializes this File Watch and starts watching changes."""
        self.__to_skip = start_line
        self.read_records()
        self.__initialized = True

    def close(self) -> None:
        """Performs required cleanup before object being disposed."""
        self.__file_stream.close()

    def file_disappeared(self) -> None:
        """Raises file disappeared event for underlaying file."""
        timestamp = datetime.now()
        file_pos = self.__file_stream.tell()
        self.__collector.on_file_disappeared(self.__filename, timestamp, file_pos)

    def read_records(self) -> None:
        """Reads pending records from underlaying file and raises record added events for them."""
        timestamp = datetime.now()
        file_pos = self.__file_stream.tell() - len(self.__prev_line)
        data = self.__file_stream.read()

        if len(data) == 0:
            return

        lines = data.splitlines(True)
        if len(self.__prev_line) > 0:
            lines[0] = self.__prev_line + lines[0]
            self.__prev_line = str()

        for line in lines:
            if not line.endswith(os.linesep):
                if len(self.__prev_line) > 0:
                    raise ValueError('__prev_line not empty')
                self.__prev_line = line
            else:
                if self.__to_skip > 0:
                    self.__to_skip -= 1
                else:
                    self.__collector.on_record_added(self.__filename, timestamp, file_pos, line.rstrip())
                file_pos += len(line)


class LogFileWatcherManager(LogWatchManager):
    """
    Base log file watcher manager class.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_pyte, exc_value, tb):
        self.dispose()

    @abstractmethod
    def dispose(self) -> None:
        """Performs required cleanup before object being disposed."""
        pass

    @abstractmethod
    def listen(self) -> None:
        """Begins listening for events for all watched files and directories."""
        pass

    @abstractmethod
    def begin_watch(self, path: str) -> None:
        """Adds watch for specified file/directory."""
        pass

    @abstractmethod
    def end_watch(self, path: str) -> None:
        """Removes watch from specified file/directory."""
        pass
