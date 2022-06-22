import os, pyinotify
from typing import NoReturn
from abc import ABC, abstractmethod

from proxy.filesystem.logfilewatcher import FileRecordCollector, LogFileWatcher, LogFileWatcherManager



class EventBase(ABC):
    """
    Base class for pyinotify watches.
    """

    def my_init(self):
        """Explicit constructor."""
        self.__watch_descriptor: int = None

    @abstractmethod
    def close(self) -> None:
        """Performs required cleanup before object being disposed."""
        pass

    def get_watch_descriptor(self) -> int:
        """Returns pyinotify watch descriptor."""
        return self.__watch_descriptor

    def set_watch_descriptor(self, wd: int) -> None:
        """Sets pyinotify watch descriptor."""
        self.__watch_descriptor = wd


class FileEvent(pyinotify.ProcessEvent, EventBase, LogFileWatcher):
    """
    Pyinotify file handler.
    """

    def my_init(self, filename: str, collector: FileRecordCollector):
        self.setup(filename, collector)
        EventBase.my_init(self)

    def close(self) -> None:
        LogFileWatcher.close(self)

    def process_IN_MODIFY(self, event) -> None:
        """Event received on watched file modification."""
        if not self.is_initialized():
            return
        self.read_records()

    def process_IN_MOVE_SELF(self, event) -> None:
        """Event received on watched file moving out of directory."""
        if not self.is_initialized():
            return
        self.file_disappeared()
        
    def process_IN_DELETE_SELF(self, event) -> None:
        """Event received on watched file deletion."""
        if not self.is_initialized():
            return
        self.file_disappeared()


class DirEvent(pyinotify.ProcessEvent, EventBase):
    """
    Pyinotify directory handler.
    """
    def my_init(self, dir_name: str, manager: LogFileWatcherManager):
        self.__dir_name = dir_name
        self.__manager = manager
        EventBase.my_init(self)

    def close(self):
        pass

    def process_IN_CREATE(self, event) -> None:
        """Event received on file creation in watched directory."""
        self.__begin_watch(event.name)

    def process_IN_MOVED_TO(self, event) -> None:
        """Event received on file moved into watched directory."""
        self.__begin_watch(event.name)

    def process_IN_DELETE(self, event) -> None:
        """Event received on watched directory deletion."""
        self.__end_watch(event.name)

    def process_IN_MOVED_FROM(self, event) -> None:
        """Event received on file moved out of watched directory."""
        self.__end_watch(event.name)

    def __begin_watch(self, filename: str) -> None:
        """Adds specified filename to watched files list."""
        filepath = os.path.join(self.__dir_name, filename)
        self.__manager.begin_watch(filepath)

    def __end_watch(self, filename: str) -> None:
        """Removes specified filename from watched files list."""
        filepath = os.path.join(self.__dir_name, filename)
        self.__manager.end_watch(filepath)


class UnixFileWatcherManager(LogFileWatcherManager):
    """
    Class for managing file/directory watches.
    """

    def __init__(self, collector: FileRecordCollector):
        self.__collector = collector
        self.__events = dict[str, EventBase]()
        self.__manager = pyinotify.WatchManager()
        self.__notifier = pyinotify.Notifier(self.__manager)

    def dispose(self) -> None:
        for event in self.__events.values():
            event.close()
        self.__events.clear()

    def listen(self) -> NoReturn:
        self.__notifier.loop()

    def initialize_watch(self, path: str, start_record: int) -> None:
        if not path in self.__events:
            raise ValueError(f'"{path}" not watched')
        event = self.__events[path]
        if type(event) is not FileEvent:
            raise ValueError('event is not FileEvent')
        event: FileEvent
        event.initialize(start_record)

    def contains_watch(self, log_name: str) -> bool:
        return log_name in self.__events

    def begin_watch(self, path: str) -> None:
        if path in self.__events:
            raise ValueError(f'"{path}" is already watched')

        if os.path.isdir(path):
            event = DirEvent(dir_name=path, manager=self)
            for sub_path in os.listdir(path):
                base_path = os.path.basename(sub_path)
                npath = os.path.join(path, base_path)
                self.begin_watch(npath)
        else:
            event = FileEvent(filename=path, collector=self.__collector)
        wd = self.__manager.add_watch(path, pyinotify.ALL_EVENTS, proc_fun=event)
        event.set_watch_descriptor(wd[path])
        self.__events[path] = event

    def end_watch(self, path: str) -> None:
        if not path in self.__events:
            raise ValueError(f'"{path}" is not watched')

        event = self.__events[path]
        self.__manager.del_watch(event.get_watch_descriptor())
        event.close()
        del self.__events[path]
