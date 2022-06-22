from networking import *



class LogWatchManager(ABC):
    """Contract for implementation of manager able to launch watchers on specified position."""

    @abstractmethod
    def initialize_watch(self, log_name: str, start_record: int) -> None:
        """Launchs watch for specified log on specified record index."""
        pass

    @abstractmethod
    def contains_watch(self, log_name: str) -> bool:
        """Returns if this manager contains watch for specified log."""
        pass


class LogContentManager(ABC):
    """Contract for implementation of content request handler."""

    @abstractmethod
    def handle_log_content_request(self, log_name: str, request_id: int, begin_record: int, end_record: int) -> None:
        """Handles a request for log content."""
        pass



class LogProxyMessageDeserializer(NetworkMessageDeserializer):
    """
    NetworkMessageDeserializer implementation for parsing network messages from Agent to LogProxy.
    """

    def __init__(self):
        self.__watch_manager: LogWatchManager = None
        self.__content_manager: LogContentManager = None

    def set_content_manager(self, manager: LogContentManager) -> None:
        self.__content_manager = manager

    def set_watch_manager(self, manager: LogWatchManager) -> None:
        self.__watch_manager = manager

    def deserialize(self, message: RawNetworkMessage) -> NetworkMessage:
        match message.get_type():
            case NetworkMessageType.LOG_POSITION_RESPONSE.value:
                pmessage = LogPositionResponseNetworkMessage.from_raw(message)
                print(f'Initializing {pmessage.log.log_name} on record {pmessage.position}')
                self.__watch_manager.initialize_watch(pmessage.log.log_name, pmessage.position)
            case NetworkMessageType.GET_LOG_CONTENT.value:
                pmessage = GetLogContentNetworkMessage.from_raw(message)
                print(f'Got request for log={pmessage.log.log_name} records={pmessage.begin_record}:{pmessage.end_record} request_id={pmessage.request_id}')
                self.__content_manager.handle_log_content_request(pmessage.log.log_name, pmessage.request_id, pmessage.begin_record, pmessage.end_record)
            case _:
                raise ValueError('incorrect message type')
        return None

