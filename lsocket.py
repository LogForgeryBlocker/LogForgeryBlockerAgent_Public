import socket
from collections import deque



class BufferingSocket:
    """
    Socket wrapper allowing for simple reading/writing specified count of bytes

    Attributes:
        sock        : socket    - underlaying socket
        buffer_size : int       - read buffer size (attempting to read more bytes than buffer_size will result in allocating temporary buffer)
        buffer      : bytearray - read buffer
        temp_buffer : bytearray - temporary buffer (when reading more bytes than buffer_size)
        current_view : memoryview - view for active buffer
        position    : int       - current read position
        count_to_read : int     - total count of bytes to read in current attempt
        send_queue  : deque     - queue of bytearray to be sent
    """

    def __init__(self, sock : socket.socket, buffer_size : int):
        self.__sock = sock
        self.__buffer_size = buffer_size
        self.__buffer = bytearray(buffer_size)
        self.__temp_buffer = None
        self.__current_view = None
        self.__position = 0
        self.__count_to_read = 0
        self.__send_queue = deque()
        
        self.__sock.setblocking(0)

    def get_buffer(self) -> bytearray:
        """Returns active read buffer."""
        return self.__temp_buffer if self.__temp_buffer is not None else self.__buffer

    def begin_read(self, byte_count : int) -> None:
        """Begins read for specified count of bytes."""
        self.__temp_buffer = None
        self.__position = 0
        self.__count_to_read = byte_count
        if byte_count <= self.__buffer_size:
            self.__current_view = memoryview(self.__buffer)
        else:
            self.__temp_buffer = bytearray(byte_count)
            self.__current_view = memoryview(self.__temp_buffer)

    def read(self, no_block : bool = True) -> bool:
        """
        Tries to read next bytes from socket.

        Returns True if BufferingSocket.count_to_read bytes has been read in total (current read has finished), False otherwise.
        
        Use BufferingSocket.get_buffer() to get current read buffer.
        """
        while True:
            try:
                if self.__count_to_read == 0:
                    return True
                
                count = self.__sock.recv_into(self.__current_view[self.__position:], self.__count_to_read - self.__position)
                if count == 0:
                    raise ConnectionError()

                self.__position += count
                if self.__position == self.__count_to_read:
                    return True

            except socket.error as e:
                if any(e.args) and (e.args[0] == socket.EAGAIN or e.args[0] == socket.EWOULDBLOCK):
                    if no_block or self.__position == 0:
                        break
                else:
                    raise
        return False

    def write(self, content : bytearray) -> None:
        """
        Tries to write bytearray contents to socket.

        If contents can't be sent immediately, it is queued to retry to be sent on next BufferingSocket.write() call.
        """
        self.__send_queue.append(content)
        while any(self.__send_queue):
            to_send = self.__send_queue.popleft()
            sent = self.send(to_send)
            if sent < len(to_send):
                self.__send_queue.appendleft(to_send[sent:])
                break

    def send(self, content : bytearray) -> int:
        """Sends bytearray contents. Returns count of bytes sent."""
        try:
            return self.__sock.send(content)
        except socket.error as e:
            if any(e.args) and (e.args[0] == socket.EAGAIN or e.args[0] == socket.EWOULDBLOCK):
                return 0
            else:
                raise
