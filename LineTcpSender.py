import sys
import socket
from datetime import datetime, timezone


class LineTcpSender:
    _client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _position = 0
    _has_metric = False
    _quoted = False
    _no_fields = True

    def __init__(self, address, port, buffer_size=4096):
        self.address = address
        self.port = port
        self.buffer_size = buffer_size

        self._client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._client_socket.setblocking(True)
        self._client_socket.connect((address, port))

        self._send_buffer = bytearray(buffer_size)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            if (self._position > 0):
                self.flush()
        except Exception as Ex:
            print(f"Error disposing LineTcpSender: {Ex}")
        finally:
            self._client_socket.close()

    def flush(self):
        sent = self._client_socket.send(self._send_buffer[:self._position])
        self._position -= sent

    def _put(self, data):
        if isinstance(data, str):
            if (len(data) == 1):
                if (self._position + 1 >= len(self._send_buffer)):
                    self.flush()
                self._send_buffer[self._position:self._position] = data.encode('utf-8')
                self._position += 1
            else:
                for char in data:
                    self._put(char)
            return self

        elif isinstance(data, int):
            if (data == -sys.maxsize - 1):
                raise OverflowError

            value = str(data).encode("utf-8")
            length = len(value)

            end_position = self._position + length
            if (end_position >= len(self._send_buffer)):
                self.flush()

            self._send_buffer[self._position:end_position] = value
            self._position += length
            return self

        else:
            raise TypeError("Unsupported type")

    def _put_special(self, char):
        if (char == ' ' or char == ',' or char == '='):
            if (not self._quoted):
                self._put('\\')
            self._put(char)
        elif (char == '\n' or char == '\r'):
            self._put('\\')._put(char)
        elif (char == '"'):
            if (self._quoted):
                self._put('\\')
            self._put(char)
        elif (char == '\\'):
            self._put('\\')._put('\\')
        else:
            self._put(char)

    def _put_utf8(self, char):
        if (self._position + 4 >= len(self._send_buffer)):
            self.flush()

        encoding = char.encode('utf8')
        length = len(encoding)

        self._send_buffer[self._position:self._position + length] = encoding
        self._position += length

    def _encode_utf8(self, name):
        for i in range(len(name)):
            char = ord(name[i])
            if (char < 128):
                self._put_special(name[i])
            else:
                self._put_utf8(name[i])
        return self

    def table(self, name):
        if (self._has_metric):
            raise Exception("Duplicate metric")

        self._quoted = False
        self._has_metric = True
        self._encode_utf8(name)
        return self

    def symbol(self, tag, value):
        if (self._has_metric and self._no_fields):
            self._put(',')._encode_utf8(tag)._put('=')._encode_utf8(value)
            return self

        raise Exception("Metric expected")

    def column(self, *args):
        name = args[0]
        if len(args) == 1:
            if (self._has_metric):
                if (self._no_fields):
                    self._put(' ') 
                    self._no_fields = False
                else:
                    self._put(',')
                return self._encode_utf8(name)._put('=')

            raise Exception("Metric expected")

        elif len(args) == 2:
            value = args[1]
            if isinstance(value, int):
                self.column(name)._put(value)._put('i')
                return self

            elif isinstance(value, float):
                self.column(name)._put(str(value))
                return self

            elif isinstance(value, str):
                self.column(name)._put('\"')
                self._quoted = True
                self._encode_utf8(value)
                self._quoted = False
                self._put('\"')
                return self

            else:
                raise TypeError("Unsupported type")

    def at_now(self):
        self._put('\n')
        self._has_metric = False
        self._no_fields = True

    def at(self, time):
        if isinstance(time, datetime):
            time_in_s = time.replace(tzinfo=timezone.utc).timestamp()
            time_in_ns = int(time_in_s * 1e9)
            self.at(time_in_ns)

        elif isinstance(time, int):
            self._put(' ')._put(time).at_now()

        else:
            raise TypeError("Unsupported type")