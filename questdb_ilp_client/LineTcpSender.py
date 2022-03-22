import socket as skt
import sys
from datetime import datetime, timezone

FORBIDDEN_NAME_CHARS = [0, 37, 40, 41, 42, 43, 44, 45, 46, 47, 58, 63, 92, 126, 163]


class LineTcpSender:
    def __init__(self, address, port, buffer_size=4096):
        self._position = 0
        self._has_metric = False
        self._quoted = False
        self._named = False
        self._no_fields = True
        self._client_socket = skt.socket(skt.AF_INET, skt.SOCK_STREAM)
        self._client_socket.setsockopt(skt.IPPROTO_TCP, skt.TCP_NODELAY, 1)
        self._client_socket.setblocking(True)
        self._client_socket.connect((address, port))
        self._send_buffer = bytearray(buffer_size)

    def __del__(self):
        self._client_socket.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            if self._position > 0:
                self.flush()
        except Exception as Ex:
            print(f"Error disposing LineTcpSender: {Ex}")
        finally:
            self._client_socket.close()

    def flush(self):
        sent = self._client_socket.send(self._send_buffer[: self._position])
        self._position -= sent

    def _put_str(self, data: str):
        encoded = data.encode("utf-8")
        length = len(encoded)
        end_position = self._position + length
        if end_position >= len(self._send_buffer):
            self.flush()
        self._send_buffer[self._position : end_position] = encoded
        self._position += length
        return self

    def _put_int(self, data: int):
        if data == -sys.maxsize - 1:
            raise OverflowError
        return self._put_str(str(data))

    def _put_special(self, char: str):
        if char == " " or char == "," or char == "=":
            if not self._quoted:
                self._put_str("\\")
            self._put_str(char)
        elif char == "\n" or char == "\r":
            self._put_str("\\")._put_str(char)
        elif char == '"':
            if self._quoted:
                self._put_str("\\")
            self._put_str(char)
        elif char == "\\":
            self._put_str("\\")._put_str("\\")
        else:
            self._put_str(char)

    def _encode_utf8(self, name: str):
        for i in range(len(name)):
            char = ord(name[i])
            if self._named and char in FORBIDDEN_NAME_CHARS:
                raise Exception("Invalid char in name")
            if char < 128:
                self._put_special(name[i])
            else:
                self._put_str(name[i])
        self._named = False
        return self

    def table(self, name: str):
        if self._has_metric:
            raise Exception("Duplicate metric")

        self._quoted = False
        self._has_metric = True
        self._named = True
        self._encode_utf8(name)
        return self

    def symbol(self, tag: str, value: str):
        if self._has_metric and self._no_fields:
            self._named = True
            return (
                self._put_str(",")._encode_utf8(tag)._put_str("=")._encode_utf8(value)
            )
        raise Exception("Metric expected")

    def column(self, name: str):
        if self._has_metric:
            self._named = True
            if self._no_fields:
                self._put_str(" ")
                self._no_fields = False
            else:
                self._put_str(",")
            return self._encode_utf8(name)._put_str("=")

        raise Exception("Metric expected")

    def column_int(self, name: str, value: int):
        self.column(name)._put_int(value)._put_str("i")
        return self

    def column_float(self, name: str, value: float):
        self.column(name)._put_str(str(value))
        return self

    def column_str(self, name: str, value: str):
        self.column(name)._put_str('"')
        self._quoted = True
        self._encode_utf8(value)
        self._quoted = False
        self._put_str('"')
        return self

    def at_now(self):
        self._put_str("\n")
        self._has_metric = False
        self._no_fields = True

    def at_utc_datetime(self, date_time: datetime):
        time_in_s = date_time.replace(tzinfo=timezone.utc).timestamp()
        time_in_ns = int(int(time_in_s * 10**6) * 10**3)
        self.at_timestamp(time_in_ns)

    def at_timestamp(self, time_in_ns: int):
        if isinstance(time_in_ns, datetime):
            raise TypeError("Must be int, not datetime")
        self._put_str(" ")._put_int(time_in_ns).at_now()
