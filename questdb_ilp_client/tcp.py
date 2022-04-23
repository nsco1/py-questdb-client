import socket as skt
import sys
from datetime import datetime, timezone

FORBIDDEN_NAME_CHARS = set(ord(c) for c in "()*/%+-:.,?~\0\\")
GEOHASH_CHARS = set(ord(c) for c in "0123456789bcdefghjkmnpqrstuvwxyz")


class LineTcpSender:
    def __init__(self, host_name: str, port: int, buffer_size: int = 4096):
        self._send_buffer = bytearray(buffer_size)
        self._position = 0
        self._has_metric = False
        self._quoted = False
        self._named = False
        self._geohash = False
        self._no_fields = True
        self._client_socket = skt.socket(skt.AF_INET, skt.SOCK_STREAM)
        self._client_socket.setsockopt(skt.IPPROTO_TCP, skt.TCP_NODELAY, 1)
        self._client_socket.setblocking(True)
        self._client_socket.connect((host_name, port))

    def table(self, name: str):
        if self._has_metric:
            raise ValueError("Duplicate metric")
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
        raise ValueError("Metric expected")

    def column_int(self, name: str, value: int):
        self._column(name)._put_int(value)._put_str("i")
        return self

    def column_long(self, name: str, value: str):
        if value[:2] != "0x":
            raise ValueError("Invalid hex format")
        self._column(name)._put_long(value)._put_str("i")
        return self

    def column_float(self, name: str, value: float):
        self._column(name)._put_str(str(value))
        return self

    def column_str(self, name: str, value: str):
        self._column(name)._put_str('"')
        self._quoted = True
        self._encode_utf8(value)
        self._quoted = False
        self._put_str('"')
        return self

    def column_geohash(self, name: str, value: str):
        self._geohash = True
        self.column_str(name, str(value))
        self._geohash = False
        return self

    def column_bool(self, name: str, value: bool):
        self._column(name)._put_str(str(value))
        return self

    def at_utc_datetime(self, date_time: datetime):
        time_in_s = date_time.replace(tzinfo=timezone.utc).timestamp()
        time_in_ns = int(int(time_in_s * 10**6) * 10**3)
        self.at_timestamp(time_in_ns)

    def at_timestamp(self, time_in_ns: int):
        if isinstance(time_in_ns, datetime):
            raise TypeError("Must be int, not datetime")
        self._put_str(" ")._put_int(time_in_ns).at_now()

    def at_now(self):
        self._put_str("\n")
        self._has_metric = False
        self._no_fields = True
        return self

    def flush(self):
        sent = self._client_socket.send(self._send_buffer[: self._position])
        # print(self._send_buffer[: self._position])
        self._position -= sent

    def reset(self):
        self._position = 0
        self._has_metric = False
        self._quoted = False
        self._named = False
        self._geohash = False
        self._no_fields = True

    def _put_str(self, data: str):
        encoded = data.encode("utf-8")
        length = len(encoded)
        end_position = self._position + length
        if end_position >= len(self._send_buffer):
            self.flush()
        self._send_buffer[self._position :] = encoded
        self._position += length
        return self

    def _put_int(self, data: int):
        if int(data) < -sys.maxsize - 1 or int(data) > sys.maxsize:
            raise ValueError("Integer too large or small")
        return self._put_str(str(data))

    def _put_long(self, data: str):
        if int(data, 16) > int(
            "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16
        ):
            raise ValueError("Integer too large")
        return self._put_str(str(data))

    def _put_special(self, char: str):
        if (char == " " or char == "," or char == "=") and not self._quoted:
            self._put_str("\\")
        elif char == "\n" or char == "\r" or char == "\\" or char == '"':
            self._put_str("\\")
        self._put_str(char)

    def _encode_utf8(self, name: str):
        for i in range(len(name)):
            char = ord(name[i])
            if self._named:
                if char in FORBIDDEN_NAME_CHARS:
                    raise ValueError("Invalid char in name")
            else:
                if self._geohash and char not in GEOHASH_CHARS:
                    raise ValueError("Invalid char in geohash")
            if char < 128:
                self._put_special(name[i])
            else:
                self._put_str(name[i])
        self._named = False
        return self

    def _column(self, name: str):
        if self._has_metric:
            self._named = True
            if self._no_fields:
                self._put_str(" ")
                self._no_fields = False
            else:
                self._put_str(",")
            return self._encode_utf8(name)._put_str("=")

        raise ValueError("Metric expected")

    def __del__(self):
        self._delete()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._delete()

    def _delete(self):
        try:
            if self._position > 0:
                self.flush()
        except ValueError as Ex:
            print(f"Error disposing LineTcpSender: {Ex}")
        finally:
            self._client_socket.close()
