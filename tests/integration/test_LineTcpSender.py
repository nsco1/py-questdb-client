import socket as skt
import unittest
from datetime import datetime, timedelta, timezone
from select import select

from questdb_ilp_client import LineTcpSender


class TestLineTcpSender(unittest.TestCase):
    HOST = ""  # Standard loopback interface address (localhost)
    PORT = 9009  # Port to listen on (non-privileged ports are > 1023)
    SIZE = 1024  # Number of bytes to send / receive at one time

    def setUp(self):
        self.client_socket = skt.socket(skt.AF_INET, skt.SOCK_STREAM)
        self.client_socket.setsockopt(skt.SOL_SOCKET, skt.SO_REUSEADDR, 1)
        self.client_socket.bind((self.HOST, self.PORT))
        self.client_socket.listen()

        self.ls = LineTcpSender(self.HOST, self.PORT, self.SIZE)

    def tearDown(self):
        self.client_socket.close()

    def receive(self):
        data = b""
        while True:
            ready = select([self.conn], [], [], 1)
            if ready[0]:
                data += self.conn.recv(self.SIZE)
            else:
                self.conn.close()
                break
        return repr(data.decode())

    def test_initial(self):
        expected = repr(
            'metric_name,Symbol=value number=10i,double=12.23,string="born to shine" 1\n'
        )

        self.ls.table("metric_name")
        self.ls.symbol("Symbol", "value")
        self.ls.column_int("number", 10)
        self.ls.column_float("double", 12.23)
        self.ls.column_str("string", "born to shine")
        self.ls.at_timestamp(1)
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_multiple_symbols(self):
        expected = repr(
            "readings,city=London,make=Omron temperature=23.5,humidity=0.343 1465839830100400000\n"
        )

        self.ls.table("readings")
        self.ls.symbol("city", "London")
        self.ls.symbol("make", "Omron")
        self.ls.column_float("temperature", 23.5)
        self.ls.column_float("humidity", 0.343)
        self.ls.at_timestamp(1465839830100400000)
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_sans_timestamp(self):
        expected = repr("tracking,loc=north val=200i\n")

        self.ls.table("tracking")
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_now()
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_datetime_seconds(self):
        expected = repr("test 1647218817000000000\n")

        self.ls.table("test")
        self.ls.at_utc_datetime(datetime(2022, 3, 14, 0, 46, 57))
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_datetime_microseconds(self):
        expected = repr("test 1647218817000123000\n")

        self.ls.table("test")
        self.ls.at_utc_datetime(datetime(2022, 3, 14, 0, 46, 57, 123))
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_datetime_timezone(self):
        expected = repr("test 1647263337000000000\n")
        tz = timezone(timedelta(hours=1))

        self.ls.table("test")
        self.ls.at_utc_datetime(datetime(2022, 3, 14, 13, 8, 57, tzinfo=tz))
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_timestamp_nanoseconds(self):
        expected = repr("test 1647218817123456789\n")

        self.ls.table("test")
        self.ls.at_timestamp(1647218817123456789)
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_spaces(self):
        expected = repr('space\ table,space\ symbol=test space\ column="test"\n')

        self.ls.table("space table")
        self.ls.symbol("space symbol", "test")
        self.ls.column_str("space column", "test")
        self.ls.at_now()
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_many_lines(self):
        expected = repr("tracking,loc=north val=200i\n" * 1000)

        for _ in range(1000):
            self.ls.table("tracking")
            self.ls.symbol("loc", "north")
            self.ls.column_int("val", 200)
            self.ls.at_now()
            self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_table_longer_than_buffer_size(self):
        expected = repr("tracking,loc=north val=200i\n")
        original_size = self.SIZE
        self.SIZE = 5

        self.ls.table("tracking")
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_now()
        self.ls.flush()

        self.SIZE = original_size
        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_foreign_chars(self):
        string = "Greetings,"
        string += "Russian=Здравствуйте,"
        string += "Здравствуйте=Reverse "
        string += 'Japanese="こんにちは。",'
        string += 'こんにちは。="Reverse",'
        string += 'Chinese="你好",'
        string += '你好="Reverse",'
        string += 'Arabic="أهلا",'
        string += 'أهلا="Reverse",'
        string += 'Emoji="👋🏻",'
        string += '👋🏻="Reverse"\n'
        expected = repr(string)

        self.ls.table("Greetings")
        self.ls.symbol("Russian", "Здравствуйте")
        self.ls.symbol("Здравствуйте", "Reverse")
        self.ls.column_str("Japanese", "こんにちは。")
        self.ls.column_str("こんにちは。", "Reverse")
        self.ls.column_str("Chinese", "你好")
        self.ls.column_str("你好", "Reverse")
        self.ls.column_str("Arabic", "أهلا")
        self.ls.column_str("أهلا", "Reverse")
        self.ls.column_str("Emoji", "👋🏻")
        self.ls.column_str("👋🏻", "Reverse")
        self.ls.at_now()
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_timestamp_wrong_value(self):
        self.ls.table("test")
        self.assertRaises(TypeError, self.ls.at_timestamp, datetime.now())

    def test_duplicate_table(self):
        self.ls.table("table1")
        self.assertRaises(Exception, self.ls.table, "table2")

    def test_symbol_metric_expected(self):
        self.assertRaises(Exception, self.ls.symbol, "name", "value")

    def test_column_metric_expected(self):
        self.assertRaises(Exception, self.ls.column, "name")


if __name__ == "__main__":
    unittest.main()
