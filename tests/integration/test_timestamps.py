import unittest
from datetime import datetime, timedelta, timezone

from . import TestLineTcpSender


class Main(TestLineTcpSender, unittest.TestCase):
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

    def test_early_datetime(self):
        expected = repr("test 1000000000\n")

        self.ls.table("test")
        self.ls.at_utc_datetime(datetime(1970, 1, 1, 0, 0, 1))
        self.ls.flush()

        self.conn, self.addr = self.client_socket.accept()
        self.assertEqual(self.receive(), expected)

    def test_timestamp_wrong_value(self):
        self.ls.table("test")
        self.assertRaises(TypeError, self.ls.at_timestamp, datetime.now())


if __name__ == "__main__":
    unittest.main()
