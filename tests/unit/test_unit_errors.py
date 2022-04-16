import sys
import unittest
from datetime import datetime

from . import TestLineTcpSender


class Main(TestLineTcpSender, unittest.TestCase):
    def test_duplicate_table(self):
        self.ls.table("table1")
        self.assertRaises(Exception, self.ls.table, "table2")

    def test_symbol_metric_expected(self):
        self.assertRaises(Exception, self.ls.symbol, "name", "value")

    def test_column_metric_expected(self):
        self.assertRaises(Exception, self.ls._column, "name")

    def test_integer_max_size(self):
        self.ls.table("table")
        self.assertRaises(Exception, self.ls.column_int, "name", sys.maxsize + 1)

    def test_integer_min_size(self):
        self.ls.table("table")
        self.assertRaises(Exception, self.ls.column_int, "name", -sys.maxsize - 2)

    def test_long_invalid_hex(self):
        self.ls.table("table")
        self.assertRaises(Exception, self.ls.column_long, "value", "FF")

    def test_long_max_size(self):
        self.ls.table("table")
        big_long = "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
        self.assertRaises(Exception, self.ls.column_long, "value", big_long)

    def test_invalid_geohash(self):
        self.ls.table("location")
        self.assertRaises(Exception, self.ls.column_geohash, "gh", "9va")

    def test_timestamp_wrong_value(self):
        self.ls.table("test_timestamp_wrong_value")
        self.assertRaises(TypeError, self.ls.at_timestamp, datetime.now())


if __name__ == "__main__":
    unittest.main()
