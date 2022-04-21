import unittest

from . import TestLineTcpSender


class Main(TestLineTcpSender, unittest.TestCase):
    def test_initial(self):
        expected_columns = [
            {"name": "Symbol", "type": "SYMBOL"},
            {"name": "number", "type": "LONG"},
            {"name": "double", "type": "DOUBLE"},
            {"name": "string", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [
            ["value", 10, 12.23, "born to shine", "1970-01-01T00:00:00.000001Z"]
        ]
        expected = (expected_columns, expected_dataset)
        table_name = "test_initial"

        self.ls.table(table_name)
        self.ls.symbol("Symbol", "value")
        self.ls.column_int("number", 10)
        self.ls.column_float("double", 12.23)
        self.ls.column_str("string", "born to shine")
        self.ls.at_timestamp(1000)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_multiple_symbols(self):
        expected_columns = [
            {"name": "city", "type": "SYMBOL"},
            {"name": "make", "type": "SYMBOL"},
            {"name": "temperature", "type": "DOUBLE"},
            {"name": "humidity", "type": "DOUBLE"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [
            ["London", "Omron", 23.5, 0.343, "2016-06-13T17:43:50.100400Z"]
        ]
        expected = (expected_columns, expected_dataset)
        table_name = "test_multiple_symbols"

        self.ls.table(table_name)
        self.ls.symbol("city", "London")
        self.ls.symbol("make", "Omron")
        self.ls.column_float("temperature", 23.5)
        self.ls.column_float("humidity", 0.343)
        self.ls.at_timestamp(1465839830100400000)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_no_symbol(self):
        expected_columns = [
            {"name": "column", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["test", "2022-04-07T15:39:48.774345Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_no_symbol"

        self.ls.table(table_name)
        self.ls.column_str("column", "test")
        self.ls.at_timestamp(1649345988774345984)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_spaces(self):
        expected_columns = [
            {"name": "space symbol", "type": "SYMBOL"},
            {"name": "space column", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["test", "test", "2022-04-07T15:39:48.774345Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test spaces"

        self.ls.table(table_name)
        self.ls.symbol("space symbol", "test")
        self.ls.column_str("space column", "test")
        self.ls.at_timestamp(1649345988774345984)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_quoted(self):
        expected_columns = [
            {"name": "symbol", "type": "SYMBOL"},
            {"name": "column", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [['"quoted"', '"quoted"', "2022-04-07T15:39:48.774345Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_quoted"

        self.ls.table(table_name)
        self.ls.symbol("symbol", '"quoted"')
        self.ls.column_str("column", '"quoted"')
        self.ls.at_timestamp(1649345988774345984)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_many_lines(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "2022-04-07T16:17:28.000000Z"]] * 1000
        expected = (expected_columns, expected_dataset)
        table_name = "test_many_lines"

        for _ in range(1000):
            self.ls.table(table_name)
            self.ls.symbol("loc", "north")
            self.ls.column_int("val", 200)
            self.ls.at_timestamp(1649348248000000000)
            self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_table_longer_than_buffer_size(self):
        del self.ls
        original_size = self.SIZE
        self.SIZE = 5
        self.setUp()

        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "2012-08-26T22:48:21.000000Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_buffer"

        self.ls.table(table_name)
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_timestamp(1346021301000000000)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)
        self.SIZE = original_size

    def test_foreign_chars(self):
        table_name = "test_foreign_chars"
        expected_columns = [
            {"name": "Russian", "type": "SYMBOL"},
            {"name": "Здравствуйте", "type": "SYMBOL"},
            {"name": "Japanese", "type": "STRING"},
            {"name": "こんにちは。", "type": "STRING"},
            {"name": "Chinese", "type": "STRING"},
            {"name": "你好", "type": "STRING"},
            {"name": "Arabic", "type": "STRING"},
            {"name": "أهلا", "type": "STRING"},
            {"name": "Emoji", "type": "STRING"},
            {"name": "👋🏻", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [
            [
                "Здравствуйте",
                "Reverse",
                "こんにちは。",
                "Reverse",
                "你好",
                "Reverse",
                "أهلا",
                "Reverse",
                "👋🏻",
                "Reverse",
                "2012-08-26T22:48:21.000000Z",
            ]
        ]
        expected = (expected_columns, expected_dataset)

        self.ls.table(table_name)
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
        self.ls.at_timestamp(1346021301000000000)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_ls_as_object(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "2022-04-07T15:11:20.159604Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_ls_as_object"

        with self.ls as line_sender:
            line_sender.table(table_name)
            line_sender.symbol("loc", "north")
            line_sender.column_int("val", 200)
            line_sender.at_timestamp(1649344280159604000)
            line_sender.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_long(self):
        expected_columns = [
            {"name": "value", "type": "LONG256"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["0x0123a4", "2021-11-29T16:20:21.000000Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_long"

        self.ls.table(table_name)
        self.ls.column_long("value", "0x123a4")
        self.ls.at_timestamp(1638202821000000000)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_geohash(self):
        expected_columns = [
            {"name": "gh", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["9v1s8hm7wpkssv1h", "2021-11-29T16:20:21.000000Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_geohash"

        self.ls.table(table_name)
        self.ls.column_geohash("gh", "9v1s8hm7wpkssv1h")
        self.ls.at_timestamp(1638202821000000000)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)

    def test_bool(self):
        expected_columns = [
            {"name": "val", "type": "BOOLEAN"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [[True, "2022-04-07T15:39:48.774345Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_bool"

        self.ls.table(table_name)
        self.ls.column_bool("val", True)
        self.ls.at_timestamp(1649345988774345984)
        self.ls.flush()

        self.assertEqual(self.receive(table_name), expected)


if __name__ == "__main__":
    unittest.main()
