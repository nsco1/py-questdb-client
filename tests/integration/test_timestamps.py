from datetime import datetime, timedelta, timezone

from . import TestLineTcpSender, select_all_from


class TimestampsTest(TestLineTcpSender):
    def test_sans_timestamp(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = ["north", 200]
        expected = (expected_columns, expected_dataset)
        table_name = "test_sans_timestamp"

        self.ls.table(table_name)
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_now()
        self.ls.flush()

        received_columns, received_dataset = select_all_from(table_name)
        received = (received_columns, received_dataset[0][:2])
        self.assertEqual(received, expected)

    def test_datetime_seconds(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "2022-03-14T00:46:57.000000Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_datetime_seconds"

        self.ls.table(table_name)
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_utc_datetime(datetime(2022, 3, 14, 0, 46, 57))
        self.ls.flush()

        self.assertEqual(select_all_from(table_name), expected)

    def test_datetime_microseconds(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "2022-03-14T00:46:57.000123Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_datetime_microseconds"

        self.ls.table(table_name)
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_utc_datetime(datetime(2022, 3, 14, 0, 46, 57, 123))
        self.ls.flush()

        self.assertEqual(select_all_from(table_name), expected)

    def test_datetime_timezone(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "2022-03-14T13:08:57.000000Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_datetime_timezone"
        tz = timezone(timedelta(hours=1))

        self.ls.table(table_name)
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_utc_datetime(datetime(2022, 3, 14, 13, 8, 57, tzinfo=tz))
        self.ls.flush()

        self.assertEqual(select_all_from(table_name), expected)

    def test_early_datetime(self):
        expected_columns = [
            {"name": "loc", "type": "SYMBOL"},
            {"name": "val", "type": "LONG"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ]
        expected_dataset = [["north", 200, "1970-01-01T00:00:01.000000Z"]]
        expected = (expected_columns, expected_dataset)
        table_name = "test_early_datetime"

        self.ls.table(table_name)
        self.ls.symbol("loc", "north")
        self.ls.column_int("val", 200)
        self.ls.at_utc_datetime(datetime(1970, 1, 1, 0, 0, 1))
        self.ls.flush()

        self.assertEqual(select_all_from(table_name), expected)
