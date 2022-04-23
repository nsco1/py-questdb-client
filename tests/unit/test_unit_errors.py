import sys
from datetime import datetime

import pytest

from questdb_ilp_client.tcp import LineTcpSender

ls = LineTcpSender("localhost", 9009, 1024)


@pytest.fixture(autouse=True)
def run_around_tests():
    ls.reset()
    yield


def test_duplicate_table():
    ls.table("table1")
    with pytest.raises(ValueError):
        ls.table("table2")


def test_symbol_metric_expected():
    with pytest.raises(ValueError):
        ls.symbol("name", "value")


def test_column_metric_expected():
    with pytest.raises(ValueError):
        ls._column("name")


def test_integer_max_size():
    ls.table("table")
    with pytest.raises(ValueError):
        ls.column_int("name", sys.maxsize + 1)


def test_integer_min_size():
    ls.table("table")
    with pytest.raises(ValueError):
        ls.column_int("name", -sys.maxsize - 2)


def test_long_invalid_hex():
    ls.table("table")
    with pytest.raises(ValueError):
        ls.column_long("value", "FF")


def test_long_max_size():
    ls.table("table")
    big_long = "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
    with pytest.raises(ValueError):
        ls.column_long("value", big_long)


def test_invalid_geohash():
    ls.table("location")
    with pytest.raises(ValueError):
        ls.column_geohash("gh", "9va")


def test_timestamp_wrong_value():
    ls.table("table")
    with pytest.raises(TypeError):
        ls.at_timestamp(datetime.now())
