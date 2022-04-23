from typing import Optional

from questdb_ilp_client.tcp import LineTcpSender


def test_invalid_table_name_chars():
    ls = LineTcpSender("localhost", 9009, 1024)
    expect_fail_invalid_char(ls, "table", ".")
    expect_fail_invalid_char(ls, "table", "?")
    expect_fail_invalid_char(ls, "table", ",")
    expect_fail_invalid_char(ls, "table", ":")
    expect_fail_invalid_char(ls, "table", "\\")
    expect_fail_invalid_char(ls, "table", "/")
    expect_fail_invalid_char(ls, "table", ")")
    expect_fail_invalid_char(ls, "table", "\0")
    expect_fail_invalid_char(ls, "table", "(")
    expect_fail_invalid_char(ls, "table", "+")
    expect_fail_invalid_char(ls, "table", "*")
    expect_fail_invalid_char(ls, "table", "~")
    expect_fail_invalid_char(ls, "table", "%")
    expect_fail_invalid_char(ls, "table", "-")


def test_invalid_symbol_name_chars():
    ls = LineTcpSender("localhost", 9009, 1024)
    expect_fail_invalid_char(ls, "symbol", ".", "test")
    expect_fail_invalid_char(ls, "symbol", "?", "test")
    expect_fail_invalid_char(ls, "symbol", ",", "test")
    expect_fail_invalid_char(ls, "symbol", ":", "test")
    expect_fail_invalid_char(ls, "symbol", "\\", "test")
    expect_fail_invalid_char(ls, "symbol", "/", "test")
    expect_fail_invalid_char(ls, "symbol", ")", "test")
    expect_fail_invalid_char(ls, "symbol", "\0", "test")
    expect_fail_invalid_char(ls, "symbol", "(", "test")
    expect_fail_invalid_char(ls, "symbol", "+", "test")
    expect_fail_invalid_char(ls, "symbol", "*", "test")
    expect_fail_invalid_char(ls, "symbol", "~", "test")
    expect_fail_invalid_char(ls, "symbol", "%", "test")
    expect_fail_invalid_char(ls, "symbol", "-", "test")


def test_invalid_column_name_chars():
    ls = LineTcpSender("localhost", 9009, 1024)
    expect_fail_invalid_char(ls, "column_str", ".", "test")
    expect_fail_invalid_char(ls, "column_str", "?", "test")
    expect_fail_invalid_char(ls, "column_str", ",", "test")
    expect_fail_invalid_char(ls, "column_str", ":", "test")
    expect_fail_invalid_char(ls, "column_str", "\\", "test")
    expect_fail_invalid_char(ls, "column_str", "/", "test")
    expect_fail_invalid_char(ls, "column_str", ")", "test")
    expect_fail_invalid_char(ls, "column_str", "\0", "test")
    expect_fail_invalid_char(ls, "column_str", "(", "test")
    expect_fail_invalid_char(ls, "column_str", "+", "test")
    expect_fail_invalid_char(ls, "column_str", "*", "test")
    expect_fail_invalid_char(ls, "column_str", "~", "test")
    expect_fail_invalid_char(ls, "column_str", "%", "test")
    expect_fail_invalid_char(ls, "column_str", "-", "test")


def expect_fail_invalid_char(
    ls: LineTcpSender,
    method: str,
    arg: str,
    table_name: Optional[str] = None,
):
    try:
        ls.reset()
        if table_name:
            ls.table(table_name)
        if method == "table":
            ls.table(arg)
        elif method == "symbol":
            ls.symbol(arg, "")
        elif method == "column_str":
            ls.column_str(arg, "")
        raise AssertionError()
    except ValueError as err:
        if str(err) != "Invalid char in name":
            raise AssertionError()
