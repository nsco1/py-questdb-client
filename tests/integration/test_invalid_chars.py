import unittest

from . import TestLineTcpSender


class Main(TestLineTcpSender, unittest.TestCase):
    def invalid_char_helper(self, func, *args):
        try:
            if len(args) == 1:
                func(args)
            else:
                func(args[0], args[1])
        except Exception as e:
            return str(e) == "Invalid char in name"

    def test_invalid_table_name_char1(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "."))

    def test_invalid_table_name_char2(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "?"))

    def test_invalid_table_name_char3(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, ","))

    def test_invalid_table_name_char4(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, ":"))

    def test_invalid_table_name_char5(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "\\"))

    def test_invalid_table_name_char6(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "/"))

    def test_invalid_table_name_char7(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, ")"))

    def test_invalid_table_name_char8(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "\0"))

    def test_invalid_table_name_char9(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "("))

    def test_invalid_table_name_char10(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "+"))

    def test_invalid_table_name_char11(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "*"))

    def test_invalid_table_name_char12(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "~"))

    def test_invalid_table_name_char13(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "%"))

    def test_invalid_table_name_char14(self):
        self.assertTrue(self.invalid_char_helper(self.ls.table, "-"))

    def test_invalid_symbol_name_char1(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, ".", ""))

    def test_invalid_symbol_name_char2(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "?", ""))

    def test_invalid_symbol_name_char3(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, ",", ""))

    def test_invalid_symbol_name_char4(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, ":", ""))

    def test_invalid_symbol_name_char5(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "\\", ""))

    def test_invalid_symbol_name_char6(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "/", ""))

    def test_invalid_symbol_name_char7(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, ")", ""))

    def test_invalid_symbol_name_char8(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "\0", ""))

    def test_invalid_symbol_name_char9(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "(", ""))

    def test_invalid_symbol_name_char10(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "+", ""))

    def test_invalid_symbol_name_char11(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "*", ""))

    def test_invalid_symbol_name_char12(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "~", ""))

    def test_invalid_symbol_name_char13(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "%", ""))

    def test_invalid_symbol_name_char14(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls.symbol, "-", ""))

    def test_invalid_column_name_char1(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "."))

    def test_invalid_column_name_char2(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "?"))

    def test_invalid_column_name_char3(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, ","))

    def test_invalid_column_name_char4(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, ":"))

    def test_invalid_column_name_char5(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "\\"))

    def test_invalid_column_name_char6(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "/"))

    def test_invalid_column_name_char7(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, ")"))

    def test_invalid_column_name_char8(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "\0"))

    def test_invalid_column_name_char9(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "("))

    def test_invalid_column_name_char10(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "+"))

    def test_invalid_column_name_char11(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "*"))

    def test_invalid_column_name_char12(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "~"))

    def test_invalid_column_name_char13(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "%"))

    def test_invalid_column_name_char14(self):
        self.ls.table("test")
        self.assertTrue(self.invalid_char_helper(self.ls._column, "-"))


if __name__ == "__main__":
    unittest.main()
