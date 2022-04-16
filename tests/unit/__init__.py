import unittest

from questdb_ilp_client.tcp import LineTcpSender


class TestLineTcpSender(unittest.TestCase):
    HOST = ""  # Standard loopback interface address (localhost)
    PORT = 9009  # Port to listen on (non-privileged ports are > 1023)
    SIZE = 1024  # Number of bytes to send / receive at one time

    def setUp(self):
        self.ls = LineTcpSender(self.HOST, self.PORT, self.SIZE)

    def invalid_char_helper(self, func, *args):
        try:
            if len(args) == 1:
                func(args)
            else:
                func(args[0], args[1])
        except Exception as e:
            return str(e) == "Invalid char in name"
