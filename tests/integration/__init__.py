import socket as skt
import unittest
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

        self.ls = LineTcpSender.LineTcpSender(self.HOST, self.PORT, self.SIZE)

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
