import json
import time
import unittest

import requests as req

from questdb_ilp_client.tcp import LineTcpSender


class TestLineTcpSender(unittest.TestCase):
    HOST = ""  # Standard loopback interface address (localhost)
    PORT = 9009  # Port to listen on (non-privileged ports are > 1023)
    SIZE = 1024  # Number of bytes to send / receive at one time

    def setUp(self):
        self.ls = LineTcpSender(self.HOST, self.PORT, self.SIZE)

    def receive(self, table_name):
        time.sleep(0.75)
        params = {"query": f"select * from '{table_name}';"}
        r = req.get("http://localhost:9000/exec", params)
        data = json.loads(r.text)

        time.sleep(0.75)
        params = {"query": f"drop table '{table_name}';"}
        req.get("http://localhost:9000/exec", params)

        expected = (data["columns"], data["dataset"])
        return expected
