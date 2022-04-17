import json
import time
import unittest

import requests as req

from questdb_ilp_client.tcp import LineTcpSender


class TestLineTcpSender(unittest.TestCase):
    HOST = ""  # Standard loopback interface address (localhost)
    PORT = 9009  # Port to listen on (non-privileged ports are > 1023)
    SIZE = 1024  # Number of bytes to send / receive at one time
    URL = "http://localhost:9000/exec"  # REST API endpoint

    def setUp(self):
        self.ls = LineTcpSender(self.HOST, self.PORT, self.SIZE)

    def receive(self, table_name):
        received = False
        while not received:
            time.sleep(0.1)
            r = req.get(self.URL + f"?query=select+%2A+from+%27{table_name}%27%3B")
            data = json.loads(r.text)

            if data["count"] > 0:
                req.get(self.URL + f"?query=drop+table+%27{table_name}%27%3B")
                received = True

        return (data["columns"], data["dataset"])
