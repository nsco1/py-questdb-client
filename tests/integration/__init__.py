import json
import time
import unittest

import requests as req

from questdb_ilp_client.tcp import LineTcpSender

URL = "http://localhost:9000/exec"  # REST API endpoint


def select_all_from(table_name: str):
    received = False
    select_query = f"'{table_name}';"
    drop_query = f"drop table if exists '{table_name}';"
    while not received:
        r = req.get(url=URL, params={"query": select_query})
        data = json.loads(r.text)
        if "error" not in data and data["count"] > 0:
            req.get(url=URL, params={"query": drop_query})
            received = True
        time.sleep(0.1)
    return data["columns"], data["dataset"]


class TestLineTcpSender(unittest.TestCase):
    buffer_size = 1024

    def setUp(self):
        self.ls = LineTcpSender("localhost", 9009, self.buffer_size)
