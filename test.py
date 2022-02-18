from datetime import datetime
from LineTcpSender import LineTcpSender

HOST = ''      # Standard loopback interface address (localhost)
PORT = 9009    # Port to listen on (non-privileged ports are > 1023)
SIZE = 1024    # Number of bytes to send / receive at one time

with LineTcpSender(HOST, PORT, SIZE) as ls:
    ls.table("metric_name")
    ls.symbol("Symbol", "value")
    ls.column("number", 10)
    ls.column("double", 12.23)
    ls.column("string", "born to shine")
    ls.at(datetime(2021, 11, 25, 0, 46, 26))
    ls.flush()