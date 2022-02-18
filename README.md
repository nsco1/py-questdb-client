# py-questdb-client

### Python QuestDB ILP TCP client

- Basic usage

```py
with LineTcpSender(HOST, PORT, SIZE) as ls:
    ls.table("metric_name")
    ls.symbol("Symbol", "value")
    ls.column("number", 10)
    ls.column("double", 12.23)
    ls.column("string", "born to shine")
    ls.at(datetime(2021, 11, 25, 0, 46, 26))
    ls.flush()
```

- Multi-line send

```py
with LineTcpSender(HOST, PORT, SIZE) as ls:
    for i in range(int(1e6)):
        ls.table("metric_name")
        ls.column("counter", i)
        ls.at_now()
    ls.flush()
```

To test:
- Run echo.py first
- Then run test.py
