# py-questdb-client

### Python QuestDB ILP TCP client

To test:
- Run echo.py first
- Then run test.py

Basic usage

```py
with LineTcpSender(HOST, PORT, SIZE) as ls:
    ls.table("metric_name")
    ls.symbol("Symbol", "value")
    ls.column_int("number", 10)
    ls.column_float("double", 12.23)
    ls.column_str("string", "born to shine")
    ls.at_datetime(datetime(2021, 11, 25, 0, 46, 26))
    ls.flush()
```

Multi-line send

```py
with LineTcpSender(HOST, PORT, SIZE) as ls:
    for i in range(int(1e6)):
        ls.table("metric_name")
        ls.column_int("counter", i)
        ls.at_now()
    ls.flush()
```
