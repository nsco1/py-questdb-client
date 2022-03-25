
# Python QuestDB ILP TCP client

## Requirements

This repository contains a Python 3.9 API for ingesting data into QuestDB through the InfluxDB Line Protocol. 

We use [make](https://www.gnu.org/software/make/) as a CLI to various convenient work developer flows.

### Install Flow

We require **Python 3.9.\*, or above** installed in your system, and `pip` needs to be up-to-date:

```shell
$ python3 --version
$ Python 3.9.<some-integer>
$ pip3 install --upgrade pip
```

Now we can install the project's dependencies in a virtual environment and activate it:

```shell
$ make install-dependencies
```

Or for development (Required for code quality and test flows):

```shell
$ make install-dependencies-dev
```

To activate the environment:

```shell
$ poetry shell
$ echo $SHLVL
2
```

To deactivate the environment:

```shell
$ exit
$ echo $SHLVL
1
```

### Code Quality Flow (Requires dev dependencies)

For convenience, we can let standard tools apply standard code formatting; the second command will report
issues that need to be addressed before using the client in production environments.

```shell
$ make format-code
$ make check-code-quality
```

### Test Flow (Requires dev dependencies)

To run all tests in the `tests` module:

```shell
$ make test
```

### Start/stop QuestDB Docker container Flow

To start QuestDB:

```shell
$ make compose-up
```

This creates a folder `questdb_root` to store QuestDB's table data/metadata, server configuration files,
and the web UI. 

**The Web UI is avaliable at**: [localhost:9000](http://localhost:9000).

Logs can be followed on the terminal:

```shell
$ docker logs -f questdb
```

To stop QuestDB:

```shell
$ make compose-down
```

Data is available, even when QuestDB is down, in folder `questdb_root`. 

## Basic usage

```py
with LineTcpSender(HOST, PORT, SIZE) as ls:
    ls.table("metric_name")
    ls.symbol("Symbol", "value")
    ls.column_int("number", 10)
    ls.column_float("double", 12.23)
    ls.column_str("string", "born to shine")
    ls.at_utc_datetime(datetime(2021, 11, 25, 0, 46, 26))
    ls.flush()
```

As an object

```py
ls = LineTcpSender(HOST, PORT, SIZE)
ls.table("metric_name")
ls.symbol("Symbol", "value")
ls.column_int("number", 10)
ls.column_float("double", 12.23)
ls.column_str("string", "born to shine")
ls.at_utc_datetime(datetime(2021, 11, 25, 0, 46, 26))
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

Object multi-line send

```py
ls = LineTcpSender(HOST, PORT, SIZE)
for i in range(int(1e6)):
    ls.table("metric_name")
    ls.column_int("counter", i)
    ls.at_now()
ls.flush()
```

## Notes

- On file `setup.py`: It is deprecated. To publish a package on PyPi you 
  can [follow this](https://www.brainsorting.com/posts/publish-a-package-on-pypi-using-poetry).
