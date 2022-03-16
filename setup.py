from distutils.core import setup

setup(
    name="questdb-ilp-client",
    version="0.0.1",
    description="A Python API for writing to QuestDB using the InfluxDB Line Protocol",
    author="Nathan Scott",
    author_email="nathan@nathanscott.co.uk",
    url="https://github.com/nsco1/py-questdb-client",
    packages=["questdb-ilp-client"],
    license="Apache License v2.0",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Time series"
    ]
)