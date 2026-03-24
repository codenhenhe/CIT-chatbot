import os
from threading import Lock

from neo4j import GraphDatabase


_driver = None
_driver_lock = Lock()


def get_driver():
    global _driver
    if _driver is not None:
        return _driver

    with _driver_lock:
        if _driver is None:
            uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            username = os.getenv("NEO4J_USERNAME", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "password")
            _driver = GraphDatabase.driver(uri, auth=(username, password))
    return _driver


def close_driver():
    global _driver
    with _driver_lock:
        if _driver is not None:
            _driver.close()
            _driver = None