from __future__ import annotations

import dataclasses
from typing import Any
import socket


def get_scheduler_host() -> str:
    """get scheduler host to connect to"""
    hostname = socket.gethostname()
    if hostname == "jcmacbook.local":
        return "127.0.0.1:12100"
    elif hostname == "research":
        return "10.142.0.51:9998"
    else:
        raise ValueError("unable to determine scheduler host")


class Result:
    """runner result"""

    def __init__(self, result: Any, error_tb: str):
        self.result = result
        self.error_tb = error_tb  # error traceback string

    @classmethod
    def error(cls, err_tb: str):
        return cls(None, err_tb)

    @classmethod
    def success(cls, result: Any):
        return cls(result, None)


@dataclasses.dataclass(frozen=True)
class Resources:
    """task resources

    Default evenly allocates mem per core on nodes
    """

    cpus: str = "900m"
    mem: str = "700Mi"


@dataclasses.dataclass(frozen=True)
class JobSpec:
    """spec for the overall job"""

    name: str
    parallelism: int  # max parallelism
    image: str = "gcr.io/myproject/test"
    resources: Resources = Resources()  # resources to apply to each task
    scheduler: str = "http://127.0.0.1:8080/ws"
