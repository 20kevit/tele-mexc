"""Regression coverage for the in-process test network guard."""

import socket

import pytest


@pytest.mark.parametrize("method", ["connect", "connect_ex", "sendto"])
def test_internet_sockets_are_blocked(method):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        args = (b"test", ("127.0.0.1", 9)) if method == "sendto" else (("127.0.0.1", 9),)
        with pytest.raises(AssertionError, match="Internet sockets are disabled"):
            getattr(sock, method)(*args)


def test_unix_socketpair_still_works():
    left, right = socket.socketpair()
    try:
        left.sendall(b"ok")
        assert right.recv(2) == b"ok"
    finally:
        left.close()
        right.close()
