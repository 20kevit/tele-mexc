"""Authenticated encryption for exchange secrets stored in SQLite.

The key is supplied by the operator through ``CONTROL_SECRET_KEY`` and never
lives in the database, source control, logs, or messages. This module uses
only the standard library: PBKDF2-HMAC-SHA256 for key derivation, an
HMAC-SHA256 counter stream for confidentiality, and encrypt-then-MAC for
integrity.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from typing import Any

ALG = "pbkdf2-hmac-sha256-ctr-hmac-sha256-v1"
ITERATIONS = 200_000


class SecretBoxError(Exception):
    """Encryption/decryption failed; the message is safe to display."""


def _material(secret_key: str) -> bytes:
    raw = secret_key.strip()
    if not raw:
        raise SecretBoxError("CONTROL_SECRET_KEY is not configured")
    try:
        if len(raw) == 64:
            key = bytes.fromhex(raw)
        else:
            key = raw.encode("utf-8")
    except ValueError:
        key = raw.encode("utf-8")
    if len(key) < 16:
        raise SecretBoxError("CONTROL_SECRET_KEY must contain at least 16 bytes of entropy")
    return key


def _keys(secret_key: str, salt: bytes) -> tuple[bytes, bytes]:
    derived = hashlib.pbkdf2_hmac("sha256", _material(secret_key), salt, ITERATIONS, dklen=64)
    return derived[:32], derived[32:]


def _keystream(key: bytes, nonce: bytes, length: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < length:
        block = hmac.new(key, nonce + counter.to_bytes(8, "big"), hashlib.sha256).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:length])


class SecretBox:
    def __init__(self, secret_key: str | None):
        self._secret_key = secret_key or ""

    @property
    def available(self) -> bool:
        try:
            _material(self._secret_key)
        except SecretBoxError:
            return False
        return True

    def encrypt(self, plaintext: str) -> dict[str, str]:
        if not plaintext:
            raise SecretBoxError("refusing to encrypt an empty secret")
        salt = os.urandom(16)
        nonce = os.urandom(16)
        enc_key, mac_key = _keys(self._secret_key, salt)
        data = plaintext.encode("utf-8")
        cipher = bytes(a ^ b for a, b in zip(data, _keystream(enc_key, nonce, len(data)), strict=True))
        mac = hmac.new(mac_key, ALG.encode() + salt + nonce + cipher, hashlib.sha256).digest()
        return {
            "alg": ALG,
            "salt": base64.b64encode(salt).decode(),
            "nonce": base64.b64encode(nonce).decode(),
            "ciphertext": base64.b64encode(cipher).decode(),
            "mac": base64.b64encode(mac).decode(),
        }

    def decrypt(self, record: dict[str, Any]) -> str:
        try:
            if record.get("alg") != ALG:
                raise SecretBoxError("unsupported secret format")
            salt = base64.b64decode(record["salt"])
            nonce = base64.b64decode(record["nonce"])
            cipher = base64.b64decode(record["ciphertext"])
            expected = base64.b64decode(record["mac"])
        except (KeyError, TypeError, ValueError) as exc:
            raise SecretBoxError("stored secret is malformed") from exc
        enc_key, mac_key = _keys(self._secret_key, salt)
        actual = hmac.new(mac_key, ALG.encode() + salt + nonce + cipher, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, actual):
            raise SecretBoxError("stored secret failed integrity verification")
        data = bytes(a ^ b for a, b in zip(cipher, _keystream(enc_key, nonce, len(cipher)), strict=True))
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SecretBoxError("stored secret is not valid UTF-8") from exc
