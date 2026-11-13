"""Minimal official Telegram Bot API client (stdlib only).

Requests go directly to ``api.telegram.org`` over HTTPS. ``http.client`` does
not consult ``HTTP_PROXY``/``HTTPS_PROXY``, so ambient proxy variables cannot
change this transport. The token is never included in exception text, logs, or
repr(). A transport callable is injectable so the control flow is testable
without network access.
"""

from __future__ import annotations

import http.client
import json
import re
import socket
import ssl
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

Transport = Callable[[str, dict[str, Any]], dict[str, Any]]


class _DirectHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection over a direct TCP socket (no proxy discovery)."""

    def __init__(self, host: str, port: int, timeout: float):
        context = ssl.create_default_context()
        super().__init__(host, port, timeout=timeout, context=context)


class BotApiError(Exception):
    """Bot API call failed; description is safe to show to the owner."""


class BotApiTransportError(BotApiError):
    """The request may or may not have reached Telegram (ambiguous)."""


class BotApiRetryableError(BotApiError):
    """A retryable failure that is only raised by the local transport layer."""


class BotApiNotSentError(BotApiRetryableError):
    """The request definitely did not leave this host (DNS/connect failure).

    Only failures in this class are safe to retry automatically.  Any failure
    after the request may have been transmitted is classified as
    :class:`BotApiTransportError` (ambiguous) and is never retried blindly.
    """


class BotApiRateLimitError(BotApiError):
    """Telegram asked us to wait; the request was not applied."""

    def __init__(self, message: str, *, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class BotApiForbiddenError(BotApiError):
    """Telegram definitively refused this chat (blocked/kicked/deactivated)."""


class BotApiClient:
    def __init__(
        self,
        token: str,
        *,
        api_base: str = "https://api.telegram.org",
        transport: Transport | None = None,
        timeout_s: float = 40.0,
    ):
        if not token or ":" not in token:
            raise BotApiError("Telegram bot token is missing or malformed")
        parsed = urlsplit(api_base)
        if parsed.scheme != "https" or not parsed.hostname:
            raise BotApiError("Telegram Bot API base URL must be an https:// URL")
        self._token = token
        self._api_base = api_base.rstrip("/")
        self._api_host = parsed.hostname
        self._api_port = parsed.port or 443
        self._api_path = parsed.path.rstrip("/")
        self._transport = transport
        self._timeout_s = timeout_s

    def __repr__(self) -> str:  # never expose the token
        return "BotApiClient(<redacted>)"

    @property
    def ready(self) -> bool:
        return True

    # ------------------------------------------------------------ transport

    def _http_transport(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST directly to the Telegram Bot API over verified TLS.

        DNS/connect failures happen before any request byte can be written and
        are therefore safe to retry.  Every later failure is ambiguous: the
        request may already have reached Telegram, and a Bot API write is not
        idempotent.  The connection is intentionally not reused (``Connection:
        close``): a pooled socket that dies between requests would make it hard
        to prove whether a retry follows a partially transmitted write.
        """

        path = f"{self._api_path}/bot{self._token}/{method}"
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        conn: _DirectHTTPSConnection | None = None
        try:
            conn = _DirectHTTPSConnection(
                self._api_host,
                self._api_port,
                timeout=self._timeout_s,
            )
            try:
                conn.connect()
            except (socket.gaierror, ConnectionRefusedError, ConnectionAbortedError) as exc:
                raise BotApiNotSentError(
                    f"Telegram Bot API unreachable before send ({exc.__class__.__name__}); safe to retry"
                ) from None
            except OSError as exc:
                # A connect-time OSError (for example ENETUNREACH) still means no
                # request body was written, so it is definitely not sent.
                raise BotApiNotSentError(
                    f"Telegram Bot API connection failed before send ({exc.__class__.__name__}); safe to retry"
                ) from None
            try:
                conn.request(
                    "POST",
                    path,
                    body=data,
                    headers={
                        "Content-Type": "application/json",
                        "Content-Length": str(len(data)),
                        "Host": self._api_host,
                        "Connection": "close",
                    },
                )
                response = conn.getresponse()
                body = response.read()
            except Exception as exc:  # noqa: BLE001 - request may have been transmitted
                raise BotApiTransportError(
                    f"Telegram Bot API request failed after possible transmission; outcome is ambiguous "
                    f"({exc.__class__.__name__})"
                ) from None
        finally:
            if conn is not None:
                conn.close()
        try:
            result = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise BotApiTransportError(
                "Telegram Bot API returned an unreadable response; outcome is ambiguous"
            ) from None
        if not isinstance(result, dict):
            raise BotApiTransportError("Telegram Bot API returned an unexpected response; outcome is ambiguous")
        return result

    def call(self, method: str, **params: Any) -> Any:
        transport = self._transport or self._http_transport
        payload = {k: v for k, v in params.items() if v is not None}
        try:
            response = transport(method, payload)
        except BotApiError:
            raise
        except Exception as exc:  # noqa: BLE001 - transport boundary, redact details
            raise BotApiTransportError(
                f"Telegram Bot API direct network failure; outcome is ambiguous ({exc.__class__.__name__})"
            ) from None
        if not isinstance(response, dict):
            raise BotApiTransportError("Telegram Bot API returned an unexpected response; outcome is ambiguous")
        if not response.get("ok"):
            description = str(response.get("description") or "")[:200]
            try:
                error_code = int(response.get("error_code") or 0)
            except (TypeError, ValueError):
                error_code = 0
            parameters = response.get("parameters")
            retry_after = None
            if isinstance(parameters, dict):
                try:
                    retry_after = int(parameters.get("retry_after") or 0) or None
                except (TypeError, ValueError):
                    retry_after = None
            if retry_after is None:
                m = re.search(r"retry after\s*(\d+)", description, re.IGNORECASE)
                if m:
                    retry_after = int(m.group(1))
            if retry_after is not None or error_code == 429:
                raise BotApiRateLimitError(
                    f"Telegram Bot API rate limited {method}: {description or 'rate limited'}",
                    retry_after=retry_after,
                )
            if "blocked" in description.lower() or "deactivated" in description.lower():
                raise BotApiForbiddenError(
                    f"Telegram Bot API rejected {method}: {description or 'chat blocked or deactivated'}"
                )
            if error_code >= 500:
                # A 5xx response proves the request reached the server but not
                # whether the write was applied. Retrying could duplicate a
                # trade alert, so this is ambiguous rather than retryable.
                raise BotApiTransportError(
                    f"Telegram Bot API server error {error_code}; outcome is ambiguous"
                )
            raise BotApiError(f"Telegram Bot API rejected {method}: {description or 'unknown error'}")
        return response.get("result")

    # --------------------------------------------------------------- methods

    def get_me(self) -> dict[str, Any]:
        return self.call("getMe") or {}

    def get_updates(
        self,
        *,
        offset: int | None = None,
        timeout: int = 25,
        allowed_updates: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        result = self.call(
            "getUpdates",
            offset=offset,
            timeout=timeout,
            allowed_updates=allowed_updates or ["message", "callback_query"],
        )
        return result if isinstance(result, list) else []

    def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
        disable_web_page_preview: bool = True,
        reply_to_message_id: int | None = None,
    ) -> dict[str, Any]:
        return self.call(
            "sendMessage",
            chat_id=chat_id,
            text=text[:4096],
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_to_message_id=reply_to_message_id,
        ) or {}

    def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        return self.call(
            "editMessageText",
            chat_id=chat_id,
            message_id=message_id,
            text=text[:4096],
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        ) or {}

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> bool:
        try:
            self.call("answerCallbackQuery", callback_query_id=callback_query_id, text=text)
        except BotApiError:
            return False
        return True

    def delete_message(self, chat_id: int, message_id: int) -> bool:
        try:
            self.call("deleteMessage", chat_id=chat_id, message_id=message_id)
        except BotApiError:
            return False
        return True

    def set_my_commands(self, commands: list[dict[str, str]]) -> bool:
        try:
            self.call("setMyCommands", commands=commands)
        except BotApiError:
            return False
        return True
