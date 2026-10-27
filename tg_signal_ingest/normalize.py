"""Normalization of Telegram messages into storable records.

Works against duck-typed objects that mirror Telethon ``Message`` attributes so
the same code path can be exercised with deterministic fixtures in tests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass
class MediaMeta:
    media_type: str | None = None
    media_meta: dict[str, Any] | None = None

    @property
    def media_json(self) -> str | None:
        return json.dumps(self.media_meta) if self.media_meta else None


@dataclass
class StoredMessage:
    channel_id: int
    message_id: int
    datetime: str
    edit_datetime: str | None
    text: str | None
    caption: str | None
    media_type: str | None
    media_meta_json: str | None
    grouped_id: int | None
    reply_to_message_id: int | None
    fwd_from_name: str | None
    fwd_from_id: int | None
    fwd_post_message_id: int | None
    raw_json: str
    source: str  # 'backfill' | 'live' | 'recovery'
    sender_id: int | None = None
    sender_name: str | None = None
    post_author: str | None = None
    entities_json: str | None = None


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _extract_media(msg: Any) -> MediaMeta:
    media = getattr(msg, "media", None)
    if media is None:
        return MediaMeta()
    class_name = media.__class__.__name__  # MessageMediaPhoto, MessageMediaDocument, ...
    meta: dict[str, Any] = {"kind": class_name}
    photo = getattr(media, "photo", None)
    document = getattr(media, "document", None)
    webpage = getattr(media, "webpage", None)

    if class_name == "MessageMediaPhoto" and photo is not None:
        meta["photo"] = _describe_photo(photo)
    elif class_name == "MessageMediaDocument" and document is not None:
        meta["document"] = _describe_document(document)
    elif class_name == "MessageMediaWebPage" and webpage is not None:
        meta["webpage"] = _describe_webpage(webpage)
    return MediaMeta(media_type=class_name, media_meta=meta)


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    return getattr(obj, name, default)


def _describe_photo(photo: Any) -> dict[str, Any]:
    sizes = _attr(photo, "sizes", []) or []
    dims: dict[str, Any] = {}
    for size in sizes:
        w, h = _attr(size, "w"), _attr(size, "h")
        if w and h and (not dims or w * h > dims.get("width", 0) * dims.get("height", 1)):
            dims = {"width": w, "height": h}
    return {
        "id": _attr(photo, "id"),
        "width": dims.get("width"),
        "height": dims.get("height"),
        "requires_ocr": True,  # the future parser must treat photos as image content, not text
    }


def _describe_document(doc: Any) -> dict[str, Any]:
    mime = _attr(doc, "mime_type")
    size = _attr(doc, "size")
    name = None
    for attr in _attr(doc, "attributes", []) or []:
        if attr.__class__.__name__ == "DocumentAttributeFilename":
            name = _attr(attr, "file_name")
    return {
        "id": _attr(doc, "id"),
        "mime_type": mime,
        "size_bytes": size,
        "file_name": name,
    }


def _describe_webpage(webpage: Any) -> dict[str, Any]:
    return {"url": _attr(webpage, "url"), "title": _attr(webpage, "title")}


def _forward_fields(msg: Any) -> tuple[str | None, int | None, int | None]:
    fwd = _attr(msg, "forward", None)
    if fwd is None:
        return None, None, None
    fwd_hdr = getattr(fwd, "_forward", None) or fwd
    sender_id = None
    sender_name: str | None = None
    from_id = _attr(fwd_hdr, "from_id")
    if from_id is not None:
        sender_id = _attr(from_id, "user_id", None) or _attr(from_id, "_channel_id", None)
    if sender_id is None and _attr(fwd_hdr, "channel_id") is not None:
        sender_id = _attr(fwd_hdr, "channel_id")
    sender_name = _attr(fwd_hdr, "from_name")
    post_id = _attr(fwd_hdr, "channel_post")
    return sender_name, sender_id, post_id


def message_to_record(channel_id: int, msg: Any, source: str) -> StoredMessage:
    """Convert a Telethon (or equivalent) message into a storage record."""

    msg = getattr(msg, "_message", msg) if type(msg).__name__ == "EventMessage" else msg
    text = _attr(msg, "message")
    if not isinstance(text, str):
        text = None
    media_meta = _extract_media(msg)
    caption = text if media_meta.media_type is not None else None
    body = None if media_meta.media_type is not None else text
    group_id = _attr(msg, "grouped_id")
    reply = _attr(msg, "reply_to")
    reply_id = _attr(reply, "reply_to_msg_id") if reply is not None else None
    fwd_name, fwd_id, fwd_post = _forward_fields(msg)
    raw = _serialize_raw(msg)
    sender_id = _attr(msg, "sender_id")
    if sender_id is None:
        from_id = _attr(msg, "from_id")
        sender_id = (
            _attr(from_id, "user_id", None)
            or _attr(from_id, "_channel_id", None)
            or _attr(from_id, "channel_id", None)
        )
    sender = _attr(msg, "sender", None)
    sender_name = None
    if sender is not None:
        sender_name = (
            getattr(sender, "title", None)
            or " ".join(
                part for part in (getattr(sender, "first_name", None), getattr(sender, "last_name", None)) if part
            ).strip()
            or getattr(sender, "username", None)
        )
    post_author = _attr(msg, "post_author")
    entities = _attr(msg, "entities", None)
    entities_json = None
    if entities:
        try:
            entities_json = json.dumps(
                [e.to_dict() if hasattr(e, "to_dict") else {"type": type(e).__name__} for e in entities],
                ensure_ascii=False,
            )
        except Exception:
            entities_json = None

    return StoredMessage(
        channel_id=channel_id,
        message_id=int(_attr(msg, "id")),
        datetime=_iso(_attr(msg, "date")) or "1970-01-01T00:00:00Z",
        edit_datetime=_iso(_attr(msg, "edit_date")),
        text=body,
        caption=caption,
        media_type=media_meta.media_type,
        media_meta_json=media_meta.media_json,
        grouped_id=int(group_id) if group_id else None,
        reply_to_message_id=int(reply_id) if reply_id else None,
        fwd_from_name=fwd_name,
        fwd_from_id=int(fwd_id) if fwd_id else None,
        fwd_post_message_id=fwd_post,
        raw_json=raw,
        source=source,
        sender_id=int(sender_id) if sender_id is not None else None,
        sender_name=str(sender_name) if sender_name else None,
        post_author=str(post_author) if post_author else None,
        entities_json=entities_json,
    )


def _serialize_raw(msg: Any) -> str:
    """Original Telegram message reference, serialized safely offline."""

    try:
        return json.dumps(msg.to_dict(), default=lambda o: o.__class__.__name__)
    except Exception:
        try:
            return json.dumps({"unserializable": type(msg).__name__})
        except Exception:  # pragma: no cover
            return '{"unserializable": "unknown"}'
