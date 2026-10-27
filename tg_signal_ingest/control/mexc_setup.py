"""Fail-closed MEXC credential verification for the setup conversation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..mexc.errors import (
    MexcAuthError,
    MexcNetworkError,
    MexcPermissionError,
    MexcRateLimitError,
    MexcServerError,
    MexcWafError,
)

WITHDRAW_FLAGS = {
    "ENABLEWITHDRAWALS",
    "WITHDRAWENABLED",
    "CANWITHDRAW",
    "ENABLEWITHDRAWAL",
    "WITHDRAWPERMISSION",
}


@dataclass(frozen=True)
class MexcCheck:
    ok: bool
    category: str  # ok | auth | permission | network | server | malformed
    detail: str


def _truthy(value: Any) -> bool:
    return value is True or str(value).strip().lower() in ("1", "true", "yes", "on")


def _flatten(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            name = f"{prefix}.{key}".upper().lstrip(".")
            out.append((name, item))
            out.extend(_flatten(item, name))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            out.extend(_flatten(item, f"{prefix}.{index}"))
    return out


def _has_withdrawal_permission(info: Any) -> bool:
    for name, value in _flatten(info):
        leaf = name.rsplit(".", 1)[-1]
        if leaf in WITHDRAW_FLAGS and _truthy(value):
            return True
        if isinstance(value, str) and value.strip().upper() == "WITHDRAW":
            return True
    return False


def verify_mexc_client(client: Any) -> MexcCheck:
    """Verify read/trade capability and reject withdrawal-enabled keys.

    The check uses only documented Spot v3 endpoints (``/account`` and
    ``/apiKeyInfo``). It never logs or returns the credentials.
    """

    try:
        account = client.account()
    except MexcAuthError:
        return MexcCheck(False, "auth", "کلید API یا امضای درخواست معتبر نیست.")
    except MexcPermissionError:
        return MexcCheck(False, "permission", "کلید اجازه خواندن حساب را ندارد.")
    except (MexcNetworkError, MexcWafError, MexcRateLimitError) as exc:
        return MexcCheck(False, "network", f"ارتباط با MEXC برقرار نشد ({exc.__class__.__name__}).")
    except MexcServerError:
        return MexcCheck(False, "server", "MEXC در حال حاضر خطای سرور برمی‌گرداند.")
    except Exception as exc:  # noqa: BLE001 - boundary; detail stays generic
        return MexcCheck(False, "malformed", f"پاسخ نامعتبر از MEXC ({exc.__class__.__name__}).")

    if not isinstance(account, dict):
        return MexcCheck(False, "malformed", "پاسخ حساب MEXC ساختار مورد انتظار را ندارد.")
    if account.get("canTrade") is not True:
        return MexcCheck(False, "permission", "حساب اجازه معامله اسپات را ندارد (canTrade=false).")
    permissions = account.get("permissions")
    if isinstance(permissions, list) and permissions:
        upper = {str(p).upper() for p in permissions}
        if "SPOT" not in upper:
            return MexcCheck(False, "permission", "کلید مجوز SPOT ندارد.")
    if _has_withdrawal_permission(account):
        return MexcCheck(False, "permission", "کلید مجوز برداشت دارد؛ آن را با کلید محدود جایگزین کنید.")

    try:
        info = client.api_key_info()
    except MexcAuthError:
        return MexcCheck(False, "auth", "کلید API یا امضای درخواست معتبر نیست.")
    except MexcPermissionError:
        return MexcCheck(False, "permission", "امکان بررسی مجوزهای کلید وجود ندارد.")
    except (MexcNetworkError, MexcWafError, MexcRateLimitError) as exc:
        return MexcCheck(False, "network", f"بررسی مجوز کلید ناموفق بود ({exc.__class__.__name__}).")
    except MexcServerError:
        return MexcCheck(False, "server", "MEXC هنگام بررسی مجوز کلید خطای سرور داد.")
    except Exception as exc:  # noqa: BLE001
        return MexcCheck(False, "malformed", f"پاسخ مجوز کلید نامعتبر است ({exc.__class__.__name__}).")

    if not isinstance(info, dict):
        return MexcCheck(False, "malformed", "پاسخ مجوز کلید ساختار مورد انتظار را ندارد.")
    if _has_withdrawal_permission(info):
        return MexcCheck(
            False, "permission",
            "این کلید مجوز برداشت دارد. برای امنیت، آن را غیرفعال یا با کلید بدون برداشت جایگزین کنید.",
        )
    label = "Spot read/trade (بدون برداشت)"
    return MexcCheck(True, "ok", label)
