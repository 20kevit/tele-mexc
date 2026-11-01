"""Control-bot config loading (offline, no secrets committed)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tg_signal_ingest.config import ConfigError, load_config


def _root(tmp_path: Path, toml_text: str = "") -> Path:
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h\n")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text(toml_text)
    return tmp_path


def test_control_env_is_loaded_and_enables_configured_bot(tmp_path):
    cfg = load_config(
        root=_root(tmp_path),
        environ={
            "CONTROL_BOT_TOKEN": "123:token",
            "CONTROL_OWNER_USER_ID": "4242",
            "CONTROL_OWNER_B_USER_ID": "4243",
            "CONTROL_DEVELOPER_USER_ID": "4244",
            "CONTROL_SECRET_KEY": "secret-key-value",
        },
        require_credentials=False,
    )
    assert cfg.control.bot_token == "123:token"
    assert cfg.control.owner_user_id == 4242
    assert cfg.control.owner_b_user_id == 4243
    assert cfg.control.developer_user_id == 4244
    assert cfg.control.owner_ids == (4242, 4243)
    assert cfg.control.secret_key == "secret-key-value"
    assert cfg.control.enabled is True
    assert cfg.control.configured is True
    assert cfg.control.owners_configured is True


def test_control_roles_must_be_distinct(tmp_path):
    with pytest.raises(ConfigError, match="different"):
        load_config(
            root=_root(tmp_path),
            environ={
                "CONTROL_BOT_TOKEN": "123:token",
                "CONTROL_OWNER_USER_ID": "4242",
                "CONTROL_OWNER_B_USER_ID": "4242",
            },
            require_credentials=False,
        )
    with pytest.raises(ConfigError, match="different"):
        load_config(
            root=_root(tmp_path),
            environ={
                "CONTROL_BOT_TOKEN": "123:token",
                "CONTROL_OWNER_USER_ID": "4242",
                "CONTROL_DEVELOPER_USER_ID": "4242",
            },
            require_credentials=False,
        )


def test_control_can_be_explicitly_disabled(tmp_path):
    cfg = load_config(
        root=_root(tmp_path),
        environ={
            "CONTROL_BOT_TOKEN": "123:token",
            "CONTROL_OWNER_USER_ID": "4242",
            "CONTROL_BOT_ENABLED": "false",
        },
        require_credentials=False,
    )
    assert cfg.control.configured is True
    assert cfg.control.enabled is False


def test_invalid_owner_id_fails_closed_and_legacy_approval_setting_is_ignored(tmp_path):
    with pytest.raises(ConfigError, match="numeric"):
        load_config(
            root=_root(tmp_path),
            environ={"CONTROL_OWNER_USER_ID": "owner"},
            require_credentials=False,
        )
    with pytest.raises(ConfigError, match="positive"):
        load_config(
            root=_root(tmp_path),
            environ={"CONTROL_OWNER_USER_ID": "-1"},
            require_credentials=False,
        )
    legacy = load_config(
        root=_root(tmp_path, "[control]\napproval_ttl_s = 5"),
        environ={},
        require_credentials=False,
    )
    assert not hasattr(legacy.control, "approval_ttl_s")


def test_explicitly_enabled_but_incomplete_bot_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="both required"):
        load_config(
            root=_root(tmp_path),
            environ={"CONTROL_BOT_ENABLED": "true", "CONTROL_BOT_TOKEN": "123:token"},
            require_credentials=False,
        )


def test_control_rejects_insecure_api_base_and_malformed_token(tmp_path):
    with pytest.raises(ConfigError, match="https"):
        load_config(
            root=_root(tmp_path),
            environ={"CONTROL_API_BASE": "http://insecure.example"},
            require_credentials=False,
        )
    with pytest.raises(ConfigError, match="malformed"):
        load_config(
            root=_root(tmp_path),
            environ={"CONTROL_BOT_TOKEN": "missing-colon"},
            require_credentials=False,
        )
    with pytest.raises(ConfigError, match="at least 16 bytes"):
        load_config(
            root=_root(tmp_path),
            environ={"CONTROL_SECRET_KEY": "short"},
            require_credentials=False,
        )
