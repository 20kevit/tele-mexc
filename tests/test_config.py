"""Config validation and precedence tests (offline)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tg_signal_ingest.config import ConfigError, load_config


def test_missing_credentials_raise(tmp_path: Path):
    with pytest.raises(ConfigError) as err:
        load_config(root=tmp_path, environ={}, require_credentials=True)
    assert "TELEGRAM_API_ID" in str(err.value)


def test_credentials_from_env(tmp_path: Path):
    cfg = load_config(
        root=tmp_path,
        environ={"TELEGRAM_API_ID": "123456", "TELEGRAM_API_HASH": "x" * 32},
        require_credentials=True,
    )
    assert cfg.api_id == 123456
    assert cfg.api_hash == "x" * 32


def test_invalid_api_id_type(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_config(root=tmp_path, environ={"TELEGRAM_API_ID": "not-a-number"}, require_credentials=True)


def test_env_file_values_loaded(tmp_path: Path):
    (tmp_path / ".env").write_text(
        "# comment\nexport TELEGRAM_API_ID=999\nTELEGRAM_API_HASH=abc\n"
    )
    cfg = load_config(root=tmp_path, environ={}, require_credentials=True)
    assert cfg.api_id == 999
    assert cfg.api_hash == "abc"
    assert not hasattr(cfg, "proxy")


def test_toml_overrides(tmp_path: Path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text(
        'backfill_scope = "since"\nbackfill_since = "2025-01-01"\n'
        '[options]\ndownload_media = true\nrecovery_sweep_size = 42\n'
    )
    cfg = load_config(root=tmp_path, environ={}, require_credentials=True)
    assert cfg.backfill_scope == "since"
    assert cfg.backfill_since == "2025-01-01"
    assert cfg.download_media is True
    assert cfg.recovery_sweep_size == 42


def test_session_path_env_overrides_toml(tmp_path: Path):
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text(
        'session_path = "config/employer_session"\n'
    )
    cfg = load_config(
        root=tmp_path,
        environ={
            "TELEGRAM_API_ID": "1",
            "TELEGRAM_API_HASH": "h",
            "TG_SESSION_PATH": "config/personal_session",
        },
        require_credentials=True,
    )
    assert cfg.session_path == Path("config/personal_session")


def test_session_path_toml_used_without_env_override(tmp_path: Path):
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text(
        'session_path = "config/employer_session"\n'
    )
    cfg = load_config(
        root=tmp_path,
        environ={"TELEGRAM_API_ID": "1", "TELEGRAM_API_HASH": "h"},
        require_credentials=True,
    )
    assert cfg.session_path == Path("config/employer_session")


def test_since_scope_requires_date(tmp_path: Path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text('backfill_scope = "since"\n')
    with pytest.raises(ConfigError) as err:
        load_config(root=tmp_path, environ={}, require_credentials=True)
    assert "backfill_since" in str(err.value)


def test_legacy_proxy_environment_is_rejected(tmp_path: Path):
    with pytest.raises(ConfigError, match="direct networking"):
        load_config(
            root=tmp_path,
            environ={
                "TELEGRAM_API_ID": "1",
                "TELEGRAM_API_HASH": "h",
                "TELEGRAM_PROXY_HOST": "127.0.0.1",
                "TELEGRAM_PROXY_PORT": "1081",
            },
            require_credentials=True,
        )


def test_legacy_proxy_toml_section_is_rejected(tmp_path: Path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text('[proxy]\ntype = "http"\n')
    with pytest.raises(ConfigError, match="\\[proxy\\]"):
        load_config(root=tmp_path, environ={}, require_credentials=True)


def test_no_credentials_ok_for_connectivity_checks(tmp_path: Path):
    cfg = load_config(root=tmp_path, environ={}, require_credentials=False)
    assert cfg.api_id is None
    assert cfg.api_hash is None
    assert not hasattr(cfg, "proxy")


def test_owner_and_developer_ids_are_separate_and_numeric(tmp_path: Path):
    base = {
        "TELEGRAM_API_ID": "1",
        "TELEGRAM_API_HASH": "h",
        "CONTROL_BOT_TOKEN": "123:abc",
        "CONTROL_OWNER_USER_ID": "111",
        "CONTROL_DEVELOPER_USER_ID": "222",
    }
    cfg = load_config(root=tmp_path, environ=base, require_credentials=True)
    assert cfg.control.owner_user_id == 111
    assert cfg.control.developer_user_id == 222
    assert cfg.control.configured is True
    assert cfg.control.management_configured is True

    with pytest.raises(ConfigError, match="numeric"):
        load_config(
            root=tmp_path,
            environ={**base, "CONTROL_DEVELOPER_USER_ID": "developer"},
            require_credentials=True,
        )
    with pytest.raises(ConfigError, match="different"):
        load_config(
            root=tmp_path,
            environ={**base, "CONTROL_DEVELOPER_USER_ID": "111"},
            require_credentials=True,
        )
    with pytest.raises(ConfigError, match="positive"):
        load_config(
            root=tmp_path,
            environ={**base, "CONTROL_DEVELOPER_USER_ID": "-2"},
            require_credentials=True,
        )
