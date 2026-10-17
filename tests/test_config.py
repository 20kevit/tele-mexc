"""Config validation and precedence tests (offline)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tg_signal_ingest.config import ConfigError, load_config
from tg_signal_ingest.proxy import ProxyError, telethon_proxy_arg


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
        "# comment\nTELEGRAM_API_ID=999\nTELEGRAM_API_HASH=abc\nexport TELEGRAM_PROXY_PORT=2081\n"
    )
    cfg = load_config(root=tmp_path, environ={}, require_credentials=True)
    assert cfg.api_id == 999
    assert cfg.api_hash == "abc"
    # env file feeds proxy config too
    assert cfg.proxy.port == 2081


def test_toml_overrides(tmp_path: Path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text(
        'backfill_scope = "since"\nbackfill_since = "2025-01-01"\n'
        '[proxy]\ntype = "socks5"\nport = 1234\n'
        '[options]\ndownload_media = true\nrecovery_sweep_size = 42\n'
    )
    cfg = load_config(root=tmp_path, environ={}, require_credentials=True)
    assert cfg.backfill_scope == "since"
    assert cfg.backfill_since == "2025-01-01"
    assert cfg.proxy.type == "socks5"
    assert cfg.proxy.port == 1234
    assert cfg.download_media is True
    assert cfg.recovery_sweep_size == 42


def test_since_scope_requires_date(tmp_path: Path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text('backfill_scope = "since"\n')
    with pytest.raises(ConfigError) as err:
        load_config(root=tmp_path, environ={}, require_credentials=True)
    assert "backfill_since" in str(err.value)


def test_bad_proxy_type(tmp_path: Path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text('[proxy]\ntype = "gre"\n')
    with pytest.raises(ConfigError):
        load_config(root=tmp_path, environ={}, require_credentials=True)


def test_proxy_disabled_rejected_for_telethon(tmp_path: Path):
    cfg = load_config(
        root=tmp_path,
        environ={"TELEGRAM_API_ID": "1", "TELEGRAM_API_HASH": "h"},
        require_credentials=True,
    )
    cfg.proxy.enabled = False
    with pytest.raises(ProxyError):
        telethon_proxy_arg(cfg.proxy)


def test_telethon_proxy_tuple_is_http_or_socks(tmp_path: Path):
    cfg = load_config(root=tmp_path, environ={"TELEGRAM_API_ID": "1", "TELEGRAM_API_HASH": "h"})
    cfg.proxy.type = "http"
    assert telethon_proxy_arg(cfg.proxy)[0] == 3  # PySocks HTTP
    cfg.proxy.type = "socks5"
    assert telethon_proxy_arg(cfg.proxy)[0] == 2  # PySocks SOCKS5


def test_no_credentials_ok_for_connectivity_checks(tmp_path: Path):
    cfg = load_config(root=tmp_path, environ={}, require_credentials=False)
    assert cfg.api_id is None
    assert cfg.proxy.enabled
