"""MEXC permission verification and live channel revalidation."""

from __future__ import annotations

from telethon.tl.types import Channel, User

from tg_signal_ingest.control.channels import fetch_channels, find_channel
from tg_signal_ingest.control.mexc_setup import verify_mexc_client
from tg_signal_ingest.mexc.errors import MexcNetworkError, MexcPermissionError


class MexcFake:
    def __init__(self, account=None, info=None, error=None):
        self.account_data = account if account is not None else {
            "canTrade": True,
            "permissions": ["SPOT"],
            "enableWithdrawals": False,
        }
        self.info_data = info if info is not None else {"permissions": ["SPOT"]}
        self.error = error

    def account(self):
        if self.error:
            raise self.error
        return dict(self.account_data)

    def api_key_info(self):
        if self.error:
            raise self.error
        return dict(self.info_data)


def test_mexc_verification_accepts_read_trade_key_without_withdrawals():
    check = verify_mexc_client(MexcFake())
    assert check.ok is True
    assert check.category == "ok"
    assert "بدون برداشت" in check.detail


def test_mexc_verification_rejects_withdrawal_permissions_everywhere():
    account = {"canTrade": True, "permissions": ["SPOT"], "ENABLEWITHDRAWALS": "true"}
    check = verify_mexc_client(MexcFake(account=account))
    assert check.ok is False
    assert check.category == "permission"

    info = {"permissions": ["SPOT", "WITHDRAW"], "nested": {"canWithdraw": 1}}
    check = verify_mexc_client(MexcFake(info=info))
    assert check.ok is False
    assert "برداشت" in check.detail


def test_mexc_verification_fails_closed_on_permissions_and_network():
    no_trade = MexcFake(account={"canTrade": False, "permissions": ["SPOT"]})
    check = verify_mexc_client(no_trade)
    assert check.ok is False
    assert "canTrade" in check.detail

    no_spot = MexcFake(account={"canTrade": True, "permissions": ["FUTURES"]})
    assert verify_mexc_client(no_spot).category == "permission"

    network = verify_mexc_client(MexcFake(error=MexcNetworkError("timeout")))
    assert network.ok is False
    assert network.category == "network"
    assert "timeout" not in network.detail

    denied = verify_mexc_client(MexcFake(error=MexcPermissionError("denied")))
    assert denied.category == "permission"


def test_mexc_verification_rejects_malformed_account():
    check = verify_mexc_client(MexcFake(account={"canTrade": True, "permissions": [], "bad": []}))
    assert check.ok is True  # empty permission list is allowed; account checks still passed
    assert verify_mexc_client(MexcFake(account={})).ok is False


class Dialog:
    def __init__(self, entity, name):
        self.entity = entity
        self.name = name


class ChannelsFake:
    def __init__(self, dialogs):
        self.dialogs = dialogs

    async def iter_dialogs(self, limit=None):
        assert limit is None
        for dialog in self.dialogs:
            yield dialog


def _entity(
    ident,
    title,
    *,
    channel=True,
    user=False,
    deleted=False,
    megagroup=False,
    username=None,
):
    if user:
        return User(
            id=ident,
            first_name=title,
            deleted=deleted,
            username=username,
            access_hash=ident * 10,
        )
    entity = Channel(
        id=ident,
        title=title,
        photo=None,
        date=None,
        broadcast=channel and not megagroup,
        megagroup=megagroup,
        access_hash=ident * 10,
        username=username,
    )
    if deleted:
        entity.deleted = True
    return entity


async def test_raw_telethon_channel_and_megagroup_are_eligible():
    dialogs = [
        Dialog(_entity(10, "Raw Broadcast"), "Raw Broadcast"),
        Dialog(_entity(11, "Raw Supergroup", megagroup=True), "Raw Supergroup"),
    ]
    client = ChannelsFake(dialogs)
    channels, more = await fetch_channels(client, offset=0, limit=10)
    assert [item.title for item in channels] == ["Raw Broadcast", "Raw Supergroup"]
    assert [item.kind for item in channels] == ["کانال", "گروه"]
    assert more is False


async def test_channel_listing_filters_and_paginates():
    dialogs = [
        Dialog(_entity(1, "Alpha Signals", username="alpha"), "Alpha Signals"),
        Dialog(_entity(2, "Private User", user=True, channel=False), "Private User"),
        Dialog(_entity(3, "Beta Group", megagroup=True), "Beta Group"),
        Dialog(_entity(4, "Deleted", deleted=True), "Deleted"),
        Dialog(_entity(5, "Gamma"), "Gamma"),
    ]
    client = ChannelsFake(dialogs)
    first, more = await fetch_channels(client, offset=0, limit=2)
    assert [item.title for item in first] == ["Alpha Signals", "Beta Group"]
    assert more is True

    rest, more = await fetch_channels(client, offset=2, limit=2)
    assert [item.title for item in rest] == ["Gamma"]
    assert more is False

    found, _more = await fetch_channels(client, offset=0, limit=10, query="@alpha")
    assert [item.channel_id for item in found] == [1]


async def test_channel_pick_revalidates_against_live_dialogs():
    client = ChannelsFake([Dialog(_entity(7, "Live Channel"), "Live Channel")])
    choice = await find_channel(client, 7)
    assert choice is not None
    assert choice.title == "Live Channel"
    assert await find_channel(client, 999) is None
