"""Confirmation / backup-receipt handoff semantics.

Covers the fix for the incident where a backup delivery had consumed a send
attempt, the process died before the gateway request completed, and the restart
re-posted the backup *after* the alarm had been confirmed.

Rules under test:

* A backup still queued (never dispatched) when the alarm is confirmed is
  cancelled and never reaches the gateway.
* A backup already in flight or whose receipt is unknown at confirmation time
  becomes lookup-only: the original notification number may be queried, but no
  new or repeated POST is ever sent, even when the gateway keeps answering
  "no record" or the lookup endpoint is failing.
* Ambiguous outcomes stay displayed as 待核实 (verification pending) — never as
  cancelled or delivered.
* A late gateway acceptance is shown with the gateway's real acceptance time
  and flagged as delivered after confirmation.
* The rules survive a process restart; normal primary retries for an
  unconfirmed alarm remain available.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import httpx

from tests.conftest import ApiHarness
from tests.helpers import _status
from tests.test_recovery import RestartableApi


def _refuse_client(base_url: str = "http://gateway.test") -> httpx.AsyncClient:
    async def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(refuse), base_url=base_url)


def _backup(view: dict) -> dict:
    backups = [n for n in view["notifications"] if n["role"] == "backup"]
    assert len(backups) == 1, f"expected exactly one backup row: {view}"
    return backups[0]


async def _wait_backup_state(api: ApiHarness, alarm_no: str, states: set[str]) -> bool:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        response = await api.client.get(f"/alarms/{alarm_no}")
        if response.status_code == 200 and any(
            n["role"] == "backup" and n["state"] in states
            for n in response.json()["notifications"]
        ):
            return True
        await asyncio.sleep(0.02)
    return False


async def test_confirmed_backup_with_no_gateway_record_is_never_reposted(api: ApiHarness):
    """The exact incident: backup POST never reached the gateway, repeated
    lookups find no record, and the alarm is confirmed in the meantime. The
    backup must stay 待核实 and must never be re-sent."""
    gateway = api.app.state.gateway
    await api.client.post("/alarms", json=api.alarm_payload("HANDOFF-1", deadline=2))
    assert await api.wait_until(lambda: _status(api, "HANDOFF-1", "awaiting_confirmation"))

    # Cut the wire: the backup POST fails as a transport error, so the gateway
    # has no record and every lookup fails too (query-fault branch).
    real_transport = gateway._client._transport
    gateway._client = _refuse_client(api.settings.gateway_url)
    assert await _wait_backup_state(api, "HANDOFF-1", {"unknown", "in_flight"})
    await asyncio.sleep(0.8)  # several worker ticks against the dead transport

    confirm = await api.client.post(
        "/alarms/HANDOFF-1/confirm", json={"confirmed_by": "张工"}
    )
    assert confirm.status_code == 200
    assert confirm.json()["status"] == "confirmed_after_escalation"

    posts_before = api.fake.post_calls
    await asyncio.sleep(0.8)  # lookup failures while confirmed: still no POST

    # Restore connectivity. The gateway has never seen the backup, so lookups
    # now answer 404 (not-found branch). The old code re-posted after 3 misses.
    gateway._client = httpx.AsyncClient(
        transport=real_transport, base_url=api.settings.gateway_url
    )
    await asyncio.sleep(1.5)

    view = (await api.client.get("/alarms/HANDOFF-1")).json()
    backup = _backup(view)
    assert backup["state"] == "unknown"
    assert backup["display_status"] == "receipt_unconfirmed"
    assert "待核实" in backup["display_text"]
    assert "取消" not in backup["display_text"] and "送达" not in backup["display_text"]
    assert backup["gateway_status"] == "not_found"
    # Only the single pre-confirmation POST attempt exists; no re-send happened.
    assert backup["attempts"] == 1
    assert api.fake.role_notifications("backup") == []
    assert api.fake.post_calls == posts_before
    assert any("不会新发或重发" in w for w in view["warnings"])
    # The primary page is untouched.
    assert len(api.fake.role_notifications("primary")) == 1


async def test_pending_backup_is_cancelled_on_confirmation_and_never_dispatched(api: ApiHarness):
    """A backup row created by the deadline but never claimed is cancelled by
    the confirmation and must not be delivered after delivery resumes."""
    dispatcher = api.app.state.dispatcher
    await api.client.post("/alarms", json=api.alarm_payload("HANDOFF-2", deadline=2))
    assert await api.wait_until(lambda: _status(api, "HANDOFF-2", "awaiting_confirmation"))

    dispatcher.delivery_paused = True
    try:
        assert await _wait_backup_state(api, "HANDOFF-2", {"pending"})
        confirm = await api.client.post(
            "/alarms/HANDOFF-2/confirm", json={"confirmed_by": "张工"}
        )
        assert confirm.status_code == 200
        assert _backup(confirm.json())["state"] == "cancelled"
    finally:
        dispatcher.delivery_paused = False

    await asyncio.sleep(1.0)
    view = (await api.client.get("/alarms/HANDOFF-2")).json()
    assert view["status"] == "confirmed"
    assert _backup(view)["state"] == "cancelled"
    assert api.fake.role_notifications("backup") == []


async def test_late_gateway_acceptance_uses_real_time_and_post_confirmation_flag(api: ApiHarness):
    """If the gateway accepts the in-flight backup only after confirmation, the
    lookup must adopt the gateway's real acceptance timestamp and mark the
    post-confirmation delivery explicitly."""
    gateway = api.app.state.gateway
    await api.client.post("/alarms", json=api.alarm_payload("HANDOFF-3", deadline=2))
    assert await api.wait_until(lambda: _status(api, "HANDOFF-3", "awaiting_confirmation"))

    real_transport = gateway._client._transport
    gateway._client = _refuse_client(api.settings.gateway_url)
    assert await _wait_backup_state(api, "HANDOFF-3", {"unknown", "in_flight"})

    confirm = await api.client.post(
        "/alarms/HANDOFF-3/confirm", json={"confirmed_by": "张工"}
    )
    assert confirm.status_code == 200
    confirmed_at = confirm.json()["confirmed_at"]

    # Gateway comes back and the backup is accepted *after* the confirmation.
    # Sleep first so the gateway timestamp is later than the confirmation but
    # still a real (non-future) time — the worker clamps future timestamps.
    await asyncio.sleep(1.2)
    view = (await api.client.get("/alarms/HANDOFF-3")).json()
    backup_no = _backup(view)["notification_no"]
    late_dt = datetime.now(tz=timezone.utc)
    late_iso = late_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{late_dt.microsecond // 1000:03d}Z"
    assert late_iso > confirmed_at
    api.fake.records[backup_no] = {
        "notification_no": backup_no,
        "alarm_no": "HANDOFF-3",
        "role": "backup",
        "contact": {},
        "status": "accepted",
        "deliveries": 1,
        "duplicate_requests": 0,
        "accepted_at": late_iso,
    }
    gateway._client = httpx.AsyncClient(
        transport=real_transport, base_url=api.settings.gateway_url
    )

    deadline = time.monotonic() + 8
    resolved = False
    while time.monotonic() < deadline:
        view = (await api.client.get("/alarms/HANDOFF-3")).json()
        backup = _backup(view)
        if backup["state"] == "accepted":
            resolved = True
            break
        await asyncio.sleep(0.02)
    assert resolved, view

    assert backup["accepted_at"] == late_iso
    assert backup["accepted_at"] > confirmed_at
    assert backup["delivered_after_confirmation"] is True
    assert "确认后" in backup["display_text"]
    assert any("在途" in w for w in view["warnings"])
    # Exactly one gateway-side delivery: the late acceptance, no API re-POST.
    assert len(api.fake.role_notifications("backup")) == 1
    assert api.fake.records[backup_no]["duplicate_requests"] == 0


async def test_handoff_rules_survive_restart_when_gateway_has_no_record(tmp_path):
    """After a restart the confirmed alarm's unknown backup must remain
    lookup-only: no record at the gateway means it stays 待核实, never re-sent."""
    from tests.fake_gateway import FakeGateway

    fake = FakeGateway()
    db_path = str(tmp_path / "handoff.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        await first.client.post(
            "/alarms",
            json={
                "alarm_no": "HANDOFF-4",
                "content": "供电柜 C03 温度 85°C",
                "primary_contact": {"name": "主值班员"},
                "backup_contact": {"name": "备班"},
                "ack_deadline_seconds": 2,
            },
        )
        assert await first.wait_until(
            lambda: _restart_status(first, "HANDOFF-4", "awaiting_confirmation")
        )
        first.app.state.gateway._client = _refuse_client("http://gw.test")
        assert await _restart_backup(first, "HANDOFF-4", {"unknown", "in_flight"})
        confirm = await first.client.post(
            "/alarms/HANDOFF-4/confirm", json={"confirmed_by": "张工"}
        )
        assert confirm.status_code == 200
    finally:
        await first.stop()

    # New process, same database, gateway still has no backup record.
    posts_before_restart = fake.post_calls
    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        await asyncio.sleep(1.5)  # enough ticks for three not-found misses
        response = await second.client.get("/alarms/HANDOFF-4")
        view = response.json()
        backup = next(n for n in view["notifications"] if n["role"] == "backup")
        assert backup["state"] == "unknown"
        assert "待核实" in backup["display_text"]
        assert fake.role_notifications("backup") == []
        assert fake.post_calls == posts_before_restart, (
            "restart re-posted a confirmed alarm's backup"
        )
    finally:
        await second.stop()


async def test_handoff_rules_survive_restart_when_gateway_has_record(tmp_path):
    """The mirror image: the gateway durably accepted the backup (after the
    confirmation) before the restart. The new process adopts the acceptance via
    lookup, keeps the gateway's real acceptance time, and never re-posts."""
    from tests.fake_gateway import FakeGateway

    fake = FakeGateway()
    db_path = str(tmp_path / "handoff2.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        await first.client.post(
            "/alarms",
            json={
                "alarm_no": "HANDOFF-5",
                "content": "供电柜 C04 温度 86°C",
                "primary_contact": {"name": "主值班员"},
                "backup_contact": {"name": "备班"},
                "ack_deadline_seconds": 2,
            },
        )
        assert await first.wait_until(
            lambda: _restart_status(first, "HANDOFF-5", "awaiting_confirmation")
        )
        first.app.state.gateway._client = _refuse_client("http://gw.test")
        assert await _restart_backup(first, "HANDOFF-5", {"unknown", "in_flight"})
        view = (await first.client.get("/alarms/HANDOFF-5")).json()
        backup_row = next(n for n in view["notifications"] if n["role"] == "backup")
        confirm = await first.client.post(
            "/alarms/HANDOFF-5/confirm", json={"confirmed_by": "张工"}
        )
        assert confirm.status_code == 200
        confirmed_at = confirm.json()["confirmed_at"]

        # Simulate the gateway's late, durable acceptance: it lands strictly
        # after the confirmation, while the API process is still down. Sleep
        # first so the timestamp is a real (non-future) time — the worker
        # clamps future timestamps defensively.
        await asyncio.sleep(1.2)
        late_dt = datetime.now(tz=timezone.utc)
        late_iso = late_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{late_dt.microsecond // 1000:03d}Z"
        assert late_iso > confirmed_at
        fake.records[backup_row["notification_no"]] = {
            "notification_no": backup_row["notification_no"],
            "alarm_no": "HANDOFF-5",
            "role": "backup",
            "contact": {},
            "content_hash": None,
            "payload": {},
            "status": "accepted",
            "deliveries": 1,
            "duplicate_requests": 0,
            "accepted_at": late_iso,
        }
    finally:
        await first.stop()

    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        deadline = time.monotonic() + 10
        accepted = False
        while time.monotonic() < deadline:
            view = (await second.client.get("/alarms/HANDOFF-5")).json()
            backup = next(n for n in view["notifications"] if n["role"] == "backup")
            if backup["state"] == "accepted":
                accepted = True
                break
            await asyncio.sleep(0.02)
        assert accepted, view
        assert backup["accepted_at"] == late_iso
        assert backup["accepted_at"] > confirmed_at
        assert backup["delivered_after_confirmation"] is True
        assert len(fake.role_notifications("backup")) == 1
        assert fake.records[backup["notification_no"]]["duplicate_requests"] == 0
    finally:
        await second.stop()


async def test_claimed_but_undispatched_backup_is_cancelled_by_dispatcher(api: ApiHarness):
    """Tight race: the dispatcher claimed the pending backup (row is in_flight)
    but has not POSTed when the confirmation commits. confirm()'s pending-only
    cancellation misses the row; the dispatcher must then cancel it itself and
    must never hand it to the gateway."""
    import time as _time

    dispatcher = api.app.state.dispatcher
    await api.client.post("/alarms", json=api.alarm_payload("HANDOFF-RACE", deadline=2))
    assert await api.wait_until(lambda: _status(api, "HANDOFF-RACE", "awaiting_confirmation"))

    dispatcher.delivery_paused = True
    # A long lease (set before the claim so the claim itself writes the long
    # value) guarantees the background loop cannot steal the claimed row while
    # the confirmation and the dispatcher's own delivery step run.
    original_lease = dispatcher.lease_seconds
    dispatcher.lease_seconds = 300.0
    try:
        assert await _wait_backup_state(api, "HANDOFF-RACE", {"pending"})
        row = api.app.state.db.query_one(
            "SELECT id FROM notifications "
            " WHERE alarm_no = 'HANDOFF-RACE' AND role = 'backup'"
        )
        assert dispatcher.service.claim_notification(
            row["id"], _time.time(), dispatcher.lease_seconds
        )
        # Defensively extend the persisted lease so no loop tick can reclaim.
        api.app.state.db.execute(
            "UPDATE notifications SET lease_until = ? WHERE id = ?",
            (_time.time() + 300.0, row["id"]),
        )
        claimed = api.app.state.db.query_one(
            "SELECT state FROM notifications WHERE id = ?", (row["id"],)
        )
        assert claimed["state"] == "in_flight"

        posts_before = api.fake.post_calls
        confirm = await api.client.post(
            "/alarms/HANDOFF-RACE/confirm", json={"confirmed_by": "张工"}
        )
        assert confirm.status_code == 200
        # confirm() could not cancel a row already claimed as in_flight.
        assert _backup(confirm.json())["state"] == "in_flight"

        # The claimed row holds an active lease, so the dispatch loop would
        # only pick it up after lease expiry. Drive the claimed row's delivery
        # step directly. dispatch_available() captures the row BEFORE claiming,
        # so the task still sees state=pending even though the DB row is now
        # in_flight; reproduce that exact snapshot.
        claimed_row = dict(
            api.app.state.db.query_one(
                "SELECT * FROM notifications WHERE id = ?", (row["id"],)
            )
        )
        claimed_row["state"] = "pending"
        await dispatcher._run_delivery(claimed_row)
    finally:
        dispatcher.lease_seconds = original_lease
        dispatcher.delivery_paused = False

    cancelled = await _wait_backup_state(api, "HANDOFF-RACE", {"cancelled"})
    assert cancelled, (await api.client.get("/alarms/HANDOFF-RACE")).text
    await asyncio.sleep(0.3)
    view = (await api.client.get("/alarms/HANDOFF-RACE")).json()
    assert view["status"] == "confirmed"
    assert _backup(view)["state"] == "cancelled"
    assert api.fake.post_calls == posts_before
    assert api.fake.role_notifications("backup") == []


async def _restart_status(api: RestartableApi, alarm_no: str, expected: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    return response.status_code == 200 and response.json()["status"] == expected


async def _restart_backup(api: RestartableApi, alarm_no: str, states: set[str]) -> bool:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        response = await api.client.get(f"/alarms/{alarm_no}")
        if response.status_code == 200 and any(
            n["role"] == "backup" and n["state"] in states
            for n in response.json()["notifications"]
        ):
            return True
        await asyncio.sleep(0.02)
    return False
