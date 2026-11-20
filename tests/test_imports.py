"""Legacy-DHCP lease import: validation, atomicity, replay, pool interplay."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import ImportItem, LeaseError, Store
from app.main import create_app

# The API fixtures run on a fake clock starting at 2027-01-15, so these
# constants are safely in the future / past for both store- and HTTP-level
# tests.
FUTURE = "2099-06-01T00:00:00Z"
PAST = "2001-01-01T00:00:00Z"


# ------------------------------------------------------------------- store


def test_import_registers_leases_with_original_expiry(store: Store, clock) -> None:
    expiry = clock.value + 7200
    receipt, replayed = store.import_leases(
        "batch-1",
        [
            ImportItem("printer", "10.20.0.4", expiry),
            ImportItem("scanner", "10.20.0.5", expiry + 60),
        ],
    )
    assert replayed is False
    assert receipt["batch_id"] == "batch-1"
    assert receipt["pool_cidr"] == "10.20.0.0/29"
    assert receipt["skipped"] == []
    assert [e["client_id"] for e in receipt["imported"]] == ["printer", "scanner"]

    lease = store.get_lease(receipt["imported"][0]["lease_id"])
    assert lease["ip"] == "10.20.0.4"
    assert lease["expires_at"] == receipt["imported"][0]["expires_at"]
    assert lease["import_batch_id"] == "batch-1"
    assert lease["status"] == "active"
    # The original expiry is kept, not the default TTL.
    assert lease["seconds_remaining"] == pytest.approx(7200, abs=2)


def test_imported_client_reclaims_original_address(store: Store, clock) -> None:
    receipt, _ = store.import_leases(
        "batch-1", [ImportItem("printer", "10.20.0.4", clock.value + 3600)]
    )
    lease, reused = store.acquire("printer")
    assert reused is True
    assert lease["lease_id"] == receipt["imported"][0]["lease_id"]
    assert lease["ip"] == "10.20.0.4"


def test_import_skips_expired_entries(store: Store, clock) -> None:
    receipt, _ = store.import_leases(
        "batch-1",
        [
            ImportItem("dead", "10.20.0.4", clock.value - 1),
            ImportItem("alive", "10.20.0.5", clock.value + 3600),
        ],
    )
    assert [e["client_id"] for e in receipt["imported"]] == ["alive"]
    assert len(receipt["skipped"]) == 1
    skipped = receipt["skipped"][0]
    assert skipped["index"] == 0
    assert skipped["client_id"] == "dead"
    assert skipped["reason"] == "expired"
    # The skipped entry never occupied its address.
    assert store.pool_info()["allocated_in_pool"] == 1


def test_import_rejects_duplicate_client_id(store: Store, clock) -> None:
    expiry = clock.value + 3600
    with pytest.raises(LeaseError) as exc:
        store.import_leases(
            "batch-1",
            [
                ImportItem("printer", "10.20.0.4", expiry),
                ImportItem("printer", "10.20.0.5", expiry),
            ],
        )
    assert exc.value.status_code == 422
    assert exc.value.code == "invalid_import"
    entry = exc.value.details["entries"][0]
    assert entry["reason"] == "duplicate_client_id"
    assert entry["index"] == 1
    assert entry["first_index"] == 0
    # Whole batch rejected: nothing persisted, and the batch id stays
    # usable once the operator fixes the export.
    assert store.pool_info()["allocated_in_pool"] == 0
    receipt, replayed = store.import_leases(
        "batch-1", [ImportItem("printer", "10.20.0.4", expiry)]
    )
    assert replayed is False
    assert len(receipt["imported"]) == 1


def test_import_rejects_duplicate_ip(store: Store, clock) -> None:
    expiry = clock.value + 3600
    with pytest.raises(LeaseError) as exc:
        store.import_leases(
            "batch-1",
            [
                ImportItem("printer", "10.20.0.4", expiry),
                ImportItem("scanner", "10.20.0.4", expiry),
            ],
        )
    assert exc.value.code == "invalid_import"
    entry = exc.value.details["entries"][0]
    assert entry["reason"] == "duplicate_ip"
    assert entry["index"] == 1
    assert store.pool_info()["allocated_in_pool"] == 0


def test_import_rejects_addresses_outside_pool(store: Store, clock) -> None:
    expiry = clock.value + 3600
    with pytest.raises(LeaseError) as exc:
        store.import_leases(
            "batch-1",
            [
                ImportItem("fine", "10.20.0.4", expiry),
                ImportItem("bcast", "10.20.0.7", expiry),  # broadcast of /29
                ImportItem("away", "10.20.9.4", expiry),  # outside the pool
                ImportItem("net", "10.20.0.0", expiry),  # network address
            ],
        )
    assert exc.value.status_code == 422
    assert exc.value.code == "address_out_of_pool"
    entries = exc.value.details["entries"]
    assert {e["index"] for e in entries} == {1, 2, 3}
    assert all(e["reason"] == "address_out_of_pool" for e in entries)
    assert store.pool_info()["allocated_in_pool"] == 0


def test_import_rejects_client_with_active_lease(store: Store, clock) -> None:
    existing, _ = store.acquire("printer")
    with pytest.raises(LeaseError) as exc:
        store.import_leases(
            "batch-1", [ImportItem("printer", "10.20.0.4", clock.value + 3600)]
        )
    assert exc.value.status_code == 409
    assert exc.value.code == "import_conflict"
    entry = exc.value.details["entries"][0]
    assert entry["reason"] == "client_has_active_lease"
    assert entry["lease_id"] == existing["lease_id"]


def test_import_rejects_address_already_leased(store: Store, clock) -> None:
    existing, _ = store.acquire("printer")  # holds 10.20.0.1
    with pytest.raises(LeaseError) as exc:
        store.import_leases(
            "batch-1", [ImportItem("other", "10.20.0.1", clock.value + 3600)]
        )
    assert exc.value.code == "import_conflict"
    entry = exc.value.details["entries"][0]
    assert entry["reason"] == "address_already_leased"
    assert entry["lease_id"] == existing["lease_id"]


def test_import_is_all_or_nothing(store: Store, clock) -> None:
    store.acquire("busy")  # holds 10.20.0.1
    with pytest.raises(LeaseError) as exc:
        store.import_leases(
            "batch-1",
            [
                ImportItem("fine", "10.20.0.4", clock.value + 3600),
                ImportItem("clash", "10.20.0.1", clock.value + 3600),
            ],
        )
    assert exc.value.code == "import_conflict"
    # Only the pre-existing lease remains; the valid entry was not imported.
    assert store.pool_info()["allocated_in_pool"] == 1


def test_replay_returns_original_receipt_after_release(store: Store, clock) -> None:
    items = [ImportItem("printer", "10.20.0.4", clock.value + 3600)]
    receipt, replayed = store.import_leases("batch-1", items)
    assert replayed is False
    store.release(receipt["imported"][0]["lease_id"])

    again, replayed = store.import_leases("batch-1", items)
    assert replayed is True
    assert again == receipt
    # The replay did not resurrect the released lease.
    assert store.get_lease(receipt["imported"][0]["lease_id"])["status"] == "released"


def test_replay_survives_restart(settings: Settings, clock) -> None:
    items = [ImportItem("printer", "10.20.0.4", clock.value + 3600)]
    first = Store(settings, clock=clock)
    first.initialize()
    receipt, _ = first.import_leases("batch-1", items)

    reopened = Store(settings, clock=clock)
    reopened.initialize()
    again, replayed = reopened.import_leases("batch-1", items)
    assert replayed is True
    assert again == receipt


def test_same_batch_id_with_different_list_is_rejected(store: Store, clock) -> None:
    expiry = clock.value + 3600
    store.import_leases("batch-1", [ImportItem("printer", "10.20.0.4", expiry)])
    with pytest.raises(LeaseError) as exc:
        store.import_leases("batch-1", [ImportItem("printer", "10.20.0.5", expiry)])
    assert exc.value.status_code == 409
    assert exc.value.code == "import_batch_conflict"
    # Changing only the expiry also counts as a different list.
    with pytest.raises(LeaseError) as exc:
        store.import_leases("batch-1", [ImportItem("printer", "10.20.0.4", expiry + 60)])
    assert exc.value.code == "import_batch_conflict"


def test_import_validates_against_pool_at_write_time(store: Store, clock) -> None:
    items = [ImportItem("printer", "10.20.0.4", clock.value + 3600)]
    receipt, _ = store.import_leases("batch-1", items)
    store.switch_pool("10.99.0.0/29")
    # The same addresses are out of range under the new pool.
    with pytest.raises(LeaseError) as exc:
        store.import_leases("batch-2", items)
    assert exc.value.code == "address_out_of_pool"
    # The imported lease still belongs to its client, even out of pool.
    lease, reused = store.acquire("printer")
    assert reused is True
    assert lease["ip"] == "10.20.0.4"
    # And the original batch still replays its stored receipt.
    again, replayed = store.import_leases("batch-1", items)
    assert replayed is True
    assert again == receipt


def test_imported_lease_expires_on_schedule(store: Store, clock) -> None:
    items = [ImportItem("printer", "10.20.0.4", clock.value + 60)]
    receipt, _ = store.import_leases("batch-1", items)
    clock.advance(61)
    lease, reused = store.acquire("printer")
    assert reused is False
    assert lease["lease_id"] != receipt["imported"][0]["lease_id"]
    assert store.get_lease(receipt["imported"][0]["lease_id"])["status"] == "expired"


def test_import_migrates_pre_existing_database(settings: Settings, clock) -> None:
    """A database written before imports existed is upgraded in place."""
    import sqlite3

    conn = sqlite3.connect(settings.db_path)
    conn.executescript(
        """
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO settings VALUES ('current_pool', '10.20.0.0/29');
        CREATE TABLE leases (
            lease_id TEXT PRIMARY KEY, client_id TEXT NOT NULL,
            ip TEXT NOT NULL, pool_cidr TEXT NOT NULL,
            issued_at REAL NOT NULL, expires_at REAL NOT NULL,
            released_at REAL, release_reason TEXT
        );
        """
    )
    conn.close()

    store = Store(settings, clock=clock)
    store.initialize()  # must not fail on the old schema
    receipt, _ = store.import_leases(
        "batch-1", [ImportItem("printer", "10.20.0.4", clock.value + 3600)]
    )
    assert store.get_lease(receipt["imported"][0]["lease_id"])["import_batch_id"] == "batch-1"


# --------------------------------------------------------------------- api


def test_import_endpoint_happy_path_and_replay(client: TestClient) -> None:
    body = {
        "batch_id": "legacy-1",
        "entries": [
            {"client_id": "printer", "ip": "10.20.0.4", "expires_at": FUTURE},
            {"client_id": "retired", "ip": "10.20.0.5", "expires_at": PAST},
        ],
    }
    first = client.post("/v1/admin/lease-imports", json=body)
    assert first.status_code == 201
    payload = first.json()
    assert payload["replayed"] is False
    assert payload["batch_id"] == "legacy-1"
    assert payload["pool_cidr"] == "10.20.0.0/29"
    assert [e["client_id"] for e in payload["imported"]] == ["printer"]
    assert payload["skipped"][0]["client_id"] == "retired"
    assert payload["skipped"][0]["reason"] == "expired"

    lease_id = payload["imported"][0]["lease_id"]
    lease = client.get(f"/v1/leases/{lease_id}").json()
    assert lease["import_batch_id"] == "legacy-1"
    assert lease["expires_at"] == payload["imported"][0]["expires_at"]

    # The device re-claims its original address with its legacy id.
    claim = client.post("/v1/leases", json={"client_id": "printer"})
    assert claim.status_code == 200
    assert claim.json()["reused"] is True
    assert claim.json()["ip"] == "10.20.0.4"
    assert claim.json()["lease_id"] == lease_id

    replay = client.post("/v1/admin/lease-imports", json=body)
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["imported"] == payload["imported"]
    assert replay.json()["skipped"] == payload["skipped"]


def test_import_requires_operator_token(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "leases.db"),
        default_pool_cidr="10.20.0.0/29",
        operator_token="s3cret",
    )
    body = {
        "batch_id": "b",
        "entries": [{"client_id": "a", "ip": "10.20.0.4", "expires_at": FUTURE}],
    }
    with TestClient(create_app(settings, store=Store(settings))) as locked:
        assert locked.post("/v1/admin/lease-imports", json=body).status_code == 401
        assert (
            locked.post(
                "/v1/admin/lease-imports",
                json=body,
                headers={"X-Operator-Token": "wrong"},
            ).status_code
            == 401
        )
        ok = locked.post(
            "/v1/admin/lease-imports",
            json=body,
            headers={"X-Operator-Token": "s3cret"},
        )
        assert ok.status_code == 201


def test_import_request_validation(client: TestClient) -> None:
    def post(entries, batch_id="b"):
        return client.post(
            "/v1/admin/lease-imports",
            json={"batch_id": batch_id, "entries": entries},
        )

    good = {"client_id": "a", "ip": "10.20.0.4", "expires_at": FUTURE}
    response = post([{**good, "ip": "999.1.1.1"}])
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    # A naive timestamp (no UTC offset) is rejected.
    assert post([{**good, "expires_at": "2099-06-01T00:00:00"}]).status_code == 422
    # Empty batch, blank ids and unexpected keys are rejected too.
    assert post([]).status_code == 422
    assert post([good], batch_id="  ").status_code == 422
    assert post([{**good, "note": "x"}]).status_code == 422
    missing = client.post("/v1/admin/lease-imports", json={"entries": [good]})
    assert missing.status_code == 422


def test_import_conflict_error_lists_entries(client: TestClient) -> None:
    client.post("/v1/leases", json={"client_id": "laptop"})  # holds 10.20.0.1
    body = {
        "batch_id": "legacy-1",
        "entries": [
            {"client_id": "laptop", "ip": "10.20.0.4", "expires_at": FUTURE},
            {"client_id": "other", "ip": "10.20.0.1", "expires_at": FUTURE},
        ],
    }
    response = client.post("/v1/admin/lease-imports", json=body)
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "import_conflict"
    reasons = {(e["index"], e["reason"]) for e in error["entries"]}
    assert (0, "client_has_active_lease") in reasons
    assert (1, "address_already_leased") in reasons
    # Nothing was imported.
    assert client.get("/v1/pool").json()["allocated_in_pool"] == 1


def test_import_batch_id_reuse_with_different_list(client: TestClient) -> None:
    body = {
        "batch_id": "legacy-1",
        "entries": [{"client_id": "a", "ip": "10.20.0.4", "expires_at": FUTURE}],
    }
    assert client.post("/v1/admin/lease-imports", json=body).status_code == 201
    changed = {
        "batch_id": "legacy-1",
        "entries": [{"client_id": "b", "ip": "10.20.0.5", "expires_at": FUTURE}],
    }
    response = client.post("/v1/admin/lease-imports", json=changed)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "import_batch_conflict"
