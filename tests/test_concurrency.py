"""Serializability tests: pool switches racing with lease operations.

The contract is "outcome equals some sequential order".  Each test races a
pool switch against a lease operation and then checks the final state
against both possible serial orders, plus invariants that no interleaving
may violate (an address is never issued twice, a lease issued from a pool is
always inside that pool, a renewal never resurrects an out-of-pool lease).
"""

from __future__ import annotations

import ipaddress
import itertools
import os
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from app.config import Settings
from app.db import ImportItem, LeaseError, Store
from app.subnets import usable_bounds

OLD = "10.20.0.0/29"
NEW = "10.40.0.0/29"

_counter = itertools.count()


def fresh_store(base: Settings) -> Store:
    directory = tempfile.mkdtemp(prefix="lease-race-")
    store = Store(
        base.with_(db_path=os.path.join(directory, f"db-{next(_counter)}.db"))
    )
    store.initialize()
    return store


def _in_pool(cidr: str, ip: str) -> bool:
    first, last = usable_bounds(ipaddress.IPv4Network(cidr))
    return first <= int(ipaddress.IPv4Address(ip)) <= last


def _run_pair(
    left: Callable[[], Any], right: Callable[[], Any]
) -> tuple[tuple[str, Any], tuple[str, Any]]:
    barrier = threading.Barrier(2)

    def wrap(fn: Callable[[], Any]) -> Callable[[], tuple[str, Any]]:
        def inner() -> tuple[str, Any]:
            barrier.wait()
            try:
                return "ok", fn()
            except LeaseError as exc:
                return "err", exc.code

        return inner

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(wrap(left)), pool.submit(wrap(right))]
        return futures[0].result(), futures[1].result()


def test_acquire_racing_switch_is_equivalent_to_serial(settings: Settings) -> None:
    for _ in range(25):
        store = fresh_store(settings)
        start_pool = store.pool_info()["cidr"]
        target = NEW if start_pool == OLD else OLD

        (acquire_kind, acquire_val), (switch_kind, switch_val) = _run_pair(
            lambda: store.acquire("client-a"),
            lambda: store.switch_pool(target),
        )

        assert switch_kind == "ok"
        assert switch_val["changed"] is True
        assert store.pool_info()["cidr"] == target

        assert acquire_kind == "ok", acquire_val
        lease, _reused = acquire_val
        # Serial order 1: acquire first -> lease came from the old pool.
        # Serial order 2: switch first  -> lease came from the new pool.
        assert lease["pool_cidr"] in {start_pool, target}
        assert _in_pool(lease["pool_cidr"], lease["ip"])
        if lease["pool_cidr"] == start_pool:
            first, _ = usable_bounds(ipaddress.IPv4Network(start_pool))
            assert int(ipaddress.IPv4Address(lease["ip"])) == first

        current = store.get_lease(lease["lease_id"])
        assert current["status"] == "active"
        out = {item["lease_id"] for item in store.out_of_pool_leases()}
        assert (lease["lease_id"] in out) == (lease["pool_cidr"] != target)


def test_renew_racing_switch_is_equivalent_to_serial(settings: Settings) -> None:
    for _ in range(25):
        store = fresh_store(settings)
        start_pool = store.pool_info()["cidr"]
        lease, _ = store.acquire("client-a", ttl_seconds=3600)
        target = NEW if start_pool == OLD else OLD

        (renew_kind, renew_val), (switch_kind, _) = _run_pair(
            lambda: store.renew(lease["lease_id"], ttl_seconds=7200),
            lambda: store.switch_pool(target),
        )

        assert switch_kind == "ok"
        if renew_kind == "ok":
            # Renew committed before the switch: it saw the old pool.
            assert renew_val["status"] == "active"
            assert renew_val["seconds_remaining"] == 7200
            final = store.get_lease(lease["lease_id"])
            assert final["status"] == "active"
            assert final["ip"] == lease["ip"]
        else:
            assert renew_val == "address_out_of_pool"
            final = store.get_lease(lease["lease_id"])
            assert final["status"] == "active"
            assert final["lease_id"] == lease["lease_id"]
            assert final["expires_at"] == lease["expires_at"]


def test_release_racing_switch_never_breaks_invariants(settings: Settings) -> None:
    for _ in range(25):
        store = fresh_store(settings)
        lease, _ = store.acquire("client-a")
        start_pool = store.pool_info()["cidr"]
        target = NEW if start_pool == OLD else OLD

        (release_kind, _), (switch_kind, _) = _run_pair(
            lambda: store.release(lease["lease_id"]),
            lambda: store.switch_pool(target),
        )
        assert release_kind == "ok"
        assert switch_kind == "ok"
        final = store.get_lease(lease["lease_id"])
        assert final["status"] == "released"
        assert store.out_of_pool_leases() == []
        # The address is free again in the old pool's space.
        store.switch_pool(start_pool)
        fresh, _ = store.acquire("client-b")
        assert fresh["ip"] == lease["ip"]


def test_concurrent_acquires_never_share_an_address(settings: Settings) -> None:
    store = fresh_store(settings)

    def acquire(i: int) -> str | None:
        try:
            return store.acquire(f"client-{i}")[0]["ip"]
        except LeaseError as exc:
            assert exc.code == "pool_exhausted"
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        addresses = list(pool.map(acquire, range(12)))

    allocated = [a for a in addresses if a is not None]
    assert len(allocated) == len(set(allocated))
    assert len(allocated) == 6  # /29 capacity


def test_import_racing_switch_is_equivalent_to_serial(settings: Settings) -> None:
    """An import is validated against the pool as of its write transaction.

    Racing an import with a pool switch must equal one of the two serial
    orders: the import committed under the old pool (its leases are now out
    of pool but still owned), or the switch committed first and the import
    is rejected as out of range.
    """
    for _ in range(25):
        store = fresh_store(settings)
        start_pool = store.pool_info()["cidr"]
        target = NEW if start_pool == OLD else OLD
        # Usable in the starting pool, outside the target pool.
        ip = "10.20.0.6" if start_pool == OLD else "10.40.0.6"
        items = [ImportItem("legacy-1", ip, time.time() + 3600)]

        (import_kind, import_val), (switch_kind, _) = _run_pair(
            lambda: store.import_leases("batch-1", items),
            lambda: store.switch_pool(target),
        )

        assert switch_kind == "ok"
        if import_kind == "ok":
            # Serial order: import first, under the starting pool.
            receipt, replayed = import_val
            assert replayed is False
            assert receipt["pool_cidr"] == start_pool
            out = store.out_of_pool_leases()
            assert [lease["ip"] for lease in out] == [ip]
            # The receipt still replays after the switch.
            again, replayed = store.import_leases("batch-1", items)
            assert replayed is True
            assert again == receipt
        else:
            # Serial order: switch first; the address is out of range now.
            assert import_val == "address_out_of_pool"
            assert store.pool_info()["occupied_out_of_pool"] == 0


def test_concurrent_same_batch_import_replays(settings: Settings) -> None:
    """Two simultaneous submissions of one batch: one imports, one replays."""
    store = fresh_store(settings)
    items = [
        ImportItem("legacy-1", "10.20.0.4", time.time() + 3600),
        ImportItem("legacy-2", "10.20.0.5", time.time() + 3600),
    ]

    (kind_a, result_a), (kind_b, result_b) = _run_pair(
        lambda: store.import_leases("batch-1", items),
        lambda: store.import_leases("batch-1", items),
    )

    assert kind_a == kind_b == "ok"
    receipt_a, replayed_a = result_a
    receipt_b, replayed_b = result_b
    assert replayed_a != replayed_b  # exactly one of them created the batch
    assert receipt_a == receipt_b
    assert store.pool_info()["allocated_in_pool"] == 2


def test_concurrent_imports_never_double_book_an_address(settings: Settings) -> None:
    """Two batches claiming one address: exactly one commits."""
    for _ in range(25):
        store = fresh_store(settings)
        expiry = time.time() + 3600
        left = [ImportItem("client-a", "10.20.0.4", expiry)]
        right = [ImportItem("client-b", "10.20.0.4", expiry)]

        (kind_a, val_a), (kind_b, val_b) = _run_pair(
            lambda: store.import_leases("batch-1", left),
            lambda: store.import_leases("batch-2", right),
        )

        assert sorted([kind_a, kind_b]) == ["err", "ok"]
        assert (val_a if kind_a == "err" else val_b) == "import_conflict"
        assert store.pool_info()["allocated_in_pool"] == 1
