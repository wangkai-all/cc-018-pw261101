from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.catalog import Catalog
from app.compactor import TaskManager
from app.config import Settings
from app.main import create_app
from app.storage import generate_events, write_parquet


def make_client(tmp_path: Path, *, seed_files: int = 24, max_files: int = 12) -> TestClient:
    settings = Settings(
        data_root=tmp_path,
        seed_files=seed_files,
        seed_rows=8,
        max_compact_files=max_files,
        min_compact_files=2,
        auto_seed=True,
    )
    app = create_app(settings)
    return TestClient(app)


def poll_task(client: TestClient, task_id: str, timeout: float = 20.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = client.get(f"/api/tasks/{task_id}")
        assert task.status_code == 200, task.text
        payload = task.json()
        if payload["status"] != "running":
            return payload
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not finish in time")


def test_append_and_fixed_snapshot_are_exact(tmp_path):
    with make_client(tmp_path, seed_files=20, max_files=8) as client:
        state = client.get("/api/state").json()
        base = client.get("/api/snapshot").json()
        before_rows = base["query"]["rows"]
        before_amount = base["query"]["total_amount_cents"]
        assert base["complete"] is True

        append = client.post("/api/append", json={"rows": 13}).json()
        after = client.get("/api/snapshot").json()
        old = client.get(f"/api/snapshot?version={base['snapshot_version']}").json()

        assert after["query"]["rows"] == before_rows + 13
        assert (
            after["query"]["total_amount_cents"]
            == before_amount + append["file"]["amount_cents"]
        )
        assert after["query"]["distinct_event_ids"] == after["query"]["rows"]
        assert old["query"]["rows"] == before_rows
        assert old["query"]["total_amount_cents"] == before_amount
        assert old["complete"] is True
        assert old["snapshot_version"] == base["snapshot_version"]
        assert old["current_version"] == after["snapshot_version"]
        assert state["current_version"] == base["snapshot_version"]


def test_append_during_compaction_conflicts_and_replans(tmp_path):
    with make_client(tmp_path, seed_files=28, max_files=10) as client:
        response = client.post(
            "/api/demo/append-during-compaction",
            json={"max_files": 6, "publish_delay_ms": 700},
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["skipped"] is False
        assert result["conflict_count"] >= 1
        assert all(result["checks"].values()), result["checks"]
        assert result["after"]["rows"] == result["before"]["rows"] + result["append"]["rows"]
        assert (
            result["after"]["amount_cents"]
            == result["before"]["amount_cents"] + result["append"]["amount_cents"]
        )
        assert result["old_snapshot_read_again"]["complete"] is True
        assert result["during_compaction_query"]["complete"] is True

        conflicts = [
            attempt
            for attempt in result["compaction"]["attempts"]
            if attempt["status"] == "conflict"
        ]
        assert conflicts and conflicts[0]["base_version"] != conflicts[0]["conflict_current_version"]


def test_dual_compaction_uses_optimistic_conflict(tmp_path):
    with make_client(tmp_path, seed_files=32, max_files=12) as client:
        before = client.get("/api/snapshot").json()
        response = client.post(
            "/api/demo/dual-compaction",
            json={"max_files": 12, "publish_delay_ms": 700},
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["conflict_attempts"], result
        assert all(result["checks"].values()), result["checks"]
        assert result["after"]["rows"] == before["query"]["rows"]
        assert result["after"]["amount_cents"] == before["query"]["total_amount_cents"]
        assert result["old_snapshot_read_again"]["complete"] is True
        # The winning task's output cannot be silently overwritten by the loser.
        published_versions = [
            task["published_version"]
            for task in result["tasks"]
            if task["published_version"] is not None
        ]
        assert published_versions
        assert len(set(published_versions)) == len(published_versions)


def test_failure_before_publish_changes_nothing(tmp_path):
    with make_client(tmp_path, seed_files=20, max_files=8) as client:
        before_state = client.get("/api/state").json()
        before = client.get("/api/snapshot").json()
        response = client.post(
            "/api/demo/failure-before-publish",
            json={"max_files": 8, "publish_delay_ms": 200},
        )
        assert response.status_code == 200
        result = response.json()
        task = result["compaction"]
        assert task["status"] == "failed"
        assert task["published_version"] is None
        assert all(result["checks"].values()), result["checks"]
        after_state = client.get("/api/state").json()
        assert after_state["current_version"] == before_state["current_version"]
        assert after_state["staging_files"] == []
        assert after_state["distribution"]["orphan"] == 0
        assert result["after"]["rows"] == before["query"]["rows"]


def test_pin_retention_and_unpin_gc(tmp_path):
    with make_client(tmp_path, seed_files=32, max_files=16) as client:
        response = client.post("/api/demo/pin-and-gc", json={"max_files": 64})
        assert response.status_code == 200, response.text
        result = response.json()
        assert all(result["checks"].values()), result["checks"]
        assert result["old_query_status"] == 410
        assert result["reclaimed_exclusive_files"]
        assert result["cleanup_with_pin"]["protected_versions"]
        assert any(
            item["reason"] == "页面固定" for item in result["cleanup_with_pin"]["protected_versions"]
        )
        # A retained pinned snapshot remains complete after the first cleanup.
        assert result["pinned_query_after_cleanup"]["complete"] is True
        # After unpinning, its exclusive files are gone, but current data is exact.
        for path in result["reclaimed_exclusive_files"]:
            assert not (tmp_path / "data" / path).exists()
        assert result["pinned_version_after_unpin"]["pruned"] is True
        final = client.get("/api/snapshot").json()
        assert final["complete"] is True


def test_temp_output_is_invisible_while_compaction_waits(tmp_path):
    with make_client(tmp_path, seed_files=24, max_files=10) as client:
        before = client.get("/api/snapshot").json()
        task = client.post(
            "/api/compactions",
            json={"max_files": 10, "publish_delay_ms": 1000},
        ).json()
        deadline = time.monotonic() + 8
        state = {}
        while time.monotonic() < deadline:
            state = client.get("/api/state").json()
            if state["staging_files"]:
                break
            time.sleep(0.04)
        assert state["staging_files"], "expected an isolated staging parquet"
        during = client.get("/api/snapshot").json()
        assert during["snapshot_version"] == before["snapshot_version"]
        assert during["complete"] is True
        staging_names = {file["name"] for file in state["staging_files"]}
        assert all(file["path"].split("/")[-1] not in staging_names for file in during["files"])
        finished = poll_task(client, task["id"])
        assert finished["status"] == "completed"
        after = client.get("/api/snapshot").json()
        assert after["query"]["rows"] == before["query"]["rows"]
        assert after["query"]["total_amount_cents"] == before["query"]["total_amount_cents"]
        assert finished["attempts"][0]["validation"]["difference_rows"] == 0


def test_restart_recovery_marks_task_interrupted_and_drops_staging(tmp_path):
    settings = Settings(
        data_root=tmp_path,
        seed_files=8,
        seed_rows=5,
        auto_seed=True,
        max_compact_files=4,
    )
    catalog = Catalog(settings)
    catalog.initialize()
    staging = settings.staging_dir / "fake-interrupted.parquet"
    write_parquet(staging, generate_events(9999, 2, "fake"))
    now = "2026-01-01T00:00:00.000+00:00"
    catalog.save_task(
        {
            "id": uuid.uuid4().hex,
            "kind": "compaction",
            "partition": settings.partition,
            "status": "running",
            "created_at": now,
            "updated_at": now,
            "request": {},
            "attempts": [],
            "events": [],
            "published_version": None,
            "error": None,
        }
    )
    catalog.initialize()
    task = catalog.list_tasks(settings.partition)[0]
    assert task["status"] == "interrupted"
    assert not staging.exists()
    snapshot = catalog.query_snapshot(settings.partition)
    assert snapshot["complete"] is True
    assert snapshot["query"]["rows"] == settings.seed_files * settings.seed_rows
    manager = TaskManager(catalog, max_workers=1)
    manager.shutdown()


def test_orphan_parquet_is_not_queried_and_is_gc_candidate(tmp_path):
    with make_client(tmp_path, seed_files=12, max_files=6) as client:
        orphan_rel = "orders/manual-orphan.parquet"
        orphan_path = tmp_path / "data" / orphan_rel
        write_parquet(orphan_path, generate_events(90_000, 3, "orphan"))
        snapshot = client.get("/api/snapshot").json()
        state = client.get("/api/state").json()
        assert snapshot["complete"] is True
        assert all(file["path"] != orphan_rel for file in snapshot["files"])
        assert any(file["path"] == orphan_rel and file["orphan"] for file in state["files"])
        cleanup = client.post("/api/cleanup", json={"keep": 3}).json()
        assert any(file["path"] == orphan_rel for file in cleanup["deleted_files"])
