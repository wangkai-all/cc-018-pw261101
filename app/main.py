from __future__ import annotations

import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .catalog import Catalog
from .compactor import TaskManager
from .config import BASE_DIR, Settings
from .errors import CatalogError, PartitionNotFound, VersionNotFound, VersionPruned


class AppendRequest(BaseModel):
    partition: str | None = None
    rows: int = Field(default=24, ge=1, le=10_000)
    batch_id: str | None = None


class CompactionRequest(BaseModel):
    partition: str | None = None
    max_files: int | None = Field(default=None, ge=2, le=64)
    publish_delay_ms: int = Field(default=0, ge=0, le=15_000)
    simulate_failure: bool = False


class PinRequest(BaseModel):
    partition: str | None = None
    note: str = ""


class CleanupRequest(BaseModel):
    partition: str | None = None
    keep: int | None = Field(default=None, ge=1, le=20)


def _summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot_version": snapshot["snapshot_version"],
        "current_version": snapshot["current_version"],
        "rows": snapshot["query"]["rows"],
        "amount_cents": snapshot["query"]["total_amount_cents"],
        "distinct_event_ids": snapshot["query"]["distinct_event_ids"],
        "file_count": len(snapshot["files"]),
        "manifest_aggregates": snapshot["manifest_aggregates"],
        "complete": snapshot["complete"],
        "checks": snapshot["checks"],
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    catalog = Catalog(settings)
    tasks = TaskManager(catalog, max_workers=settings.max_workers)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        catalog.initialize()
        yield
        tasks.shutdown()

    app = FastAPI(
        title="Parquet Snapshot Compactor",
        version="1.0.0",
        description="Immutable parquet files + validated snapshot manifests",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.catalog = catalog
    app.state.tasks = tasks

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(CatalogError)
    async def catalog_error_handler(request: Request, exc: CatalogError):
        if isinstance(exc, VersionPruned):
            status = 410
        elif isinstance(exc, VersionNotFound):
            status = 404
        elif isinstance(exc, PartitionNotFound):
            status = 404
        else:
            status = 409
        return JSONResponse(
            status_code=status,
            content={"error": exc.__class__.__name__, "message": str(exc)},
        )

    @app.exception_handler(FileNotFoundError)
    async def file_error_handler(request: Request, exc: FileNotFoundError):
        return JSONResponse(
            status_code=410,
            content={"error": "FileMissing", "message": str(exc)},
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        return JSONResponse(
            status_code=400,
            content={"error": "InvalidRequest", "message": str(exc)},
        )

    def state_payload(partition: str) -> dict[str, Any]:
        state = catalog.partition_state(partition)
        state["tasks"] = tasks.list(partition)
        state["config"] = {
            "data_root": str(settings.data_root),
            "seed_files": settings.seed_files,
            "seed_rows": settings.seed_rows,
            "max_compact_files": settings.max_compact_files,
            "min_compact_files": settings.min_compact_files,
            "keep_versions": settings.keep_versions,
        }
        return state

    # -------------------------------------------------------------- read APIs
    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "data_root": str(settings.data_root),
            "partition": settings.partition,
            "current_version": catalog.current_version(settings.partition),
        }

    @app.get("/api/state")
    def get_state(partition: str = Query(default=None)) -> dict[str, Any]:
        return state_payload(partition or settings.partition)

    @app.get("/api/versions")
    def get_versions(partition: str = Query(default=None)) -> dict[str, Any]:
        partition = partition or settings.partition
        return {
            "partition": partition,
            "versions": catalog.list_versions(partition),
            "pins": catalog.list_pins(partition),
            "current_version": catalog.current_version(partition),
        }

    @app.get("/api/versions/{version_id}")
    def get_version(version_id: int, partition: str = Query(default=None)) -> dict[str, Any]:
        return catalog.version_detail(partition or settings.partition, version_id)

    @app.post("/api/versions/{version_id}/verify")
    def verify_version(version_id: int, partition: str = Query(default=None)) -> dict[str, Any]:
        return catalog.verify_version(partition or settings.partition, version_id)

    @app.post("/api/versions/{version_id}/pin")
    def pin_version(
        version_id: int,
        payload: PinRequest | None = Body(default=None),
    ) -> dict[str, Any]:
        payload = payload or PinRequest()
        return catalog.pin_version(
            payload.partition or settings.partition, version_id, payload.note
        )

    @app.post("/api/versions/{version_id}/unpin")
    def unpin_version(
        version_id: int,
        partition: str = Query(default=None),
    ) -> dict[str, Any]:
        return catalog.unpin(partition or settings.partition, version_id=version_id)

    @app.get("/api/snapshot")
    def snapshot(
        partition: str = Query(default=None),
        version: int | None = Query(default=None),
    ) -> dict[str, Any]:
        return catalog.query_snapshot(partition or settings.partition, version)

    @app.get("/api/compare")
    def compare(
        partition: str = Query(default=None),
        version: int | None = Query(default=None),
    ) -> dict[str, Any]:
        partition = partition or settings.partition
        current = catalog.query_snapshot(partition)
        if version is None or version == current["snapshot_version"]:
            old = current
        else:
            old = catalog.query_snapshot(partition, version)
        return {
            "partition": partition,
            "old": old,
            "current": current,
            "same_snapshot": old["snapshot_version"] == current["snapshot_version"],
            "row_delta": current["query"]["rows"] - old["query"]["rows"],
            "amount_delta_cents": (
                current["query"]["total_amount_cents"]
                - old["query"]["total_amount_cents"]
            ),
            "both_complete": bool(old["complete"] and current["complete"]),
        }

    @app.get("/api/tasks")
    def list_tasks(partition: str = Query(default=None)) -> dict[str, Any]:
        partition = partition or settings.partition
        return {"partition": partition, "tasks": tasks.list(partition)}

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        return task

    # ------------------------------------------------------------- write APIs
    @app.post("/api/append")
    def append(payload: AppendRequest) -> dict[str, Any]:
        partition = payload.partition or settings.partition
        result = catalog.append(partition, rows=payload.rows, batch_id=payload.batch_id)
        result["state"] = state_payload(partition)
        return result

    @app.post("/api/compactions")
    def start_compaction(payload: CompactionRequest) -> dict[str, Any]:
        task = tasks.start_compaction(
            payload.partition or settings.partition,
            max_files=payload.max_files,
            publish_delay_ms=payload.publish_delay_ms,
            simulate_failure=payload.simulate_failure,
        )
        return task

    @app.post("/api/cleanup")
    def cleanup(payload: CleanupRequest) -> dict[str, Any]:
        partition = payload.partition or settings.partition
        result = catalog.cleanup(
            partition,
            keep=payload.keep or settings.keep_versions,
            extra_protected_versions=tasks.running_base_versions(partition),
        )
        result["state"] = state_payload(partition)
        return result

    # -------------------------------------------------------------- demo APIs
    def wait_for_plan(task_id: str, timeout_s: float = 8.0) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            last = tasks.get(task_id)
            if last is None:
                return None
            if last.get("attempts") or last["status"] not in {"running"}:
                return last
            time.sleep(0.03)
        return last

    def ensure_compactable(partition: str, minimum: int | None = None) -> list[dict[str, Any]]:
        """Make a scenario deterministic when earlier demos consumed all small files."""
        minimum = minimum or settings.min_compact_files
        created = []
        current = catalog.current_version(partition)
        manifest = catalog.load_manifest(current) if current is not None else {"files": []}
        while sum(1 for file in manifest["files"] if file.get("kind") != "compacted") < minimum:
            result = catalog.append(partition, rows=8, batch_id="scenario-setup")
            created.append(
                {
                    "version_id": result["version"]["version_id"],
                    "file": result["file"]["path"],
                    "rows": result["file"]["rows"],
                    "amount_cents": result["file"]["amount_cents"],
                }
            )
            current = catalog.current_version(partition)
            manifest = catalog.load_manifest(current)
        return created

    @app.post("/api/demo/append-during-compaction")
    def demo_append_during_compaction(payload: CompactionRequest) -> dict[str, Any]:
        partition = payload.partition or settings.partition
        delay = payload.publish_delay_ms or 1200
        before = catalog.query_snapshot(partition)
        task = tasks.start_compaction(
            partition,
            max_files=payload.max_files,
            publish_delay_ms=delay,
            simulate_failure=False,
            label="append-during-compaction",
        )
        planned = wait_for_plan(task["id"])
        if planned is None or planned["status"] in {"completed_noop", "failed"}:
            finished = tasks.wait(task["id"])
            return {
                "scenario": "append-during-compaction",
                "skipped": True,
                "reason": "没有可供合并的文件，请先追加数据或重置演示数据",
                "compaction": finished,
                "before": _summary(before),
                "after": _summary(catalog.query_snapshot(partition)),
            }
        append_result = catalog.append(partition, rows=24)
        during = catalog.query_snapshot(partition, before["snapshot_version"])
        finished = tasks.wait(task["id"], timeout_s=30.0)
        after = catalog.query_snapshot(partition)
        old_again = catalog.query_snapshot(partition, before["snapshot_version"])
        conflicts = [
            attempt
            for attempt in finished["attempts"]
            if attempt["status"] == "conflict"
        ]
        expected_rows = before["query"]["rows"] + append_result["file"]["rows"]
        expected_amount = (
            before["query"]["total_amount_cents"]
            + append_result["file"]["amount_cents"]
        )
        return {
            "scenario": "append-during-compaction",
            "skipped": False,
            "before": _summary(before),
            "append": {
                "version_id": append_result["version"]["version_id"],
                "file": append_result["file"],
                "rows": append_result["file"]["rows"],
                "amount_cents": append_result["file"]["amount_cents"],
            },
            "during_compaction_query": _summary(during),
            "compaction": finished,
            "after": _summary(after),
            "old_snapshot_read_again": _summary(old_again),
            "conflict_count": len(conflicts),
            "checks": {
                "append_did_not_get_lost": after["query"]["rows"] == expected_rows,
                "amount_exact": after["query"]["total_amount_cents"] == expected_amount,
                "old_snapshot_unchanged": (
                    old_again["query"]["rows"] == before["query"]["rows"]
                    and old_again["query"]["total_amount_cents"]
                    == before["query"]["total_amount_cents"]
                ),
                "old_snapshot_still_complete": bool(old_again["complete"]),
                "current_snapshot_complete": bool(after["complete"]),
            },
        }

    @app.post("/api/demo/dual-compaction")
    def demo_dual_compaction(payload: CompactionRequest) -> dict[str, Any]:
        partition = payload.partition or settings.partition
        delay = payload.publish_delay_ms or 900
        before = catalog.query_snapshot(partition)
        first = tasks.start_compaction(
            partition,
            max_files=payload.max_files,
            publish_delay_ms=delay,
            label="dual-round-A",
        )
        second = tasks.start_compaction(
            partition,
            max_files=payload.max_files,
            publish_delay_ms=delay,
            label="dual-round-B",
        )
        finished_a = tasks.wait(first["id"], timeout_s=30.0)
        finished_b = tasks.wait(second["id"], timeout_s=30.0)
        after = catalog.query_snapshot(partition)
        old = catalog.query_snapshot(partition, before["snapshot_version"])
        conflict_attempts = [
            {
                "task_id": item["id"],
                "attempt": attempt["attempt"],
                "base_version": attempt["base_version"],
                "current_version": attempt["conflict_current_version"],
            }
            for item in (finished_a, finished_b)
            for attempt in item["attempts"]
            if attempt["status"] == "conflict"
        ]
        return {
            "scenario": "dual-compaction",
            "before": _summary(before),
            "tasks": [finished_a, finished_b],
            "conflict_attempts": conflict_attempts,
            "after": _summary(after),
            "old_snapshot_read_again": _summary(old),
            "checks": {
                "no_duplicates": after["query"]["distinct_event_ids"]
                == after["query"]["rows"],
                "rows_unchanged": after["query"]["rows"] == before["query"]["rows"],
                "amount_unchanged": after["query"]["total_amount_cents"]
                == before["query"]["total_amount_cents"],
                "current_complete": bool(after["complete"]),
                "old_complete": bool(old["complete"]),
            },
        }

    @app.post("/api/demo/failure-before-publish")
    def demo_failure(payload: CompactionRequest) -> dict[str, Any]:
        partition = payload.partition or settings.partition
        setup_appends = ensure_compactable(partition)
        before = catalog.query_snapshot(partition)
        task = tasks.start_compaction(
            partition,
            max_files=payload.max_files,
            publish_delay_ms=payload.publish_delay_ms,
            simulate_failure=True,
            label="failure-before-publish",
        )
        finished = tasks.wait(task["id"], timeout_s=30.0)
        after = catalog.query_snapshot(partition)
        return {
            "scenario": "failure-before-publish",
            "before": _summary(before),
            "compaction": finished,
            "setup_appends": setup_appends,
            "after": _summary(after),
            "published_version": finished.get("published_version"),
            "checks": {
                "no_version_published": finished.get("published_version") is None,
                "current_version_unchanged": (
                    after["snapshot_version"] == before["snapshot_version"]
                ),
                "rows_unchanged": after["query"]["rows"] == before["query"]["rows"],
                "amount_unchanged": (
                    after["query"]["total_amount_cents"]
                    == before["query"]["total_amount_cents"]
                ),
                "current_complete": bool(after["complete"]),
            },
        }

    @app.post("/api/demo/repeat-compaction")
    def demo_repeat_compaction(payload: CompactionRequest) -> dict[str, Any]:
        partition = payload.partition or settings.partition
        before = catalog.query_snapshot(partition)
        a = tasks.start_compaction(partition, max_files=payload.max_files, label="repeat-A")
        b = tasks.start_compaction(partition, max_files=payload.max_files, label="repeat-B")
        finished_a = tasks.wait(a["id"], timeout_s=30.0)
        finished_b = tasks.wait(b["id"], timeout_s=30.0)
        after = catalog.query_snapshot(partition)
        return {
            "scenario": "repeat-compaction",
            "before": _summary(before),
            "tasks": [finished_a, finished_b],
            "after": _summary(after),
            "checks": {
                "rows_unchanged": after["query"]["rows"] == before["query"]["rows"],
                "amount_unchanged": (
                    after["query"]["total_amount_cents"]
                    == before["query"]["total_amount_cents"]
                ),
                "no_duplicates": after["query"]["distinct_event_ids"]
                == after["query"]["rows"],
                "current_complete": bool(after["complete"]),
            },
        }

    @app.post("/api/demo/pin-and-gc")
    def demo_pin_and_gc(payload: CompactionRequest) -> dict[str, Any]:
        """Pin an old version, prove retention + readability, then unpin and reclaim."""
        partition = payload.partition or settings.partition
        before = catalog.query_snapshot(partition)
        state = catalog.partition_state(partition)

        # Guarantee at least one compaction so the seed files are no longer current.
        if not state["distribution"]["compacted"]:
            task = tasks.start_compaction(
                partition,
                max_files=payload.max_files or 64,
                label="pin-gc-setup-compact",
            )
            tasks.wait(task["id"], timeout_s=30.0)
            state = catalog.partition_state(partition)

        versions = [version for version in catalog.list_versions(partition) if not version["pruned"]]
        current_paths = {
            file["path"] for file in catalog.load_manifest(state["current_version"])["files"]
        }
        candidate = None
        for version in reversed(versions):
            manifest = catalog.load_manifest(version["version_id"])
            exclusive = [file for file in manifest["files"] if file["path"] not in current_paths]
            if exclusive:
                candidate = (version, manifest, exclusive)
                break
        if candidate is None:
            version = versions[-1]
            manifest = catalog.load_manifest(version["version_id"])
            candidate = (version, manifest, list(manifest["files"]))

        pinned_version, pinned_manifest, exclusive_files = candidate
        pin = catalog.pin_version(partition, pinned_version["version_id"], "GC 演示：固定旧快照")
        pinned_before = catalog.query_snapshot(partition, pinned_version["version_id"])
        exclusive_paths = [file["path"] for file in exclusive_files]

        append_versions = []
        for index in range(5):
            result = catalog.append(partition, rows=7, batch_id=f"gc-demo-{index + 1}")
            append_versions.append(
                {
                    "version_id": result["version"]["version_id"],
                    "rows": result["file"]["rows"],
                    "amount_cents": result["file"]["amount_cents"],
                }
            )

        cleanup_with_pin = catalog.cleanup(
            partition,
            keep=3,
            extra_protected_versions=tasks.running_base_versions(partition),
        )
        pinned_after_cleanup = catalog.query_snapshot(partition, pinned_version["version_id"])
        pinned_files_present = [
            path
            for path in exclusive_paths
            if (catalog.settings.data_dir / path).exists()
        ]
        pinned_summary = catalog.version_summary(pinned_version["version_id"])

        catalog.unpin(partition, version_id=pinned_version["version_id"])
        cleanup_after_unpin = catalog.cleanup(
            partition,
            keep=3,
            extra_protected_versions=tasks.running_base_versions(partition),
        )
        deleted_paths = [file["path"] for file in cleanup_after_unpin["deleted_files"]]
        reclaimed_exclusive_files = [path for path in exclusive_paths if path in deleted_paths]
        remaining_exclusive_files = [
            path
            for path in exclusive_paths
            if (catalog.settings.data_dir / path).exists()
        ]
        pinned_after_unpin = catalog.version_summary(pinned_version["version_id"])
        old_query_status = None
        old_query_error = None
        try:
            catalog.query_snapshot(partition, pinned_version["version_id"])
        except VersionPruned as exc:
            old_query_status = 410
            old_query_error = str(exc)
        except Exception as exc:  # pragma: no cover - shown in demo evidence
            old_query_status = 500
            old_query_error = str(exc)
        after = catalog.query_snapshot(partition)
        expected_rows = before["query"]["rows"] + sum(item["rows"] for item in append_versions)
        expected_amount = before["query"]["total_amount_cents"] + sum(
            item["amount_cents"] for item in append_versions
        )
        return {
            "scenario": "pin-and-gc",
            "before": _summary(before),
            "after": _summary(after),
            "pin": pin,
            "pinned_version": pinned_version["version_id"],
            "pinned_version_rows": pinned_manifest["aggregates"]["rows"],
            "exclusive_files": exclusive_paths,
            "appends": append_versions,
            "cleanup_with_pin": cleanup_with_pin,
            "pinned_query_after_cleanup": _summary(pinned_after_cleanup),
            "pinned_version_survived": not pinned_summary["pruned"],
            "pinned_files_present": pinned_files_present,
            "cleanup_after_unpin": cleanup_after_unpin,
            "reclaimed_exclusive_files": reclaimed_exclusive_files,
            "remaining_exclusive_files": remaining_exclusive_files,
            "pinned_version_after_unpin": pinned_after_unpin,
            "old_query_status": old_query_status,
            "old_query_error": old_query_error,
            "checks": {
                "pinned_version_survived_cleanup": not pinned_summary["pruned"],
                "pinned_snapshot_readable_during_retention": bool(
                    pinned_after_cleanup["complete"]
                ),
                "pinned_files_were_protected": len(pinned_files_present) == len(exclusive_paths),
                "pinned_version_pruned_after_unpin": bool(
                    pinned_after_unpin["pruned"]
                ),
                "old_snapshot_rejected_after_unpin": old_query_status == 410,
                "exclusive_files_reclaimed": len(reclaimed_exclusive_files) > 0,
                "rows_exact_after_all_operations": after["query"]["rows"] == expected_rows,
                "amount_exact_after_all_operations": (
                    after["query"]["total_amount_cents"] == expected_amount
                ),
                "current_complete": bool(after["complete"]),
            },
        }

    # --------------------------------------------------------------- UI static
    dist = BASE_DIR / "frontend" / "dist"
    index_file = dist / "index.html"
    if dist.exists() and index_file.exists():
        app.mount("/", StaticFiles(directory=str(dist), html=True), name="ui")
    else:
        @app.get("/")
        def ui_not_built() -> dict[str, Any]:
            return {
                "message": "React 前端尚未构建。运行 `cd frontend && npm install && npm run build`。",
                "api": "/docs",
                "health": "/api/health",
            }

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    current = app.state.settings
    uvicorn.run("app.main:app", host=current.host, port=current.port, reload=False)
