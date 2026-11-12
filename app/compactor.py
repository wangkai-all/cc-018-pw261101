from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Sequence

from .catalog import Catalog
from .errors import InjectFailure, VersionConflict
from .storage import (
    FileStats,
    aggregate_parquet,
    compact_parquet,
    sha256_file,
    utc_now,
    validate_compaction,
    validate_single_file,
)


class TaskManager:
    """Runs compaction attempts and keeps an auditable, persisted task timeline."""

    def __init__(self, catalog: Catalog, max_workers: int = 4):
        self.catalog = catalog
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="compactor"
        )
        self._lock = threading.RLock()
        self._running: dict[str, dict[str, Any]] = {}
        self._max_attempts = 5

    # ------------------------------------------------------------------ public
    def start_compaction(
        self,
        partition: str,
        *,
        max_files: int | None = None,
        publish_delay_ms: int = 0,
        simulate_failure: bool = False,
        label: str | None = None,
    ) -> dict[str, Any]:
        task_id = uuid.uuid4().hex
        now = utc_now()
        task = {
            "id": task_id,
            "kind": "compaction",
            "partition": partition,
            "status": "running",
            "created_at": now,
            "updated_at": now,
            "request": {
                "max_files": max_files or self.catalog.settings.max_compact_files,
                "publish_delay_ms": max(0, min(int(publish_delay_ms), 15_000)),
                "simulate_failure": bool(simulate_failure),
                "label": label,
            },
            "attempts": [],
            "events": [
                {
                    "at": now,
                    "level": "info",
                    "message": "合并任务已创建，等待规划",
                }
            ],
            "published_version": None,
            "error": None,
        }
        with self._lock:
            self._running[task_id] = task
            self.catalog.save_task(task)
        self._executor.submit(self._run, task_id)
        return task

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            if task_id in self._running:
                return self._copy(self._running[task_id])
        return self.catalog.get_task(task_id)

    def list(self, partition: str, limit: int = 30) -> list[dict[str, Any]]:
        tasks = self.catalog.list_tasks(partition, limit=limit)
        with self._lock:
            for task_id, task in self._running.items():
                if task["partition"] != partition:
                    continue
                found = next((item for item in tasks if item["id"] == task_id), None)
                if found is None:
                    tasks.insert(0, self._copy(task))
                else:
                    tasks[tasks.index(found)] = self._copy(task)
        return tasks

    def running_base_versions(self, partition: str) -> set[int]:
        versions: set[int] = set()
        with self._lock:
            for task in self._running.values():
                if task["partition"] != partition or task["status"] != "running":
                    continue
                for attempt in task.get("attempts", []):
                    if attempt.get("base_version") is not None:
                        versions.add(int(attempt["base_version"]))
        return versions

    def wait(self, task_id: str, timeout_s: float = 30.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            task = self.get(task_id)
            if task and task["status"] not in {"running"}:
                return task
            time.sleep(0.05)
        task = self.get(task_id)
        if task is None:
            raise KeyError(task_id)
        return task

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    # ----------------------------------------------------------------- helpers
    def _copy(self, value: dict[str, Any]) -> dict[str, Any]:
        import copy

        return copy.deepcopy(value)

    def _update(self, task_id: str, mutate) -> dict[str, Any]:
        with self._lock:
            task = self._running[task_id]
            mutate(task)
            task["updated_at"] = utc_now()
            snapshot = self._copy(task)
            self.catalog.save_task(snapshot)
            return snapshot

    def _event(
        self,
        task_id: str,
        message: str,
        level: str = "info",
        detail: Any | None = None,
    ) -> None:
        def mutate(task: dict[str, Any]) -> None:
            event = {"at": utc_now(), "level": level, "message": message}
            if detail is not None:
                event["detail"] = detail
            task["events"].append(event)

        self._update(task_id, mutate)

    def _finish(
        self,
        task_id: str,
        status: str,
        *,
        error: str | None = None,
        published_version: int | None = None,
    ) -> None:
        def mutate(task: dict[str, Any]) -> None:
            task["status"] = status
            task["error"] = error
            task["published_version"] = published_version

        self._update(task_id, mutate)

    def _select_inputs(
        self, files: Sequence[dict[str, Any]], max_files: int
    ) -> list[dict[str, Any]]:
        eligible = [file for file in files if file.get("kind") != "compacted"]
        eligible.sort(key=lambda file: (int(file["bytes"]), file["created_at"], file["path"]))
        return eligible[:max_files]

    def _verify_inputs(
        self, selected: Sequence[dict[str, Any]]
    ) -> tuple[list[Path], dict[str, Any]]:
        paths = [self.catalog._abs_data_path(file["path"]) for file in selected]
        actual_rows = 0
        actual_amount = 0
        checked = []
        for file, path in zip(selected, paths):
            if not path.exists():
                raise FileNotFoundError(f"选中的输入文件不存在: {file['path']}")
            if sha256_file(path) != file["sha256"]:
                raise ValueError(f"输入文件校验和不匹配: {file['path']}")
            stats = FileStats.from_scan(path)
            if stats.rows != int(file["rows"]):
                raise ValueError(f"输入文件行数与 manifest 不符: {file['path']}")
            if stats.amount_cents != int(file["amount_cents"]):
                raise ValueError(f"输入文件金额与 manifest 不符: {file['path']}")
            if stats.distinct_event_ids != stats.rows:
                raise ValueError(f"输入文件 event_id 重复: {file['path']}")
            actual_rows += stats.rows
            actual_amount += stats.amount_cents
            checked.append(
                {
                    "path": file["path"],
                    "rows": stats.rows,
                    "amount_cents": stats.amount_cents,
                    "sha256": stats.sha256,
                    "row_groups": stats.row_groups,
                }
            )
        return paths, {
            "files": checked,
            "rows": actual_rows,
            "amount_cents": actual_amount,
            "valid": True,
            "checked_at": utc_now(),
        }

    # -------------------------------------------------------------------- run
    def _run(self, task_id: str) -> None:
        task = self.get(task_id)
        if task is None:
            return
        partition = task["partition"]
        request = task["request"]
        attempt_no = 0

        try:
            while attempt_no < self._max_attempts:
                attempt_no += 1
                base_version = self.catalog.current_version(partition)
                if base_version is None:
                    raise ValueError("分区尚无版本")
                manifest = self.catalog.load_manifest(base_version)
                selected = self._select_inputs(manifest["files"], int(request["max_files"]))
                if len(selected) < self.catalog.settings.min_compact_files:
                    self._event(
                        task_id,
                        "没有足够的零散文件，本轮无需合并，数据保持原样",
                        "warn",
                        {
                            "base_version": base_version,
                            "eligible_files": len(selected),
                            "minimum": self.catalog.settings.min_compact_files,
                        },
                    )
                    self._finish(task_id, "completed_noop")
                    return

                attempt: dict[str, Any] = {
                    "attempt": attempt_no,
                    "base_version": base_version,
                    "status": "planned",
                    "selected_files": [
                        {
                            "path": file["path"],
                            "kind": file["kind"],
                            "rows": file["rows"],
                            "amount_cents": file["amount_cents"],
                            "bytes": file["bytes"],
                            "sha256": file["sha256"],
                        }
                        for file in selected
                    ],
                    "input_rows": sum(int(file["rows"]) for file in selected),
                    "input_amount_cents": sum(int(file["amount_cents"]) for file in selected),
                    "input_file_count": len(selected),
                    "output_path": None,
                    "output_rows": None,
                    "output_amount_cents": None,
                    "validation": None,
                    "conflict_current_version": None,
                    "error": None,
                    "started_at": utc_now(),
                }

                def add_attempt(task: dict[str, Any], attempt=attempt) -> None:
                    task["attempts"].append(attempt)
                    task["events"].append(
                        {
                            "at": utc_now(),
                            "level": "info",
                            "message": (
                                f"第 {attempt_no} 次规划：基于 v{base_version}，"
                                f"选中 {len(selected)} 个小文件"
                            ),
                            "detail": {
                                "input_file_count": len(selected),
                                "input_rows": attempt["input_rows"],
                                "base_version": base_version,
                            },
                        }
                    )

                self._update(task_id, add_attempt)
                stage = self.catalog.settings.staging_dir / (
                    f"compact-{task_id[:10]}-{attempt_no}-{uuid.uuid4().hex[:8]}.parquet"
                )
                final_rel = f"{partition}/compact-{uuid.uuid4().hex[:12]}.parquet"

                try:
                    input_paths, input_validation = self._verify_inputs(selected)
                    try:
                        compact_parquet(input_paths, stage)
                    except Exception as exc:
                        raise RuntimeError(f"DuckDB 合并写出失败: {exc}") from exc

                    output_stats = validate_single_file(stage)
                    output_actual = aggregate_parquet([stage])
                    validation = validate_compaction(
                        input_paths,
                        stage,
                        attempt["input_rows"],
                        attempt["input_amount_cents"],
                    )
                    validation["inputs"] = input_validation
                    validation["output_file"] = {
                        "rows": output_stats["rows"],
                        "amount_cents": output_stats["amount_cents"],
                        "sha256": output_stats["sha256"],
                        "bytes": output_stats["bytes"],
                        "row_groups": output_stats["row_groups"],
                        "distinct_event_ids": output_stats["distinct_event_ids"],
                    }

                    output_meta = {
                        "path": final_rel,
                        "kind": "compacted",
                        "source": "compaction",
                        "rows": output_stats["rows"],
                        "amount_cents": output_stats["amount_cents"],
                        "bytes": output_stats["bytes"],
                        "sha256": output_stats["sha256"],
                        "min_event_id": output_stats["min_event_id"],
                        "max_event_id": output_stats["max_event_id"],
                        "row_groups": output_stats["row_groups"],
                        "created_at": utc_now(),
                    }

                    def mark_validated(task: dict[str, Any]) -> None:
                        attempt["status"] = "validated"
                        attempt["output_path"] = final_rel
                        attempt["output_rows"] = output_stats["rows"]
                        attempt["output_amount_cents"] = output_stats["amount_cents"]
                        attempt["validation"] = validation
                        task["events"].append(
                            {
                                "at": utc_now(),
                                "level": "success",
                                "message": (
                                    f"第 {attempt_no} 次输出已通过完整校验："
                                    f"{attempt['input_rows']} 行 / "
                                    f"{attempt['input_amount_cents']} 分 -> "
                                    f"{output_stats['rows']} 行 / "
                                    f"{output_stats['amount_cents']} 分"
                                ),
                                "detail": {
                                    "difference_rows": validation["difference_rows"],
                                    "method": validation["method"],
                                    "output_sha256": output_stats["sha256"],
                                },
                            }
                        )

                    self._update(task_id, mark_validated)

                    delay_ms = int(request["publish_delay_ms"])
                    if delay_ms:
                        def waiting(task: dict[str, Any]) -> None:
                            attempt["status"] = "validated_waiting"
                            task["events"].append(
                                {
                                    "at": utc_now(),
                                    "level": "info",
                                    "message": f"输出已校验，等待 {delay_ms}ms 后尝试乐观发布",
                                }
                            )

                        self._update(task_id, waiting)
                        time.sleep(delay_ms / 1000.0)

                    if request.get("simulate_failure"):
                        raise InjectFailure(
                            "按演示要求在发布前注入失败；临时输出将删除，当前版本不变"
                        )

                    try:
                        published = self.catalog.publish_compaction(
                            partition=partition,
                            base_version=base_version,
                            selected_files=selected,
                            output_meta=output_meta,
                            output_staging=stage,
                            validation=validation,
                        )
                    except VersionConflict as conflict:
                        def mark_conflict(task: dict[str, Any]) -> None:
                            attempt["status"] = "conflict"
                            attempt["conflict_current_version"] = conflict.current
                            task["events"].append(
                                {
                                    "at": utc_now(),
                                    "level": "warn",
                                    "message": (
                                        f"第 {attempt_no} 次发布检测到版本冲突："
                                        f"基于 v{base_version}，当前已是 v{conflict.current}；"
                                        "丢弃本轮输出并重新规划"
                                    ),
                                    "detail": {
                                        "expected_version": base_version,
                                        "current_version": conflict.current,
                                        "output_discarded": True,
                                    },
                                }
                            )

                        self._update(task_id, mark_conflict)
                        if stage.exists():
                            stage.unlink(missing_ok=True)
                        continue
                    else:
                        def mark_published(task: dict[str, Any]) -> None:
                            attempt["status"] = "published"
                            task["published_version"] = published["version_id"]
                            task["events"].append(
                                {
                                    "at": utc_now(),
                                    "level": "success",
                                    "message": (
                                        f"乐观发布成功：v{published['version_id']}，"
                                        f"{published['file_count']} 个文件 / "
                                        f"{published['rows']} 行"
                                    ),
                                    "detail": {
                                        "version_id": published["version_id"],
                                        "manifest_path": published["manifest_path"],
                                    },
                                }
                            )

                        self._update(task_id, mark_published)
                        self._finish(
                            task_id,
                            "completed",
                            published_version=published["version_id"],
                        )
                        return
                except InjectFailure as exc:
                    self._event(task_id, str(exc), "error")
                    self._finish(task_id, "failed", error=str(exc))
                    return
                except Exception as exc:
                    def mark_error(task: dict[str, Any]) -> None:
                        attempt["status"] = "error"
                        attempt["error"] = str(exc)
                        task["events"].append(
                            {
                                "at": utc_now(),
                                "level": "error",
                                "message": f"第 {attempt_no} 次合并失败：{exc}",
                            }
                        )

                    self._update(task_id, mark_error)
                    raise
                finally:
                    if stage.exists():
                        stage.unlink(missing_ok=True)
            self._finish(
                task_id,
                "failed",
                error=f"连续 {self._max_attempts} 次版本冲突，已放弃本轮合并",
            )
        except Exception as exc:
            self._event(task_id, f"任务失败：{exc}", "error")
            self._finish(task_id, "failed", error=str(exc))
        finally:
            with self._lock:
                self._running.pop(task_id, None)
