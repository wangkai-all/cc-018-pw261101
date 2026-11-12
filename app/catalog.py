from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Sequence

from .config import Settings
from .errors import PartitionNotFound, VersionConflict, VersionNotFound, VersionPruned
from .storage import (
    FileStats,
    aggregate_parquet,
    generate_events,
    mark_file_immutable,
    per_file_aggregates,
    sha256_file,
    timed_call,
    utc_now,
    validate_compaction,
    validate_single_file,
    write_parquet,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS partitions (
    name TEXT PRIMARY KEY,
    current_version INTEGER,
    next_event_id INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    version_id INTEGER PRIMARY KEY,
    partition TEXT NOT NULL,
    parent_version INTEGER,
    operation TEXT NOT NULL,
    created_at TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    file_count INTEGER NOT NULL,
    rows INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    validated INTEGER NOT NULL DEFAULT 1,
    pruned_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_versions_partition
    ON versions(partition, version_id DESC);

CREATE TABLE IF NOT EXISTS version_files (
    version_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    rows INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    PRIMARY KEY(version_id, path)
);
CREATE INDEX IF NOT EXISTS ix_version_files_path ON version_files(path);

CREATE TABLE IF NOT EXISTS pins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_pin
    ON pins(partition, version_id) WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS leases (
    token TEXT PRIMARY KEY,
    version_id INTEGER NOT NULL,
    partition TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_leases_version ON leases(version_id);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    partition TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks(status);
"""


class Catalog:
    """SQLite catalog + immutable parquet manifests with optimistic publication."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._seed_lock = threading.Lock()

    # ------------------------------------------------------------------ setup
    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(
            self.settings.db_path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_tx(self):
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def initialize(self) -> None:
        self.settings.ensure_dirs()
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.executescript(SCHEMA)

        # A process that died mid-flight cannot own a lease or a temp file.
        with self._write_tx() as conn:
            conn.execute("DELETE FROM leases")
            running = conn.execute(
                "SELECT id, payload FROM tasks WHERE status = 'running'"
            ).fetchall()
            for row in running:
                payload = json.loads(row["payload"])
                payload["status"] = "interrupted"
                payload["updated_at"] = utc_now()
                payload.setdefault("events", []).append(
                    {
                        "at": utc_now(),
                        "level": "warn",
                        "message": "进程退出，恢复时标记为中断；未发布输出已被隔离",
                    }
                )
                conn.execute(
                    "UPDATE tasks SET status = 'interrupted', updated_at = ?, payload = ? WHERE id = ?",
                    (payload["updated_at"], json.dumps(payload, ensure_ascii=False), row["id"]),
                )

        for path in sorted(self.settings.staging_dir.glob("*.parquet")):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        if self.settings.auto_seed:
            self.seed_if_empty()

    # ------------------------------------------------------------- primitives
    def _manifest_rel(self, version_id: int) -> str:
        return f"manifests/v{version_id:08d}.json"

    def _manifest_abs(self, version_id: int) -> Path:
        return self.settings.manifests_dir / f"v{version_id:08d}.json"

    def _abs_data_path(self, relative_path: str) -> Path:
        path = (self.settings.data_dir / relative_path).resolve()
        if self.settings.data_dir.resolve() not in path.parents and path != self.settings.data_dir.resolve():
            raise ValueError(f"非法数据路径: {relative_path}")
        return path

    def _partition_row(self, conn: sqlite3.Connection, partition: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM partitions WHERE name = ?", (partition,)
        ).fetchone()
        if row is None:
            raise PartitionNotFound(f"分区不存在: {partition}")
        return row

    def _ensure_partition(self, conn: sqlite3.Connection, partition: str) -> None:
        now = utc_now()
        conn.execute(
            """
            INSERT INTO partitions(name, current_version, next_event_id, created_at)
            VALUES (?, NULL, 1, ?)
            ON CONFLICT(name) DO NOTHING
            """,
            (partition, now),
        )

    def _file_meta(
        self,
        relative_path: str,
        stats: dict[str, Any],
        *,
        kind: str,
        source: str,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        return {
            "path": relative_path,
            "kind": kind,
            "source": source,
            "rows": int(stats["rows"]),
            "amount_cents": int(stats["amount_cents"]),
            "bytes": int(stats["bytes"]),
            "sha256": stats["sha256"],
            "min_event_id": stats.get("min_event_id"),
            "max_event_id": stats.get("max_event_id"),
            "row_groups": int(stats.get("row_groups", 1)),
            "created_at": created_at or utc_now(),
        }

    def _new_version_id(self, conn: sqlite3.Connection) -> int:
        value = conn.execute("SELECT COALESCE(MAX(version_id), 0) FROM versions").fetchone()[0]
        return int(value) + 1

    def _resolve_pruned(self, conn: sqlite3.Connection, version_id: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM versions WHERE version_id = ?", (version_id,)
        ).fetchone()
        if row is None:
            raise VersionNotFound(f"版本不存在: v{version_id}")
        if row["pruned_at"]:
            raise VersionPruned(
                f"v{version_id} 已清理，manifest 与仅被该版本引用的文件不可再读取"
            )
        return row

    def _write_manifest(self, version_id: int, manifest: dict[str, Any]) -> tuple[str, str]:
        path = self._manifest_abs(version_id)
        temp = path.with_suffix(f".json.tmp-{uuid.uuid4().hex}")
        content = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
        temp.write_text(content, encoding="utf-8")
        digest = sha256_file(temp)
        os.replace(temp, path)
        rel = str(path.relative_to(self.settings.data_root))
        return rel, digest

    def _insert_version(
        self,
        conn: sqlite3.Connection,
        manifest: dict[str, Any],
        manifest_rel: str,
        manifest_sha: str,
    ) -> None:
        aggregates = manifest["aggregates"]
        conn.execute(
            """
            INSERT INTO versions(
                version_id, partition, parent_version, operation, created_at,
                manifest_path, manifest_sha256, file_count, rows, amount_cents, validated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                manifest["version_id"],
                manifest["partition"],
                manifest["parent_version"],
                manifest["operation"],
                manifest["created_at"],
                manifest_rel,
                manifest_sha,
                aggregates["file_count"],
                aggregates["rows"],
                aggregates["amount_cents"],
            ),
        )
        for file in manifest["files"]:
            conn.execute(
                """
                INSERT INTO version_files(
                    version_id, path, kind, rows, amount_cents, bytes, sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest["version_id"],
                    file["path"],
                    file["kind"],
                    file["rows"],
                    file["amount_cents"],
                    file["bytes"],
                    file["sha256"],
                ),
            )
        conn.execute(
            "UPDATE partitions SET current_version = ? WHERE name = ?",
            (manifest["version_id"], manifest["partition"]),
        )

    def _build_manifest(
        self,
        *,
        version_id: int,
        partition: str,
        parent_version: int | None,
        operation: str,
        files: Sequence[dict[str, Any]],
        inputs: Sequence[dict[str, Any]] | None,
        outputs: Sequence[dict[str, Any]] | None,
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        paths = [file["path"] for file in files]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest 中出现重复文件引用")
        rows = sum(int(file["rows"]) for file in files)
        amount = sum(int(file["amount_cents"]) for file in files)
        manifest = {
            "version_id": version_id,
            "partition": partition,
            "parent_version": parent_version,
            "operation": operation,
            "created_at": utc_now(),
            "files": list(files),
            "aggregates": {
                "file_count": len(files),
                "rows": rows,
                "amount_cents": amount,
                "distinct_event_ids": rows,
            },
            "inputs": list(inputs or []),
            "outputs": list(outputs or []),
            "validation": validation,
        }
        if any(int(file["rows"]) <= 0 for file in files):
            raise ValueError("manifest 含空文件")
        manifest["validation"]["manifest_valid"] = True
        return manifest

    def load_manifest(self, version_id: int, *, allow_pruned: bool = False) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM versions WHERE version_id = ?", (version_id,)
            ).fetchone()
        if row is None:
            raise VersionNotFound(f"版本不存在: v{version_id}")
        if row["pruned_at"] and not allow_pruned:
            raise VersionPruned(f"v{version_id} 已清理")
        path = self.settings.data_root / row["manifest_path"]
        if not path.exists():
            raise VersionPruned(f"v{version_id} 的 manifest 文件已清理")
        digest = sha256_file(path)
        if digest != row["manifest_sha256"]:
            raise ValueError(f"v{version_id} manifest 校验和不匹配")
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if int(manifest["version_id"]) != int(version_id):
            raise ValueError("manifest version_id 与文件名不一致")
        return manifest

    def current_version(self, partition: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT current_version FROM partitions WHERE name = ?", (partition,)
            ).fetchone()
            if row is None:
                raise PartitionNotFound(f"分区不存在: {partition}")
            return int(row["current_version"]) if row["current_version"] is not None else None

    # ---------------------------------------------------------------- seeding
    def seed_if_empty(self) -> bool:
        with self._seed_lock:
            partition = self.settings.partition
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT current_version FROM partitions WHERE name = ?", (partition,)
                ).fetchone()
            if existing is not None and existing["current_version"] is not None:
                return False

            with self._write_tx() as conn:
                self._ensure_partition(conn, partition)
                row = self._partition_row(conn, partition)
                if row["current_version"] is not None:
                    return False
                start_id = int(row["next_event_id"])
                conn.execute(
                    "UPDATE partitions SET next_event_id = ? WHERE name = ?",
                    (start_id + self.settings.seed_files * self.settings.seed_rows, partition),
                )

            files: list[dict[str, Any]] = []
            created_at = utc_now()
            (self.settings.data_dir / partition).mkdir(parents=True, exist_ok=True)
            for index in range(self.settings.seed_files):
                batch_id = f"seed-{index + 1:02d}"
                rel = f"{partition}/seed-{index + 1:02d}-{uuid.uuid4().hex[:10]}.parquet"
                final = self._abs_data_path(rel)
                rows = generate_events(
                    start_id + index * self.settings.seed_rows,
                    self.settings.seed_rows,
                    batch_id,
                    seed=1000 + index,
                )
                write_parquet(final, rows)
                stats = validate_single_file(final, expected_rows=self.settings.seed_rows)
                mark_file_immutable(final)
                files.append(
                    self._file_meta(
                        rel, stats, kind="small", source="seed", created_at=created_at
                    )
                )

            with self._write_tx() as conn:
                self._ensure_partition(conn, partition)
                row = self._partition_row(conn, partition)
                if row["current_version"] is not None:
                    # Another concurrent initializer won; leave its catalog untouched.
                    return False
                version_id = self._new_version_id(conn)
                manifest = self._build_manifest(
                    version_id=version_id,
                    partition=partition,
                    parent_version=None,
                    operation="bootstrap",
                    files=files,
                    inputs=None,
                    outputs=None,
                    validation={
                        "valid": True,
                        "method": "PyArrow write + DuckDB per-file validation",
                        "checked_at": utc_now(),
                        "files_checked": len(files),
                    },
                )
                manifest_rel, manifest_sha = self._write_manifest(version_id, manifest)
                self._insert_version(conn, manifest, manifest_rel, manifest_sha)
            return True

    # ----------------------------------------------------------------- append
    def append(self, partition: str, rows: int = 24, batch_id: str | None = None) -> dict[str, Any]:
        if rows < 1 or rows > 10_000:
            raise ValueError("rows 必须在 1..10000 之间")
        batch_id = batch_id or f"append-{uuid.uuid4().hex[:8]}"

        with self._write_tx() as conn:
            self._partition_row(conn, partition)
            start_id = int(
                conn.execute(
                    "SELECT next_event_id FROM partitions WHERE name = ?", (partition,)
                ).fetchone()[0]
            )
            conn.execute(
                "UPDATE partitions SET next_event_id = ? WHERE name = ?",
                (start_id + rows, partition),
            )

        data = generate_events(start_id, rows, batch_id)
        stage_name = f"append-{uuid.uuid4().hex}.parquet"
        stage = self.settings.staging_dir / stage_name
        rel = f"{partition}/append-{uuid.uuid4().hex[:12]}.parquet"
        final = self._abs_data_path(rel)
        moved = False
        try:
            write_parquet(stage, data)
            stats = validate_single_file(stage, expected_rows=rows)
            file_meta = self._file_meta(
                rel, stats, kind="append", source="append"
            )

            with self._write_tx() as conn:
                partition_row = self._partition_row(conn, partition)
                current = partition_row["current_version"]
                if current is None:
                    raise PartitionNotFound(f"分区 {partition} 尚无初始版本")
                current = int(current)
                base_manifest = self.load_manifest(current)
                os.replace(stage, final)
                moved = True
                mark_file_immutable(final)

                files = list(base_manifest["files"]) + [file_meta]
                version_id = self._new_version_id(conn)
                manifest = self._build_manifest(
                    version_id=version_id,
                    partition=partition,
                    parent_version=current,
                    operation="append",
                    files=files,
                    inputs=[file_meta],
                    outputs=[file_meta],
                    validation={
                        "valid": True,
                        "method": "PyArrow write + DuckDB per-file validation",
                        "checked_at": utc_now(),
                        "file": file_meta,
                    },
                )
                manifest_rel, manifest_sha = self._write_manifest(version_id, manifest)
                self._insert_version(conn, manifest, manifest_rel, manifest_sha)
            version_summary = self.version_summary(version_id)
            return {
                "version": version_summary,
                "file": file_meta,
                "event_id_range": [start_id, start_id + rows - 1],
                "batch_id": batch_id,
            }
        except BaseException:
            if moved and final.exists():
                try:
                    final.chmod(0o644)
                    final.unlink()
                except OSError:
                    pass
            raise
        finally:
            if stage.exists():
                stage.unlink(missing_ok=True)

    # ------------------------------------------------------------- compaction
    def publish_compaction(
        self,
        *,
        partition: str,
        base_version: int,
        selected_files: Sequence[dict[str, Any]],
        output_meta: dict[str, Any],
        output_staging: Path,
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        selected_paths = [file["path"] for file in selected_files]
        if len(selected_paths) != len(set(selected_paths)):
            raise ValueError("本轮选中的文件出现重复")
        if not output_staging.exists():
            raise FileNotFoundError("合并临时输出不存在")
        if sha256_file(output_staging) != output_meta["sha256"]:
            raise ValueError("合并临时输出的校验和不匹配")
        final = self._abs_data_path(output_meta["path"])
        if final.exists():
            raise FileExistsError(f"目标文件已存在: {output_meta['path']}")

        moved = False
        try:
            with self._write_tx() as conn:
                row = self._partition_row(conn, partition)
                current = (
                    int(row["current_version"])
                    if row["current_version"] is not None
                    else None
                )
                if current != base_version:
                    raise VersionConflict(base_version, current)

                base_manifest = self.load_manifest(base_version)
                if base_manifest["partition"] != partition:
                    raise ValueError("base manifest 分区不匹配")
                recorded_sha = conn.execute(
                    "SELECT manifest_sha256 FROM versions WHERE version_id = ?",
                    (base_version,),
                ).fetchone()[0]
                if sha256_file(self._manifest_abs(base_version)) != recorded_sha:
                    raise ValueError("base manifest 在规划后发生变化")

                by_path = {file["path"]: file for file in base_manifest["files"]}
                for selected in selected_files:
                    existing = by_path.get(selected["path"])
                    if existing is None:
                        raise VersionConflict(
                            base_version,
                            current,
                            f"选中的文件 {selected['path']} 已不在 base manifest 中",
                        )
                    for field in ("rows", "amount_cents", "bytes"):
                        if int(existing[field]) != int(selected[field]):
                            raise VersionConflict(
                                base_version,
                                current,
                                f"选中的文件 {selected['path']} 元数据变化",
                            )
                    if existing["sha256"] != selected["sha256"]:
                        raise VersionConflict(
                            base_version,
                            current,
                            f"选中的文件 {selected['path']} 校验和变化",
                        )

                expected_rows = sum(int(file["rows"]) for file in selected_files)
                expected_amount = sum(int(file["amount_cents"]) for file in selected_files)
                if int(output_meta["rows"]) != expected_rows:
                    raise ValueError("合并输出行数与替换规则不一致")
                if int(output_meta["amount_cents"]) != expected_amount:
                    raise ValueError("合并输出金额与替换规则不一致")
                if not validation.get("valid") or validation.get("difference_rows") != 0:
                    raise ValueError("合并输出尚未通过完整校验")

                os.replace(output_staging, final)
                moved = True
                mark_file_immutable(final)

                new_files = [
                    file for file in base_manifest["files"] if file["path"] not in selected_paths
                ] + [output_meta]
                version_id = self._new_version_id(conn)
                manifest = self._build_manifest(
                    version_id=version_id,
                    partition=partition,
                    parent_version=base_version,
                    operation="compact",
                    files=new_files,
                    inputs=list(selected_files),
                    outputs=[output_meta],
                    validation=validation,
                )
                manifest_rel, manifest_sha = self._write_manifest(version_id, manifest)
                self._insert_version(conn, manifest, manifest_rel, manifest_sha)
            summary = self.version_summary(version_id)
            return summary
        except BaseException:
            if moved and final.exists():
                try:
                    final.chmod(0o644)
                    final.unlink()
                except OSError:
                    pass
            raise
        finally:
            if output_staging.exists():
                output_staging.unlink(missing_ok=True)

    # -------------------------------------------------------------- query/snap
    def query_snapshot(
        self, partition: str, version: int | None = None
    ) -> dict[str, Any]:
        token = uuid.uuid4().hex
        lease_started = utc_now()
        with self._write_tx() as conn:
            row = self._partition_row(conn, partition)
            resolved = (
                int(version)
                if version is not None
                else (int(row["current_version"]) if row["current_version"] is not None else None)
            )
            if resolved is None:
                raise VersionNotFound("分区尚无版本")
            version_row = self._resolve_pruned(conn, resolved)
            if version_row["partition"] != partition:
                raise VersionNotFound(f"v{resolved} 不属于分区 {partition}")
            current = int(row["current_version"]) if row["current_version"] is not None else None
            conn.execute(
                "INSERT INTO leases(token, version_id, partition, created_at) VALUES (?, ?, ?, ?)",
                (token, resolved, partition, lease_started),
            )
        try:
            manifest = self.load_manifest(resolved)
            files = manifest["files"]
            paths = [self._abs_data_path(file["path"]) for file in files]
            missing = [file["path"] for file, path in zip(files, paths) if not path.exists()]
            if missing:
                raise FileNotFoundError(
                    "快照引用的文件缺失: " + ", ".join(missing[:5])
                )
            result, elapsed_ms = timed_call(aggregate_parquet, paths)
            by_actual = per_file_aggregates(paths)
            file_rows = []
            for file, path in zip(files, paths):
                actual = by_actual.get(str(path)) or by_actual.get(str(path.resolve())) or {}
                file_rows.append(
                    {
                        **file,
                        "actual_rows": actual.get("rows"),
                        "actual_amount_cents": actual.get("amount_cents"),
                        "exists": path.exists(),
                    }
                )
            manifest_rows = manifest["aggregates"]["rows"]
            manifest_amount = manifest["aggregates"]["amount_cents"]
            checks = {
                "files_exist": len(missing) == 0,
                "file_count_matches": len(file_rows) == manifest["aggregates"]["file_count"],
                "row_count_matches": result["rows"] == manifest_rows,
                "amount_matches": result["amount_cents"] == manifest_amount,
                "no_duplicate_event_ids": result["distinct_event_ids"] == result["rows"],
            }
            return {
                "partition": partition,
                "snapshot_version": resolved,
                "current_version": current,
                "is_current": resolved == current,
                "manifest_sha256": self.version_summary(resolved)["manifest_sha256"],
                "manifest_aggregates": manifest["aggregates"],
                "query": {
                    "rows": result["rows"],
                    "total_amount_cents": result["amount_cents"],
                    "distinct_event_ids": result["distinct_event_ids"],
                    "min_event_id": result["min_event_id"],
                    "max_event_id": result["max_event_id"],
                },
                "per_file": file_rows,
                "files": files,
                "checks": checks,
                "complete": all(checks.values()),
                "validated": bool(manifest["validation"].get("valid")),
                "query_ms": round(elapsed_ms, 2),
            }
        finally:
            with self._connect() as conn:
                conn.execute("DELETE FROM leases WHERE token = ?", (token,))

    def verify_version(self, partition: str, version_id: int) -> dict[str, Any]:
        token = uuid.uuid4().hex
        with self._write_tx() as conn:
            row = self._partition_row(conn, partition)
            vrow = self._resolve_pruned(conn, version_id)
            if vrow["partition"] != partition:
                raise VersionNotFound(f"v{version_id} 不属于分区 {partition}")
            conn.execute(
                "INSERT INTO leases(token, version_id, partition, created_at) VALUES (?, ?, ?, ?)",
                (token, version_id, partition, utc_now()),
            )
        try:
            manifest = self.load_manifest(version_id)
            files = manifest["files"]
            checked = []
            problems: list[str] = []
            for file in files:
                path = self._abs_data_path(file["path"])
                exists = path.exists()
                actual = None
                digest = None
                if exists:
                    actual = aggregate_parquet([path])
                    digest = sha256_file(path)
                checks = {
                    "exists": exists,
                    "rows": exists and actual["rows"] == file["rows"],
                    "amount": exists and actual["amount_cents"] == file["amount_cents"],
                    "sha256": exists and digest == file["sha256"],
                    "distinct": exists and actual["distinct_event_ids"] == file["rows"],
                }
                valid = all(checks.values())
                if not valid:
                    problems.append(file["path"])
                checked.append(
                    {
                        "path": file["path"],
                        "expected": {
                            "rows": file["rows"],
                            "amount_cents": file["amount_cents"],
                            "sha256": file["sha256"],
                        },
                        "actual": actual,
                        "sha256": digest,
                        "checks": checks,
                        "valid": valid,
                    }
                )
            total = aggregate_parquet(
                [self._abs_data_path(file["path"]) for file in files]
            )
            aggregate_matches = (
                total["rows"] == manifest["aggregates"]["rows"]
                and total["amount_cents"] == manifest["aggregates"]["amount_cents"]
                and total["distinct_event_ids"] == total["rows"]
            )
            return {
                "version": version_id,
                "partition": partition,
                "valid": not problems and aggregate_matches,
                "problems": problems,
                "manifest_aggregates": manifest["aggregates"],
                "actual_aggregates": total,
                "aggregate_matches": aggregate_matches,
                "files": checked,
                "checked_at": utc_now(),
            }
        finally:
            with self._connect() as conn:
                conn.execute("DELETE FROM leases WHERE token = ?", (token,))

    # --------------------------------------------------------------- inventory
    def version_summary(self, version_id: int) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT v.*,
                       (SELECT count(*) FROM pins p
                         WHERE p.version_id = v.version_id AND p.released_at IS NULL) AS active_pins
                FROM versions v WHERE v.version_id = ?
                """,
                (version_id,),
            ).fetchone()
        if row is None:
            raise VersionNotFound(f"版本不存在: v{version_id}")
        return {
            "version_id": int(row["version_id"]),
            "partition": row["partition"],
            "parent_version": row["parent_version"],
            "operation": row["operation"],
            "created_at": row["created_at"],
            "manifest_path": row["manifest_path"],
            "manifest_sha256": row["manifest_sha256"],
            "file_count": int(row["file_count"]),
            "rows": int(row["rows"]),
            "amount_cents": int(row["amount_cents"]),
            "validated": bool(row["validated"]),
            "pruned": row["pruned_at"] is not None,
            "pruned_at": row["pruned_at"],
            "pinned": int(row["active_pins"]) > 0,
            "active_pins": int(row["active_pins"]),
        }

    def list_versions(self, partition: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT v.*,
                       (SELECT count(*) FROM pins p
                         WHERE p.version_id = v.version_id AND p.released_at IS NULL) AS active_pins
                FROM versions v
                WHERE v.partition = ?
                ORDER BY v.version_id DESC
                """,
                (partition,),
            ).fetchall()
        result = []
        for row in rows:
            result.append(
                {
                    "version_id": int(row["version_id"]),
                    "parent_version": row["parent_version"],
                    "operation": row["operation"],
                    "created_at": row["created_at"],
                    "manifest_path": row["manifest_path"],
                    "manifest_sha256": row["manifest_sha256"],
                    "file_count": int(row["file_count"]),
                    "rows": int(row["rows"]),
                    "amount_cents": int(row["amount_cents"]),
                    "validated": bool(row["validated"]),
                    "pruned": row["pruned_at"] is not None,
                    "pruned_at": row["pruned_at"],
                    "pinned": int(row["active_pins"]) > 0,
                    "active_pins": int(row["active_pins"]),
                }
            )
        return result

    def list_pins(self, partition: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, partition, version_id, note, created_at
                FROM pins
                WHERE partition = ? AND released_at IS NULL
                ORDER BY created_at DESC
                """,
                (partition,),
            ).fetchall()
        return [dict(row) for row in rows]

    def version_detail(self, partition: str, version_id: int) -> dict[str, Any]:
        summary = self.version_summary(version_id)
        if summary["partition"] != partition:
            raise VersionNotFound(f"v{version_id} 不属于分区 {partition}")
        summary["current"] = self.current_version(partition) == version_id
        if summary["pruned"]:
            summary["manifest"] = None
        else:
            summary["manifest"] = self.load_manifest(version_id)
        summary["pins"] = [
            pin for pin in self.list_pins(partition) if int(pin["version_id"]) == version_id
        ]
        return summary

    def partition_state(self, partition: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = self._partition_row(conn, partition)
            current = row["current_version"]
            current = int(current) if current is not None else None
            versions = self.list_versions(partition)
            pins = self.list_pins(partition)

            refs_rows = conn.execute(
                """
                SELECT vf.path, vf.version_id, vf.kind, vf.rows, vf.amount_cents,
                       vf.bytes, vf.sha256
                FROM version_files vf
                JOIN versions v ON v.version_id = vf.version_id
                WHERE v.partition = ? AND v.pruned_at IS NULL
                """,
                (partition,),
            ).fetchall()
        refs: dict[str, list[int]] = {}
        latest_meta: dict[str, dict[str, Any]] = {}
        for row_ref in refs_rows:
            refs.setdefault(row_ref["path"], []).append(int(row_ref["version_id"]))
            current_latest = latest_meta.get(row_ref["path"])
            if current_latest is None or int(row_ref["version_id"]) > current_latest["version_id"]:
                latest_meta[row_ref["path"]] = {
                    "version_id": int(row_ref["version_id"]),
                    "kind": row_ref["kind"],
                    "rows": int(row_ref["rows"]),
                    "amount_cents": int(row_ref["amount_cents"]),
                    "bytes": int(row_ref["bytes"]),
                    "sha256": row_ref["sha256"],
                }

        manifest = self.load_manifest(current) if current is not None else None
        current_paths = {file["path"] for file in manifest["files"]} if manifest else set()
        partition_dir = self.settings.data_dir / partition
        physical = []
        if partition_dir.exists():
            for path in sorted(partition_dir.glob("*.parquet")):
                rel = f"{partition}/{path.name}"
                stat = path.stat()
                meta = latest_meta.get(rel, {})
                physical.append(
                    {
                        "path": rel,
                        "bytes": stat.st_size,
                        "rows": meta.get("rows"),
                        "amount_cents": meta.get("amount_cents"),
                        "sha256": meta.get("sha256"),
                        "kind": meta.get("kind", "unknown"),
                        "referenced_by": sorted(refs.get(rel, []), reverse=True),
                        "referenced_by_current": rel in current_paths,
                        "orphan": len(refs.get(rel, [])) == 0,
                        "modified_at": stat.st_mtime,
                        "read_only": not bool(stat.st_mode & 0o222),
                    }
                )
        distribution = {
            "files": len(physical),
            "small": sum(1 for file in physical if file["kind"] in {"small", "append"}),
            "compacted": sum(1 for file in physical if file["kind"] == "compacted"),
            "orphan": sum(1 for file in physical if file["orphan"]),
            "bytes": sum(file["bytes"] for file in physical),
            "current_files": len(current_paths),
        }
        staging = sorted(self.settings.staging_dir.glob("*.parquet"))
        return {
            "partition": partition,
            "current_version": current,
            "partition_row": {
                "name": row["name"],
                "created_at": row["created_at"],
                "next_event_id": int(row["next_event_id"]),
            },
            "current_manifest_aggregates": manifest["aggregates"] if manifest else None,
            "versions": versions,
            "pins": pins,
            "files": physical,
            "distribution": distribution,
            "staging_files": [
                {"name": path.name, "bytes": path.stat().st_size} for path in staging
            ],
            "keep_versions": self.settings.keep_versions,
            "generated_at": utc_now(),
        }

    # --------------------------------------------------------------- pin/cleanup
    def pin_version(self, partition: str, version_id: int, note: str = "") -> dict[str, Any]:
        with self._write_tx() as conn:
            vrow = self._resolve_pruned(conn, version_id)
            if vrow["partition"] != partition:
                raise VersionNotFound(f"v{version_id} 不属于分区 {partition}")
            existing = conn.execute(
                """
                SELECT id FROM pins
                WHERE partition = ? AND version_id = ? AND released_at IS NULL
                """,
                (partition, version_id),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE pins SET note = ? WHERE id = ?", (note, existing["id"])
                )
                pin_id = int(existing["id"])
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO pins(partition, version_id, note, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (partition, version_id, note, utc_now()),
                )
                pin_id = int(cursor.lastrowid)
        return {
            "id": pin_id,
            "partition": partition,
            "version_id": version_id,
            "note": note,
            "created_at": utc_now(),
            "active": True,
        }

    def unpin(self, partition: str, pin_id: int | None = None, version_id: int | None = None) -> dict[str, Any]:
        if pin_id is None and version_id is None:
            raise ValueError("pin_id 或 version_id 至少提供一个")
        with self._write_tx() as conn:
            if pin_id is not None:
                row = conn.execute(
                    "SELECT * FROM pins WHERE id = ? AND partition = ? AND released_at IS NULL",
                    (pin_id, partition),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM pins WHERE version_id = ? AND partition = ?
                    AND released_at IS NULL ORDER BY id DESC LIMIT 1
                    """,
                    (version_id, partition),
                ).fetchone()
            if row is None:
                raise VersionNotFound("没有找到活动中的固定记录")
            conn.execute(
                "UPDATE pins SET released_at = ? WHERE id = ?", (utc_now(), row["id"])
            )
            return {
                "id": int(row["id"]),
                "version_id": int(row["version_id"]),
                "released_at": utc_now(),
            }

    def cleanup(
        self,
        partition: str,
        keep: int | None = None,
        extra_protected_versions: Iterable[int] | None = None,
    ) -> dict[str, Any]:
        keep = max(1, int(keep or self.settings.keep_versions))
        extra_protected = {int(v) for v in (extra_protected_versions or [])}
        now = utc_now()
        with self._write_tx() as conn:
            self._partition_row(conn, partition)
            versions = conn.execute(
                """
                SELECT version_id, manifest_path, file_count, rows, amount_cents
                FROM versions
                WHERE partition = ? AND pruned_at IS NULL
                ORDER BY version_id DESC
                """,
                (partition,),
            ).fetchall()
            latest_protected = {int(row["version_id"]) for row in versions[:keep]}
            pinned = {
                int(row["version_id"])
                for row in conn.execute(
                    """
                    SELECT version_id FROM pins
                    WHERE partition = ? AND released_at IS NULL
                    """,
                    (partition,),
                ).fetchall()
            }
            leased = {
                int(row["version_id"])
                for row in conn.execute(
                    "SELECT version_id FROM leases WHERE partition = ?", (partition,)
                ).fetchall()
            }
            protected = latest_protected | pinned | leased | extra_protected
            protected &= {int(row["version_id"]) for row in versions}
            prunable = [int(row["version_id"]) for row in versions if int(row["version_id"]) not in protected]
            if prunable:
                conn.executemany(
                    "UPDATE versions SET pruned_at = ? WHERE version_id = ?",
                    [(now, version_id) for version_id in prunable],
                )

            protected_refs: set[str] = set()
            if protected:
                placeholders = ",".join("?" for _ in protected)
                refs = conn.execute(
                    f"""
                    SELECT DISTINCT path FROM version_files
                    WHERE version_id IN ({placeholders})
                    """,
                    tuple(protected),
                ).fetchall()
                protected_refs = {row["path"] for row in refs}

            partition_dir = self.settings.data_dir / partition
            physical = sorted(partition_dir.glob("*.parquet")) if partition_dir.exists() else []
            candidates = []
            for path in physical:
                rel = f"{partition}/{path.name}"
                if rel not in protected_refs:
                    candidates.append((rel, path))

            manifest_to_delete = [
                row["manifest_path"]
                for row in versions
                if int(row["version_id"]) in prunable
            ]

        deleted_files = []
        errors = []
        for rel, path in candidates:
            try:
                path.chmod(0o644)
                size = path.stat().st_size
                path.unlink()
                deleted_files.append({"path": rel, "bytes": size})
            except FileNotFoundError:
                pass
            except OSError as exc:
                errors.append({"path": rel, "error": str(exc)})
        for rel_manifest in manifest_to_delete:
            path = self.settings.data_root / rel_manifest
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                errors.append({"manifest": rel_manifest, "error": str(exc)})

        return {
            "partition": partition,
            "keep": keep,
            "generated_at": now,
            "protected_versions": [
                {
                    "version_id": version_id,
                    "reason": (
                        "最近版本"
                        if version_id in latest_protected
                        else "页面固定"
                        if version_id in pinned
                        else "活动快照查询"
                        if version_id in leased
                        else "运行中任务"
                    ),
                }
                for version_id in sorted(protected, reverse=True)
            ],
            "pruned_versions": sorted(prunable, reverse=True),
            "deleted_files": deleted_files,
            "deleted_bytes": sum(file["bytes"] for file in deleted_files),
            "kept_file_count": len(protected_refs),
            "errors": errors,
        }

    # ------------------------------------------------------------------- tasks
    def save_task(self, payload: dict[str, Any]) -> None:
        task_id = payload["id"]
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tasks(id, kind, partition, status, created_at, updated_at, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  status = excluded.status,
                  updated_at = excluded.updated_at,
                  payload = excluded.payload
                """,
                (
                    task_id,
                    payload.get("kind", "compaction"),
                    payload["partition"],
                    payload["status"],
                    payload["created_at"],
                    payload["updated_at"],
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT payload FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return json.loads(row["payload"]) if row else None

    def list_tasks(self, partition: str, limit: int = 30) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload FROM tasks WHERE partition = ?
                ORDER BY created_at DESC LIMIT ?
                """,
                (partition, limit),
            ).fetchall()
        return [json.loads(row["payload"]) for row in rows]
