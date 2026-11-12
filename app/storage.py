from __future__ import annotations

import hashlib
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.int64(), nullable=False),
        pa.field("amount_cents", pa.int64(), nullable=False),
        pa.field("customer", pa.string(), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp("us"), nullable=False),
    ]
)

EVENT_COLUMNS = ["event_id", "amount_cents", "customer", "batch_id", "created_at"]
CUSTOMERS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def path_list(paths: Sequence[str | Path]) -> str:
    return "[" + ",".join(sql_string(p) for p in paths) + "]"


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def generate_events(
    start_id: int,
    count: int,
    batch_id: str,
    *,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    rng = random.Random(seed if seed is not None else int(uuid.uuid4().int & 0xFFFFFFFF))
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows: list[dict[str, Any]] = []
    for offset in range(count):
        rows.append(
            {
                "event_id": start_id + offset,
                "amount_cents": rng.randint(100, 250_000),
                "customer": CUSTOMERS[(start_id + offset) % len(CUSTOMERS)],
                "batch_id": batch_id,
                "created_at": now,
            }
        )
    return rows


def write_parquet(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    table = pa.Table.from_pylist(list(rows), schema=EVENT_SCHEMA)
    pq.write_table(table, path, compression="zstd", row_group_size=1024)


def scan_file(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    parquet = pq.ParquetFile(file_path)
    stats = aggregate_parquet([file_path])
    stats.update(
        {
            "bytes": file_path.stat().st_size,
            "sha256": sha256_file(file_path),
            "row_groups": parquet.metadata.num_row_groups,
            "schema_columns": parquet.schema_arrow.names,
        }
    )
    stats["path"] = str(file_path)
    return stats


def aggregate_parquet(paths: Sequence[str | Path]) -> dict[str, Any]:
    if not paths:
        return {
            "rows": 0,
            "amount_cents": 0,
            "distinct_event_ids": 0,
            "min_event_id": None,
            "max_event_id": None,
        }
    con = duckdb.connect(database=":memory:")
    try:
        query = f"""
            SELECT
              count(*)::BIGINT AS rows,
              coalesce(sum(amount_cents), 0)::BIGINT AS amount_cents,
              count(DISTINCT event_id)::BIGINT AS distinct_event_ids,
              min(event_id)::BIGINT AS min_event_id,
              max(event_id)::BIGINT AS max_event_id
            FROM read_parquet({path_list(paths)}, union_by_name = true)
        """
        row = con.execute(query).fetchone()
        return {
            "rows": int(row[0]),
            "amount_cents": int(row[1]),
            "distinct_event_ids": int(row[2]),
            "min_event_id": int(row[3]) if row[3] is not None else None,
            "max_event_id": int(row[4]) if row[4] is not None else None,
        }
    finally:
        con.close()


def per_file_aggregates(paths: Sequence[str | Path]) -> dict[str, dict[str, int]]:
    if not paths:
        return {}
    con = duckdb.connect(database=":memory:")
    try:
        query = f"""
            SELECT filename, count(*)::BIGINT, coalesce(sum(amount_cents), 0)::BIGINT
            FROM read_parquet({path_list(paths)}, filename = true, union_by_name = true)
            GROUP BY filename
        """
        records: dict[str, dict[str, int]] = {}
        for filename, rows, amount in con.execute(query).fetchall():
            records[str(filename)] = {"rows": int(rows), "amount_cents": int(amount)}
        return records
    finally:
        con.close()


def exact_difference_count(left: Sequence[str | Path], right: Sequence[str | Path]) -> int:
    """Count rows that differ in either direction; 0 proves exact multiset equality."""
    if not left or not right:
        return len(left) + len(right)
    columns = ", ".join(EVENT_COLUMNS)
    con = duckdb.connect(database=":memory:")
    try:
        left_diff = f"""
            SELECT count(*)::BIGINT FROM (
              (SELECT {columns} FROM read_parquet({path_list(left)}, union_by_name = true))
              EXCEPT ALL
              (SELECT {columns} FROM read_parquet({path_list(right)}, union_by_name = true))
            )
        """
        right_diff = f"""
            SELECT count(*)::BIGINT FROM (
              (SELECT {columns} FROM read_parquet({path_list(right)}, union_by_name = true))
              EXCEPT ALL
              (SELECT {columns} FROM read_parquet({path_list(left)}, union_by_name = true))
            )
        """
        return int(con.execute(left_diff).fetchone()[0]) + int(
            con.execute(right_diff).fetchone()[0]
        )
    finally:
        con.close()


def compact_parquet(inputs: Sequence[str | Path], output: str | Path) -> None:
    if not inputs:
        raise ValueError("没有输入文件可合并")
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    try:
        columns = ", ".join(EVENT_COLUMNS)
        sql = f"""
            COPY (
              SELECT {columns}
              FROM read_parquet({path_list(inputs)}, union_by_name = true)
              ORDER BY event_id
            ) TO {sql_string(output_path)}
            (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 8192)
        """
        con.execute(sql)
    finally:
        con.close()


def mark_file_immutable(path: Path) -> None:
    """Best-effort read-only flag for published parquet files."""
    try:
        path.chmod(0o444)
    except OSError:
        pass


@dataclass(frozen=True)
class FileStats:
    path: str
    rows: int
    bytes: int
    amount_cents: int
    distinct_event_ids: int
    min_event_id: int | None
    max_event_id: int | None
    sha256: str
    row_groups: int

    @classmethod
    def from_scan(cls, path: Path) -> "FileStats":
        stats = scan_file(path)
        return cls(
            path=str(path),
            rows=stats["rows"],
            bytes=stats["bytes"],
            amount_cents=stats["amount_cents"],
            distinct_event_ids=stats["distinct_event_ids"],
            min_event_id=stats["min_event_id"],
            max_event_id=stats["max_event_id"],
            sha256=stats["sha256"],
            row_groups=stats["row_groups"],
        )


def validate_single_file(path: Path, expected_rows: int | None = None) -> dict[str, Any]:
    stats = scan_file(path)
    problems: list[str] = []
    if stats["rows"] <= 0:
        problems.append("文件为空")
    if stats["rows"] != stats["distinct_event_ids"]:
        problems.append("文件内 event_id 重复")
    if expected_rows is not None and stats["rows"] != expected_rows:
        problems.append(f"行数不符：期望 {expected_rows}，实际 {stats['rows']}")
    if problems:
        raise ValueError("Parquet 校验失败：" + "；".join(problems))
    stats["valid"] = True
    stats["checked_at"] = utc_now()
    return stats


def validate_compaction(
    inputs: Sequence[Path],
    output: Path,
    expected_input_rows: int,
    expected_input_amount: int,
) -> dict[str, Any]:
    input_actual = aggregate_parquet(inputs)
    output_actual = aggregate_parquet([output])
    problems: list[str] = []

    if input_actual["rows"] != expected_input_rows:
        problems.append(
            f"输入行数与 manifest 不符：{expected_input_rows} / {input_actual['rows']}"
        )
    if input_actual["amount_cents"] != expected_input_amount:
        problems.append(
            "输入金额与 manifest 不符："
            f"{expected_input_amount} / {input_actual['amount_cents']}"
        )
    if output_actual["rows"] <= 0:
        problems.append("合并输出为空")
    if output_actual["rows"] != output_actual["distinct_event_ids"]:
        problems.append("合并输出内 event_id 重复")
    if output_actual["rows"] != input_actual["rows"]:
        problems.append(
            f"合并前后行数不一致：{input_actual['rows']} -> {output_actual['rows']}"
        )
    if output_actual["amount_cents"] != input_actual["amount_cents"]:
        problems.append(
            "合并前后金额不一致："
            f"{input_actual['amount_cents']} -> {output_actual['amount_cents']}"
        )
    difference_rows = exact_difference_count(inputs, [output])
    if difference_rows:
        problems.append(f"合并前后存在 {difference_rows} 行差异（EXCEPT ALL 校验）")

    if problems:
        raise ValueError("合并校验失败：" + "；".join(problems))

    return {
        "valid": True,
        "method": "DuckDB count/sum/distinct + bidirectional EXCEPT ALL",
        "checked_at": utc_now(),
        "input": input_actual,
        "output": output_actual,
        "difference_rows": 0,
    }


def timed_call(fn, *args, **kwargs):
    started = time.perf_counter()
    value = fn(*args, **kwargs)
    return value, (time.perf_counter() - started) * 1000
