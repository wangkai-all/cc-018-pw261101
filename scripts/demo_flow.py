#!/usr/bin/env python3
"""Drive the running demo through every required concurrency scenario.

Start the server with `./scripts/run.sh`, then run:
    .venv/bin/python scripts/demo_flow.py
"""
from __future__ import annotations

import argparse
import json
import sys

import httpx


def show(title: str, checks: dict, extra: dict | None = None) -> None:
    print(f"\n=== {title} ===")
    for key, value in checks.items():
        print(f"  {'PASS' if value else 'FAIL'}  {key}")
    for key, value in (extra or {}).items():
        print(f"  {key}: {value}")
    if not all(checks.values()):
        raise SystemExit(f"scenario failed: {title}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--json", action="store_true", help="print full response JSON")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    with httpx.Client(timeout=60) as client:
        health = client.get(f"{base}/api/health").json()
        state = client.get(f"{base}/api/state").json()
        print("catalog:", health)
        print("initial files:", state["distribution"])

        result = client.post(
            f"{base}/api/demo/append-during-compaction",
            json={"max_files": 12, "publish_delay_ms": 1000},
        ).json()
        show(
            "append during compaction",
            result["checks"],
            {
                "before": result["before"],
                "append version": result["append"]["version_id"],
                "conflicts": result["conflict_count"],
                "during old query": result["during_compaction_query"],
                "after": result["after"],
            },
        )

        result = client.post(
            f"{base}/api/demo/dual-compaction",
            json={"max_files": 16, "publish_delay_ms": 800},
        ).json()
        show(
            "dual compaction conflict",
            result["checks"],
            {
                "conflict attempts": json.dumps(result["conflict_attempts"], ensure_ascii=False),
                "before": result["before"],
                "after": result["after"],
            },
        )

        result = client.post(
            f"{base}/api/demo/failure-before-publish",
            json={"max_files": 16, "publish_delay_ms": 100},
        ).json()
        assert result["compaction"]["status"] == "failed", result["compaction"]["status"]
        assert result["compaction"]["published_version"] is None
        show(
            "failure before publish",
            result["checks"],
            {
                "task status": result["compaction"]["status"],
                "setup appends": len(result.get("setup_appends", [])),
            },
        )

        versions = client.get(f"{base}/api/versions").json()
        old = versions["versions"][-1]["version_id"]
        comparison = client.get(f"{base}/api/compare?version={old}").json()
        show(
            "old snapshot still readable",
            {
                "old_complete": comparison["old"]["complete"],
                "current_complete": comparison["current"]["complete"],
                "old_not_rewritten": comparison["old"]["snapshot_version"] == old,
            },
            {
                "old": comparison["old"]["query"],
                "current": comparison["current"]["query"],
                "delta_rows": comparison["row_delta"],
            },
        )

        result = client.post(f"{base}/api/demo/pin-and-gc", json={"max_files": 64}).json()
        show(
            "pin -> cleanup -> unpin -> reclaim",
            result["checks"],
            {
                "pinned version": result["pinned_version"],
                "reclaimed files": len(result["reclaimed_exclusive_files"]),
                "old query status after unpin": result["old_query_status"],
                "protected during pin": result["cleanup_with_pin"]["protected_versions"],
            },
        )

        state = client.get(f"{base}/api/state").json()
        print("\n=== final state ===")
        print("current version:", state["current_version"])
        print("distribution:", state["distribution"])
        print("versions:", [(v["version_id"], v["operation"], v["pruned"]) for v in state["versions"]])
        if args.json:
            print(json.dumps(state, ensure_ascii=False, indent=2))
        print("\nAll scripted invariants passed.")


if __name__ == "__main__":
    try:
        main()
    except httpx.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        raise SystemExit(1)
