from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DATA_ROOT = BASE_DIR / "runtime"


@dataclass(frozen=True)
class Settings:
    """Filesystem and demo settings for one catalog instance."""

    data_root: Path = DEFAULT_DATA_ROOT
    partition: str = "orders"
    seed_files: int = 32
    seed_rows: int = 24
    max_compact_files: int = 16
    min_compact_files: int = 2
    keep_versions: int = 3
    auto_seed: bool = True
    max_workers: int = 4
    host: str = "127.0.0.1"
    port: int = 8000

    @property
    def data_dir(self) -> Path:
        return self.data_root / "data"

    @property
    def manifests_dir(self) -> Path:
        return self.data_root / "manifests"

    @property
    def staging_dir(self) -> Path:
        return self.data_root / "staging"

    @property
    def db_path(self) -> Path:
        return self.data_root / "catalog.sqlite3"

    def ensure_dirs(self) -> None:
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Settings":
        root = Path(os.environ.get("PQS_DATA_ROOT", DEFAULT_DATA_ROOT)).expanduser()
        return cls(
            data_root=root.resolve(),
            partition=os.environ.get("PQS_PARTITION", "orders"),
            seed_files=int(os.environ.get("PQS_SEED_FILES", "32")),
            seed_rows=int(os.environ.get("PQS_SEED_ROWS", "24")),
            max_compact_files=int(os.environ.get("PQS_MAX_COMPACT_FILES", "16")),
            min_compact_files=int(os.environ.get("PQS_MIN_COMPACT_FILES", "2")),
            keep_versions=int(os.environ.get("PQS_KEEP_VERSIONS", "3")),
            auto_seed=os.environ.get("PQS_AUTO_SEED", "1") not in {"0", "false", "False"},
            host=os.environ.get("PQS_HOST", "127.0.0.1"),
            port=int(os.environ.get("PQS_PORT", "8000")),
        )
