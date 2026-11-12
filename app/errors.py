from __future__ import annotations


class CatalogError(RuntimeError):
    """Base error for a user-visible catalog failure."""


class PartitionNotFound(CatalogError):
    pass


class VersionNotFound(CatalogError):
    pass


class VersionPruned(CatalogError):
    pass


class VersionConflict(CatalogError):
    def __init__(self, expected: int | None, current: int | None, message: str | None = None):
        self.expected = expected
        self.current = current
        super().__init__(
            message
            or f"版本冲突：计划基于 v{expected}，当前已推进到 v{current}，将重新规划"
        )


class InjectFailure(CatalogError):
    pass
