"""扩展配置变更的事务化管理（预检、分阶段启用、回退与崩溃恢复）。"""

from .coordinator import (
    ChangeConflictError,
    ChangeInProgressError,
    ChangeNotFoundError,
    ExtensionConfigTransactionManager,
    ExtensionHooks,
)
from .models import TERMINAL_STATES, ChangeOrder, ChangeState, Stage, StageResult, StageStatus
from .store import ChangeJournal, ConfigSnapshot, SnapshotStore

__all__ = [
    "TERMINAL_STATES",
    "ChangeConflictError",
    "ChangeInProgressError",
    "ChangeJournal",
    "ChangeNotFoundError",
    "ChangeOrder",
    "ChangeState",
    "ConfigSnapshot",
    "ExtensionConfigTransactionManager",
    "ExtensionHooks",
    "SnapshotStore",
    "Stage",
    "StageResult",
    "StageStatus",
]
