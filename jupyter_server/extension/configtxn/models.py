"""扩展配置变更单的数据模型。

本模块定义事务式扩展配置变更的核心数据结构：

- ``ChangeState``：变更单状态机。终态为 ``COMMITTED``、``ROLLED_BACK``、
  ``REJECTED``、``RECOVERY_REQUIRED``。
- ``Stage`` / ``StageStatus``：阶段（冻结、预检、准备、提交、回退、恢复）
  与阶段结果状态。
- ``StageResult``：单个阶段针对单个扩展的真实执行结果，会被写入 journal，
  运维可按变更单查询。
- ``ChangeOrder``：一张变更单（变更单号 + 各扩展配置增量 + 全部阶段结果）。
"""

from __future__ import annotations

import enum
import typing as t
from dataclasses import dataclass, field

from jupyter_server._tz import utcnow

__all__ = [
    "TERMINAL_STATES",
    "ChangeOrder",
    "ChangeState",
    "Stage",
    "StageResult",
    "StageStatus",
]


def _now_iso() -> str:
    """返回当前 UTC 时间的 ISO 字符串。"""
    return utcnow().isoformat()


class ChangeState(str, enum.Enum):
    """变更单状态。

    状态机::

        PROPOSED -> VALIDATED -> PREPARING -> PREPARED -> COMMITTING -> COMMITTED
                      |              |            |
                      v              v            v
                   REJECTED      ROLLING_BACK <---+
                                  |       \
                                  v        v
                              ROLLED_BACK  RECOVERY_REQUIRED
    """

    PROPOSED = "proposed"  # 已受理，候选快照尚未冻结
    VALIDATED = "validated"  # 候选快照已冻结且预检通过
    PREPARING = "preparing"  # 各扩展正在按稳定顺序准备
    PREPARED = "prepared"  # 全部扩展准备完成，等待共同提交
    COMMITTING = "committing"  # 已记录提交决定，正在落盘
    COMMITTED = "committed"  # 终态：已提交
    ROLLING_BACK = "rolling_back"  # 准备失败/中止，正在回退
    ROLLED_BACK = "rolled_back"  # 终态：已回退，未产生任何生效变更
    REJECTED = "rejected"  # 终态：预检未通过，未产生任何生效变更
    RECOVERY_REQUIRED = "recovery_required"  # 终态：回退不完整，保留现场待人工恢复


#: 终态集合：到达后变更单不再发生状态迁移。
TERMINAL_STATES = frozenset(
    {
        ChangeState.COMMITTED,
        ChangeState.ROLLED_BACK,
        ChangeState.REJECTED,
        ChangeState.RECOVERY_REQUIRED,
    }
)


class Stage(str, enum.Enum):
    """变更流水线中的阶段。"""

    FREEZE = "freeze"  # 冻结候选快照
    PREFLIGHT = "preflight"  # 预检（依赖、冲突、可导入性、trait 校验）
    PREPARE = "prepare"  # 各扩展按稳定顺序准备
    COMMIT = "commit"  # 共同提交（落盘 + 切换运行时配置）
    ROLLBACK = "rollback"  # 回退已准备的扩展
    RECOVERY = "recovery"  # 进程重启后的崩溃恢复


class StageStatus(str, enum.Enum):
    """单个阶段结果的执行状态。"""

    OK = "ok"  # 成功
    FAILED = "failed"  # 执行失败（携带 error）
    SKIPPED = "skipped"  # 无需执行（例如该扩展没有可回退的暂存内容）
    UNSUPPORTED = "unsupported"  # 扩展声明不支持该操作（例如不支持回退）


@dataclass
class StageResult:
    """一个阶段针对一个扩展（或全局）的真实执行结果。"""

    stage: str
    status: str
    extension: str | None = None
    detail: str = ""
    error: str | None = None
    ts: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict[str, t.Any]:
        """序列化为可 JSON 化的字典。"""
        return {
            "stage": self.stage,
            "status": self.status,
            "extension": self.extension,
            "detail": self.detail,
            "error": self.error,
            "ts": self.ts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> StageResult:
        """从字典还原。"""
        return cls(
            stage=data["stage"],
            status=data["status"],
            extension=data.get("extension"),
            detail=data.get("detail", ""),
            error=data.get("error"),
            ts=data.get("ts") or _now_iso(),
        )


@dataclass
class ChangeOrder:
    """一张扩展配置变更单。

    ``changes`` 的结构为 ``{扩展名: 增量}``，增量支持的键：

    - ``enabled`` (bool)：是否启用该扩展；
    - ``config`` (dict)：写入该扩展配置 section 的内容
      （递归合并语义，值为 ``None`` 表示删除该键）；
    - ``config_section`` (str, 可选)：配置落盘的 section 名，
      缺省时按扩展名推导。
    """

    change_id: str
    changes: dict[str, t.Any]
    payload_hash: str
    state: ChangeState = ChangeState.PROPOSED
    base_version: int = 0
    candidate_version: int = 0
    #: 冻结时记录的、被触及扩展在基线快照中的条目（用于构造反向变更单）。
    base_entries: dict[str, t.Any] = field(default_factory=dict)
    stages: list[StageResult] = field(default_factory=list)
    error: str | None = None
    recovered: bool = False
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)

    @property
    def terminal(self) -> bool:
        """是否已到达终态。"""
        return self.state in TERMINAL_STATES

    def transition(self, state: ChangeState, error: str | None = None) -> None:
        """迁移状态并刷新更新时间。"""
        self.state = state
        if error is not None:
            self.error = error
        self.updated_at = _now_iso()

    def record(self, result: StageResult) -> None:
        """追加一条阶段结果并刷新更新时间。"""
        self.stages.append(result)
        self.updated_at = _now_iso()

    def stage_results(self, stage: Stage) -> list[StageResult]:
        """按阶段过滤结果。"""
        return [r for r in self.stages if r.stage == stage.value]

    def to_dict(self) -> dict[str, t.Any]:
        """序列化为可 JSON 化的字典（供 API 与 journal 使用）。"""
        return {
            "change_id": self.change_id,
            "changes": self.changes,
            "payload_hash": self.payload_hash,
            "state": self.state.value,
            "base_version": self.base_version,
            "candidate_version": self.candidate_version,
            "base_entries": self.base_entries,
            "stages": [r.to_dict() for r in self.stages],
            "error": self.error,
            "recovered": self.recovered,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "terminal": self.terminal,
        }
