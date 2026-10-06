"""扩展配置的事务化变更(预检、分阶段启用与回退)。

针对"一次变更多个扩展配置,部分扩展失败后在进程内/重启后留下
与变更单不一致的配置组合"的问题,本模块提供:

* 候选快照冻结:变更单提交后先合并出一份不可变的候选配置快照,
  生效中的配置在被共同提交之前保持不变;
* 预检:在冻结快照上校验各扩展配置、扩展间依赖与互斥关系,
  预检失败的变更单直接被拒绝,不触碰任何运行态;
* 分阶段启用:所有相关扩展按稳定顺序(名称字典序)进入 prepare
  阶段暂存变更,全部就绪后共同 commit;任一阶段失败即按相反顺序
  回退已暂存/已提交的参与者;
* 读隔离:进行中的请求通过版本化配置存储持有旧快照,prepare
  期间到达的请求继续看到旧配置,commit 完成后新请求才看到新配置;
* 可恢复状态:每个阶段迁移都追加写入持久化日志(WAL)。扩展不支持
  回退、进程崩溃、变更单被重复提交时,均保留可查询、可恢复的状态;
  崩溃后重启时从日志与最近一份已提交快照恢复一致状态;
* 幂等:同一变更单号重复提交且负载一致时直接返回既有结果,
  负载不一致时报冲突;
* 可观测:每个变更单每个阶段、每个参与者的真实结果均可查询。
"""

from __future__ import annotations

import copy
import enum
import hashlib
import json
import os
import threading
import typing as t
from dataclasses import dataclass, field
from datetime import datetime, timezone

from traitlets import Unicode
from traitlets.config import Config, LoggingConfigurable
from traitlets.traitlets import TraitError

from jupyter_server.config_manager import recursive_update

__all__ = [
    "ChangeOrderConflict",
    "ChangeRecord",
    "ChangeStage",
    "ConfigJournal",
    "ConfigParticipant",
    "ConfigSnapshot",
    "ExtensionConfigChange",
    "ExtensionConfigRolloutCoordinator",
    "InvalidStageTransition",
    "ParticipantRecord",
    "RolloutError",
    "TraitletConfigAdapter",
    "UnknownChangeOrder",
    "VersionedConfigStore",
]


def _utcnow() -> str:
    """返回当前 UTC 时间的 ISO 格式字符串。"""
    return datetime.now(timezone.utc).isoformat()


def _freeze_config_value(value: t.Any) -> t.Any:
    """把配置值冻结为 JSON 安全的结构。

    快照需要持久化(JSON 落盘)并被在途请求安全持有,因此只保留
    字典/列表/标量等配置数据;运行态对象(处理器类、IOLoop 等)
    不属于配置数据,以 repr 占位,避免深拷贝产生副作用。
    """
    if isinstance(value, dict):
        return {str(key): _freeze_config_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_freeze_config_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
    return value


# -----------------------------------------------------------------------------
# 错误类型
# -----------------------------------------------------------------------------


class RolloutError(Exception):
    """配置变更流程的基础错误类型。"""


class ChangeOrderConflict(RolloutError):
    """同一变更单号携带不同负载被重复提交。"""


class UnknownChangeOrder(RolloutError):
    """查询或操作一个不存在的变更单号。"""


class InvalidStageTransition(RolloutError):
    """在当前阶段不允许执行的操作(如对未就绪的变更单执行提交)。"""


# -----------------------------------------------------------------------------
# 阶段状态机
# -----------------------------------------------------------------------------


class ChangeStage(str, enum.Enum):
    """变更单的生命周期阶段。

    正常路径: RECEIVED -> PRECHECKED -> PREPARING -> PREPARED
    -> COMMITTING -> COMMITTED
    失败路径: 预检失败 -> REJECTED;提交前失败 -> ABORTING -> ABORTED;
    提交后失败 -> ROLLING_BACK -> ROLLED_BACK;回退不彻底 -> DIVERGED。
    """

    RECEIVED = "received"
    PRECHECKED = "prechecked"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    DIVERGED = "diverged"
    REJECTED = "rejected"


#: 终态阶段集合;处于终态的变更单不再被自动推进。
TERMINAL_STAGES = frozenset(
    {
        ChangeStage.COMMITTED,
        ChangeStage.ABORTED,
        ChangeStage.ROLLED_BACK,
        ChangeStage.DIVERGED,
        ChangeStage.REJECTED,
    }
)


# -----------------------------------------------------------------------------
# 数据模型
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtensionConfigChange:
    """变更单中针对单个扩展的一份候选配置。"""

    extension: str
    config: t.Mapping[str, t.Any]

    def to_dict(self) -> dict[str, t.Any]:
        """序列化为可 JSON 化的字典。"""
        return {"extension": self.extension, "config": dict(self.config)}

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> ExtensionConfigChange:
        """从字典还原。"""
        return cls(extension=str(data["extension"]), config=dict(data["config"]))


@dataclass
class ParticipantRecord:
    """单个扩展(参与者)在一次变更中的真实执行结果。"""

    name: str
    supports_rollback: bool = True
    prepared: bool = False
    committed: bool = False
    rolled_back: bool = False
    error: str | None = None

    def to_dict(self) -> dict[str, t.Any]:
        """序列化为可 JSON 化的字典。"""
        return {
            "name": self.name,
            "supports_rollback": self.supports_rollback,
            "prepared": self.prepared,
            "committed": self.committed,
            "rolled_back": self.rolled_back,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> ParticipantRecord:
        """从字典还原。"""
        return cls(
            name=str(data["name"]),
            supports_rollback=bool(data.get("supports_rollback", True)),
            prepared=bool(data.get("prepared", False)),
            committed=bool(data.get("committed", False)),
            rolled_back=bool(data.get("rolled_back", False)),
            error=data.get("error"),
        )


@dataclass
class ChangeRecord:
    """一张变更单的全部状态:负载、阶段历史与每个参与者的结果。"""

    change_id: str
    payload_hash: str
    changes: tuple[ExtensionConfigChange, ...]
    auto_commit: bool = True
    stage: ChangeStage = ChangeStage.RECEIVED
    participants: dict[str, ParticipantRecord] = field(default_factory=dict)
    precheck_problems: list[str] = field(default_factory=list)
    precheck_warnings: list[str] = field(default_factory=list)
    stage_history: list[dict[str, t.Any]] = field(default_factory=list)
    base_configs: dict[str, t.Any] = field(default_factory=dict)
    error: str | None = None
    recovery_path: str | None = None
    created_at: str = field(default_factory=_utcnow)
    updated_at: str = field(default_factory=_utcnow)
    # 冻结的候选快照,仅在内存中持有,不序列化。
    candidate: ConfigSnapshot | None = field(default=None, repr=False, compare=False)

    @property
    def changed_extensions(self) -> list[str]:
        """本变更单涉及的扩展名,按稳定(字典序)顺序返回。"""
        return sorted({change.extension for change in self.changes})

    def delta_for(self, extension: str) -> dict[str, t.Any]:
        """变更单中针对某扩展的配置增量(递归合并该扩展的所有条目)。

        参与者只应接收增量;合并后的完整快照仅用于生效视图切换。
        """
        delta: dict[str, t.Any] = {}
        for change in self.changes:
            if change.extension == extension:
                recursive_update(delta, copy.deepcopy(dict(change.config)))
        return delta

    def set_stage(self, stage: ChangeStage, detail: str = "") -> None:
        """推进阶段并记录阶段历史(运维可查询的真实轨迹)。"""
        self.stage = stage
        self.updated_at = _utcnow()
        self.stage_history.append({"stage": stage.value, "at": self.updated_at, "detail": detail})

    def to_dict(self) -> dict[str, t.Any]:
        """序列化为可 JSON 化的字典(用于日志与状态查询)。"""
        return {
            "change_id": self.change_id,
            "payload_hash": self.payload_hash,
            "auto_commit": self.auto_commit,
            "stage": self.stage.value,
            "changes": [change.to_dict() for change in self.changes],
            "participants": {
                name: participant.to_dict() for name, participant in self.participants.items()
            },
            "precheck_problems": list(self.precheck_problems),
            "precheck_warnings": list(self.precheck_warnings),
            "stage_history": [dict(entry) for entry in self.stage_history],
            "base_configs": copy.deepcopy(self.base_configs),
            "error": self.error,
            "recovery_path": self.recovery_path,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> ChangeRecord:
        """从字典还原(用于崩溃恢复时重建登记簿)。"""
        record = cls(
            change_id=str(data["change_id"]),
            payload_hash=str(data["payload_hash"]),
            changes=tuple(
                ExtensionConfigChange.from_dict(change) for change in data.get("changes", [])
            ),
            auto_commit=bool(data.get("auto_commit", True)),
            stage=ChangeStage(data.get("stage", ChangeStage.RECEIVED.value)),
            participants={
                name: ParticipantRecord.from_dict(participant)
                for name, participant in data.get("participants", {}).items()
            },
            precheck_problems=list(data.get("precheck_problems", [])),
            precheck_warnings=list(data.get("precheck_warnings", [])),
            stage_history=[dict(entry) for entry in data.get("stage_history", [])],
            base_configs=dict(data.get("base_configs", {})),
            error=data.get("error"),
            recovery_path=data.get("recovery_path"),
            created_at=str(data.get("created_at", _utcnow())),
            updated_at=str(data.get("updated_at", _utcnow())),
        )
        return record


# -----------------------------------------------------------------------------
# 配置快照与版本化存储
# -----------------------------------------------------------------------------


class ConfigSnapshot:
    """一份冻结的、按版本号标识的扩展配置快照。

    快照在构造时深拷贝输入,对外访问时返回深拷贝,因此持有快照的
    在途请求不受后续提交影响,继续看到旧配置。
    """

    __slots__ = ("_configs", "version")

    def __init__(self, version: int, configs: t.Mapping[str, t.Mapping[str, t.Any]]):
        self.version = version
        self._configs: dict[str, dict[str, t.Any]] = {
            str(extension): _freeze_config_value(dict(config))
            for extension, config in configs.items()
        }

    @property
    def extensions(self) -> tuple[str, ...]:
        """快照中出现过的扩展名(字典序)。"""
        return tuple(sorted(self._configs))

    def config_for(
        self, extension: str, default: t.Mapping[str, t.Any] | None = None
    ) -> dict[str, t.Any]:
        """返回某扩展配置的深拷贝;不存在时返回 default 的拷贝或空字典。"""
        if extension in self._configs:
            return copy.deepcopy(self._configs[extension])
        return copy.deepcopy(dict(default or {}))

    def merged(self, changes: t.Iterable[ExtensionConfigChange]) -> ConfigSnapshot:
        """在本快照基础上递归合并变更,生成版本号 +1 的新快照。"""
        configs = copy.deepcopy(self._configs)
        for change in changes:
            target = configs.setdefault(change.extension, {})
            recursive_update(target, _freeze_config_value(dict(change.config)))
        return ConfigSnapshot(self.version + 1, configs)

    def replaced(self, configs: t.Mapping[str, t.Mapping[str, t.Any]]) -> ConfigSnapshot:
        """整体替换指定扩展的配置(而非合并),生成版本号 +1 的新快照。

        用于回退时精确恢复到变更前捕获的基线配置。
        """
        new_configs = copy.deepcopy(self._configs)
        for extension, config in configs.items():
            new_configs[extension] = _freeze_config_value(dict(config))
        return ConfigSnapshot(self.version + 1, new_configs)

    def to_dict(self) -> dict[str, t.Any]:
        """序列化为可 JSON 化的字典。"""
        return {"version": self.version, "configs": copy.deepcopy(self._configs)}

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> ConfigSnapshot:
        """从字典还原。"""
        return cls(version=int(data["version"]), configs=data.get("configs", {}))


class VersionedConfigStore:
    """当前生效配置快照的版本化存储。

    读取方通过 :meth:`acquire_view` 取得当前快照(不可变对象),
    提交方通过 :meth:`swap` 原子地切换版本。prepare 期间到达的请求
    仍拿到旧快照;commit 完成后新请求才看到新快照。
    """

    def __init__(self, initial: ConfigSnapshot | None = None):
        self._current = initial or ConfigSnapshot(version=0, configs={})
        self._lock = threading.Lock()

    @property
    def current(self) -> ConfigSnapshot:
        """当前生效的快照。"""
        return self.acquire_view()

    def acquire_view(self) -> ConfigSnapshot:
        """获取当前生效快照的视图(供一次请求全程持有)。"""
        with self._lock:
            return self._current

    def swap(self, snapshot: ConfigSnapshot) -> None:
        """原子地切换到新快照;已持有旧快照的读者不受影响。"""
        with self._lock:
            self._current = snapshot


# -----------------------------------------------------------------------------
# 参与者协议与 traitlets 适配器
# -----------------------------------------------------------------------------


class ConfigParticipant(t.Protocol):
    """参与配置两阶段提交的扩展协议。

    各方法语义:

    * ``validate``: 只校验,不产生副作用;返回问题列表(空表示通过);
    * ``prepare``: 暂存候选配置,不得激活,不得影响在途请求;
    * ``commit``: 激活此前暂存的配置;
    * ``rollback``: 撤销 prepare/commit 的影响,恢复到变更前状态。
    """

    name: str
    supports_rollback: bool
    dependencies: t.Collection[str]
    conflicts: t.Collection[str]

    def validate(self, candidate: t.Mapping[str, t.Any]) -> list[str]:
        """校验候选配置,返回问题列表(空列表表示通过)。"""
        ...

    def prepare(self, candidate: t.Mapping[str, t.Any]) -> None:
        """暂存候选配置(不激活)。"""
        ...

    def commit(self) -> None:
        """激活已暂存的配置。"""
        ...

    def rollback(self) -> None:
        """撤销暂存/已提交的配置,恢复变更前状态。"""
        ...


class TraitletConfigAdapter:
    """把基于 traitlets 的扩展(如 ExtensionApp 实例)适配为参与者。

    * ``validate`` 对照扩展的可配置 trait 做未知项与取值校验;
    * ``prepare`` 捕获变更前取值并暂存候选配置(不触碰运行态);
    * ``commit`` 通过 ``update_config`` 应用候选配置,并在目标具备
      ExtensionApp 风格的 ``_prepare_config``/``settings`` 时同步刷新
      web 应用设置;
    * ``rollback`` 重新应用 prepare 时捕获的旧值。

    目标对象可通过类属性 ``supports_config_rollback``、
    ``config_dependencies``、``config_conflicts`` 声明能力与依赖,
    也可在构造适配器时显式传入。
    """

    def __init__(
        self,
        target: t.Any,
        *,
        name: str | None = None,
        supports_rollback: bool | None = None,
        dependencies: t.Collection[str] = (),
        conflicts: t.Collection[str] = (),
    ):
        self.target = target
        self.name = name or getattr(target, "name", None) or type(target).__name__
        if supports_rollback is None:
            supports_rollback = bool(getattr(target, "supports_config_rollback", True))
        self.supports_rollback = supports_rollback
        self.dependencies: t.Collection[str] = tuple(
            dependencies or getattr(target, "config_dependencies", ())
        )
        self.conflicts: t.Collection[str] = tuple(
            conflicts or getattr(target, "config_conflicts", ())
        )
        self._staged: dict[str, t.Any] | None = None
        self._previous: dict[str, t.Any] | None = None

    def current_config(self) -> dict[str, t.Any]:
        """返回目标当前可配置 trait 取值的冻结(JSON 安全)快照。

        持有运行态对象(处理器、IOLoop 等)的取值不属于配置数据,
        不纳入快照。
        """
        traits = self.target.traits(config=True) if hasattr(self.target, "traits") else {}
        snapshot: dict[str, t.Any] = {}
        for key in traits:
            value = getattr(self.target, key)
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                continue
            snapshot[key] = _freeze_config_value(value)
        return snapshot

    def validate(self, candidate: t.Mapping[str, t.Any]) -> list[str]:
        """对照可配置 trait 校验候选配置,返回问题列表。"""
        problems: list[str] = []
        if not hasattr(self.target, "traits"):
            return [f"扩展 {self.name} 不支持基于 traitlets 的配置校验"]
        configurable = self.target.traits(config=True)
        all_traits = self.target.traits()
        for key, value in candidate.items():
            trait = all_traits.get(key)
            if trait is None:
                problems.append(f"扩展 {self.name} 不存在配置项 {key!r}")
                continue
            if key not in configurable:
                problems.append(f"扩展 {self.name} 的配置项 {key!r} 不可配置")
                continue
            try:
                trait.validate(self.target, value)
            except TraitError as e:
                problems.append(f"扩展 {self.name} 的配置项 {key!r} 校验失败: {e}")
            except Exception as e:  # noqa: BLE001 - 预检不应被单个校验器拖垮
                problems.append(f"扩展 {self.name} 的配置项 {key!r} 校验出现异常: {e}")
        return problems

    def prepare(self, candidate: t.Mapping[str, t.Any]) -> None:
        """暂存候选配置并捕获变更前取值(不激活)。"""
        known = self.target.traits() if hasattr(self.target, "traits") else {}
        previous: dict[str, t.Any] = {}
        for key in candidate:
            if known and key not in known:
                continue
            value = getattr(self.target, key, None)
            try:
                # 优先深拷贝,避免后续原地修改污染基线;
                # 运行态对象不可深拷贝时保留原引用(恢复语义不变)。
                previous[key] = copy.deepcopy(value)
            except Exception:  # noqa: BLE001
                previous[key] = value
        self._previous = previous
        self._staged = copy.deepcopy(dict(candidate))

    def commit(self) -> None:
        """激活已暂存的候选配置。"""
        if self._staged is None:
            msg = f"扩展 {self.name} 尚未 prepare,不能 commit"
            raise RolloutError(msg)
        self._apply(self._staged)

    def rollback(self) -> None:
        """恢复到 prepare 时捕获的变更前取值。"""
        if self._previous is None:
            return
        self._apply(self._previous)
        self._staged = None

    def _apply(self, config: t.Mapping[str, t.Any]) -> None:
        """把配置应用到目标对象,并尽力同步 ExtensionApp 风格的设置。"""
        payload: dict[str, t.Any] = {}
        for key, value in config.items():
            try:
                payload[key] = copy.deepcopy(value)
            except Exception:  # noqa: BLE001 - 运行态对象按引用传递
                payload[key] = value
        if hasattr(self.target, "update_config"):
            section = type(self.target).__name__
            self.target.update_config(Config({section: payload}))
        else:
            for key, value in payload.items():
                setattr(self.target, key, value)
        prepare_config = getattr(self.target, "_prepare_config", None)
        if callable(prepare_config):
            # ExtensionApp: 重建 settings 中的配置快照
            prepare_config()
            serverapp = getattr(self.target, "serverapp", None)
            web_app = getattr(serverapp, "web_app", None) if serverapp is not None else None
            if web_app is not None:
                web_app.settings.update(getattr(self.target, "settings", {}))


# -----------------------------------------------------------------------------
# 持久化日志(崩溃恢复)
# -----------------------------------------------------------------------------


class ConfigJournal:
    """变更单阶段迁移的追加式持久化日志。

    * 每次阶段迁移追加一条 JSON 记录并 fsync,进程崩溃后可通过
      重放日志重建变更单登记簿;
    * 已提交快照以"临时文件 + 原子改名"的方式落盘,保证重启后
      恢复到的是一份完整的一致配置;
    * 回退不彻底的变更单会额外落盘一份恢复工件,供运维人工恢复。
    """

    JOURNAL_FILE = "journal.jsonl"
    SNAPSHOT_FILE = "committed-snapshot.json"

    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    @property
    def journal_path(self) -> str:
        """日志文件路径。"""
        return os.path.join(self.state_dir, self.JOURNAL_FILE)

    @property
    def snapshot_path(self) -> str:
        """最近一份已提交快照的路径。"""
        return os.path.join(self.state_dir, self.SNAPSHOT_FILE)

    def append(
        self,
        event: str,
        record: ChangeRecord | None = None,
        **data: t.Any,
    ) -> None:
        """追加一条日志记录并落盘(fsync)。"""
        entry: dict[str, t.Any] = {"event": event, "at": _utcnow()}
        if record is not None:
            entry["change_id"] = record.change_id
            entry["record"] = record.to_dict()
        entry.update(data)
        line = json.dumps(entry, ensure_ascii=False, default=str)
        with open(self.journal_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def events(self) -> list[dict[str, t.Any]]:
        """按写入顺序读出全部日志记录;容忍崩溃造成的末尾半行。"""
        if not os.path.exists(self.journal_path):
            return []
        events: list[dict[str, t.Any]] = []
        with open(self.journal_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    # 崩溃可能留下未写完整的最后一行,忽略之。
                    continue
        return events

    def save_snapshot(self, snapshot: ConfigSnapshot) -> None:
        """原子地持久化一份已提交快照。"""
        tmp_path = self.snapshot_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(snapshot.to_dict(), f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.snapshot_path)

    def load_snapshot(self) -> ConfigSnapshot | None:
        """读取最近一份已提交快照;不存在时返回 None。"""
        if not os.path.exists(self.snapshot_path):
            return None
        with open(self.snapshot_path, encoding="utf-8") as f:
            return ConfigSnapshot.from_dict(json.load(f))

    def save_recovery_artifact(self, change_id: str, payload: t.Mapping[str, t.Any]) -> str:
        """为回退不彻底的变更单落盘恢复工件,返回其路径。"""
        path = os.path.join(self.state_dir, f"recovery-{change_id}.json")
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(dict(payload), f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        return path


# -----------------------------------------------------------------------------
# 变更协调器
# -----------------------------------------------------------------------------


def _hash_changes(changes: tuple[ExtensionConfigChange, ...]) -> str:
    """对变更负载做规范化哈希,用于变更单幂等判定。

    与变更条目顺序无关:先逐条规范化 JSON,再按 (扩展名, 负载) 排序。
    """
    canonical = sorted(
        (change.extension, json.dumps(change.config, sort_keys=True, default=str))
        for change in changes
    )
    blob = json.dumps(canonical, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class ExtensionConfigRolloutCoordinator(LoggingConfigurable):
    """扩展配置变更的两阶段提交协调器。

    使用方式::

        coordinator = ExtensionConfigRolloutCoordinator(participants=[...])
        record = coordinator.submit("CHG-001", {"ext_a": {...}, "ext_b": {...}})
        assert record.stage is ChangeStage.COMMITTED

    或分阶段人工推进::

        record = coordinator.submit("CHG-002", {...}, auto_commit=False)
        # record.stage == ChangeStage.PREPARED,运行中的请求仍看到旧配置
        coordinator.commit("CHG-002")  # 或 coordinator.rollback("CHG-002")

    进程启动时调用 :meth:`recover` 重放日志,把被崩溃打断的变更单
    收尾为可查询的终态,并把配置存储恢复为最近一份已提交快照。
    """

    state_dir = Unicode(help="变更日志与已提交快照的持久化目录(需跨进程重启保留)。").tag(
        config=True
    )

    def __init__(
        self,
        participants: t.Iterable[ConfigParticipant] = (),
        base_configs: t.Mapping[str, t.Mapping[str, t.Any]] | None = None,
        **kwargs: t.Any,
    ):
        super().__init__(**kwargs)
        if not self.state_dir:
            from jupyter_core.paths import jupyter_data_dir

            self.state_dir = os.path.join(jupyter_data_dir(), "server", "config-rollout")
        self.store = VersionedConfigStore(ConfigSnapshot(version=0, configs=base_configs or {}))
        self.journal = ConfigJournal(self.state_dir)
        self.participants: dict[str, ConfigParticipant] = {}
        self.changes: dict[str, ChangeRecord] = {}
        self._lock = threading.RLock()
        self._recovered = False
        for participant in participants:
            self.register_participant(participant)

    # -- 参与者登记 ------------------------------------------------------

    def register_participant(self, participant: ConfigParticipant) -> None:
        """登记一个扩展参与者(按名称去重,后登记覆盖先登记)。"""
        self.participants[participant.name] = participant

    def seed_base_configs(self, configs: t.Mapping[str, t.Mapping[str, t.Any]]) -> None:
        """用运行态配置填充基线快照。

        仅在持久化目录中不存在已提交快照时生效;否则以已提交快照为准,
        避免重启后运行态与变更单历史不一致。
        """
        with self._lock:
            if self.journal.load_snapshot() is None:
                self.store.swap(self.store.acquire_view().replaced(configs))

    # -- 变更单提交 ------------------------------------------------------

    def submit(
        self,
        change_id: str,
        changes: t.Mapping[str, t.Mapping[str, t.Any]]
        | t.Iterable[ExtensionConfigChange | t.Mapping[str, t.Any]],
        *,
        auto_commit: bool = True,
    ) -> ChangeRecord:
        """提交一张变更单:冻结候选快照 -> 预检 -> 分阶段准备 -> 共同提交。

        * 同一 ``change_id`` 且负载一致:直接返回既有记录(幂等重放),
          不会重复执行;
        * 同一 ``change_id`` 但负载不同:抛出 :class:`ChangeOrderConflict`;
        * 预检失败:返回处于 ``REJECTED`` 阶段的记录,运行态不受影响;
        * ``auto_commit=False`` 时,全部参与者 prepare 成功后停在
          ``PREPARED`` 阶段,等待显式 :meth:`commit` / :meth:`rollback`。
        """
        normalized = self._normalize_changes(changes)
        payload_hash = _hash_changes(normalized)
        with self._lock:
            existing = self.changes.get(change_id)
            if existing is not None:
                if existing.payload_hash == payload_hash:
                    self.log.info("变更单 %s 重复提交且负载一致,返回既有结果", change_id)
                    return existing
                msg = f"变更单 {change_id} 已存在但负载不一致"
                raise ChangeOrderConflict(msg)

            record = ChangeRecord(
                change_id=change_id,
                payload_hash=payload_hash,
                changes=normalized,
                auto_commit=auto_commit,
            )
            # 捕获涉及扩展的变更前基线,供回退精确恢复。
            view = self.store.acquire_view()
            record.base_configs = {
                extension: view.config_for(extension) for extension in record.changed_extensions
            }
            self.changes[change_id] = record
            self._transition(record, ChangeStage.RECEIVED, "submitted")

            self._precheck(record)
            if record.precheck_problems:
                self._transition(
                    record,
                    ChangeStage.REJECTED,
                    "precheck_failed",
                    detail="; ".join(record.precheck_problems),
                )
                return record

            self._prepare_all(record)
            if record.stage is ChangeStage.PREPARED and auto_commit:
                self._commit(record)
            return record

    # -- 阶段推进(公开) -------------------------------------------------

    def commit(self, change_id: str) -> ChangeRecord:
        """共同提交一张已就绪(PREPARED)的变更单。"""
        with self._lock:
            record = self._require_record(change_id)
            if record.stage is not ChangeStage.PREPARED:
                msg = f"变更单 {change_id} 处于 {record.stage.value} 阶段,不能提交"
                raise InvalidStageTransition(msg)
            self._commit(record)
            return record

    def rollback(self, change_id: str) -> ChangeRecord:
        """回退一张已就绪或已提交的变更单。

        已提交变更的回退会把涉及扩展的生效配置恢复为提交时捕获的
        基线;若其间有其他变更单修改了相同扩展,那些修改也会被覆盖,
        运维应通过状态查询确认后再执行。
        """
        with self._lock:
            record = self._require_record(change_id)
            if record.stage not in (ChangeStage.PREPARED, ChangeStage.COMMITTED):
                msg = f"变更单 {change_id} 处于 {record.stage.value} 阶段,不能回退"
                raise InvalidStageTransition(msg)
            self._rollback(record, reason="运维手动回退")
            return record

    # -- 状态查询 --------------------------------------------------------

    def status(self, change_id: str | None = None) -> dict[str, t.Any]:
        """查询变更单每个阶段、每个参与者的真实结果。

        不传 ``change_id`` 时返回全量登记簿与当前生效快照版本。
        """
        with self._lock:
            if change_id is not None:
                return self._require_record(change_id).to_dict()
            return {
                "store_version": self.store.current.version,
                "changes": {cid: record.to_dict() for cid, record in sorted(self.changes.items())},
            }

    def current_config(self, extension: str) -> dict[str, t.Any]:
        """返回当前生效的某扩展配置(供请求路径读取)。"""
        return self.store.acquire_view().config_for(extension)

    # -- 崩溃恢复 --------------------------------------------------------

    def recover(self) -> list[ChangeRecord]:
        """重放日志,恢复崩溃前状态。

        * 重建变更单登记簿(幂等判定的依据);
        * 被崩溃打断、未达终态的变更单收尾为 ``ABORTED`` —— 已提交
          快照从未被部分写盘,运行态配置保持一致;
        * 配置存储恢复为最近一份已提交快照。

        返回被收尾的中断变更单列表。重复调用是安全的(仅首次生效)。
        """
        with self._lock:
            if self._recovered:
                return []
            self._recovered = True
            records: dict[str, ChangeRecord] = {}
            for event in self.journal.events():
                change_id = event.get("change_id")
                snapshot = event.get("record")
                if change_id and snapshot:
                    records[change_id] = ChangeRecord.from_dict(snapshot)
            self.changes.update(records)

            interrupted = [
                record for record in records.values() if record.stage not in TERMINAL_STAGES
            ]
            for record in interrupted:
                record.error = record.error or "进程重启导致变更单中断"
                self._transition(
                    record,
                    ChangeStage.ABORTED,
                    "recovered",
                    detail="进程重启前未完成;已提交快照未被部分修改,运行态配置保持一致",
                )
            snapshot = self.journal.load_snapshot()
            if snapshot is not None:
                self.store.swap(snapshot)
            if interrupted:
                self.log.warning(
                    "配置变更恢复: %d 张变更单在重启前未完成,已收尾为中止: %s",
                    len(interrupted),
                    ", ".join(sorted(r.change_id for r in interrupted)),
                )
            return interrupted

    # -- 阶段推进(内部) -------------------------------------------------

    def _precheck(self, record: ChangeRecord) -> None:
        """在冻结的候选快照上校验配置、依赖与冲突。"""
        problems: list[str] = []
        warnings: list[str] = []

        seen: set[str] = set()
        for change in record.changes:
            if change.extension in seen:
                problems.append(f"变更单中扩展 {change.extension} 出现多次")
            seen.add(change.extension)

        unknown = [name for name in record.changed_extensions if name not in self.participants]
        for name in unknown:
            problems.append(f"扩展 {name} 未注册为配置参与者")

        # 冻结候选快照:后续预检与提交都基于它,生效配置保持不变。
        candidate = self.store.acquire_view().merged(record.changes)
        record.candidate = candidate

        changed = set(record.changed_extensions)
        for name in record.changed_extensions:
            participant = self.participants.get(name)
            if participant is None:
                continue
            record.participants[name] = ParticipantRecord(
                name=name, supports_rollback=bool(participant.supports_rollback)
            )
            if not participant.supports_rollback:
                warnings.append(f"扩展 {name} 不支持回退;其后的失败可能需人工恢复")
            problems.extend(participant.validate(record.delta_for(name)))
            for dependency in getattr(participant, "dependencies", ()):
                if dependency not in self.participants:
                    problems.append(f"扩展 {name} 依赖的扩展 {dependency} 未注册")
            for conflict in getattr(participant, "conflicts", ()):
                if conflict in changed:
                    problems.append(f"扩展 {name} 与 {conflict} 互斥,不能在同一变更单中修改")

        record.precheck_problems = problems
        record.precheck_warnings = warnings
        if not problems:
            self._transition(record, ChangeStage.PRECHECKED, "prechecked")

    def _prepare_all(self, record: ChangeRecord) -> None:
        """按稳定顺序让各参与者暂存变更;任一失败即回退。"""
        assert record.candidate is not None
        self._transition(record, ChangeStage.PREPARING, "prepare_started")
        for name in record.changed_extensions:
            participant = self.participants[name]
            participant_record = record.participants[name]
            try:
                participant.prepare(record.delta_for(name))
            except Exception as e:  # noqa: BLE001 - 任何参与者失败都要整体回退
                participant_record.error = f"{type(e).__name__}: {e}"
                record.error = f"扩展 {name} 准备失败: {e}"
                self.journal.append("prepare_failed", record, participant=name, error=str(e))
                self._rollback(record, reason=f"扩展 {name} 准备失败")
                return
            participant_record.prepared = True
            self.journal.append("participant_prepared", record, participant=name)
        self._transition(record, ChangeStage.PREPARED, "prepared")

    def _commit(self, record: ChangeRecord) -> None:
        """共同提交:全部参与者激活后,原子切换生效快照并落盘。"""
        assert record.candidate is not None
        self._transition(record, ChangeStage.COMMITTING, "commit_started")
        for name in record.changed_extensions:
            participant = self.participants[name]
            participant_record = record.participants[name]
            try:
                participant.commit()
            except Exception as e:  # noqa: BLE001 - 提交失败必须整体回退
                participant_record.error = f"{type(e).__name__}: {e}"
                record.error = f"扩展 {name} 提交失败: {e}"
                self.journal.append("commit_failed", record, participant=name, error=str(e))
                self._rollback(record, reason=f"扩展 {name} 提交失败")
                return
            participant_record.committed = True
            self.journal.append("participant_committed", record, participant=name)
        # 全部参与者提交完成后,才切换生效快照并持久化。
        # 就绪期间可能有其他变更单已提交,先把本变更的增量重定基到
        # 当前生效快照,避免换入过期候选而覆盖他人的已提交配置。
        record.candidate = self.store.acquire_view().merged(record.changes)
        self.store.swap(record.candidate)
        self.journal.save_snapshot(record.candidate)
        self._transition(record, ChangeStage.COMMITTED, "committed")

    def _rollback(self, record: ChangeRecord, reason: str) -> None:
        """按相反顺序回退已暂存/已提交的参与者。

        不支持回退或回退抛错的参与者会被记录为分叉(diverged),
        变更单进入 ``DIVERGED`` 终态并落盘恢复工件,保留可恢复状态。
        """
        reached_commit = record.stage in (
            ChangeStage.COMMITTING,
            ChangeStage.COMMITTED,
            ChangeStage.ROLLING_BACK,
        )
        self._transition(
            record,
            ChangeStage.ROLLING_BACK if reached_commit else ChangeStage.ABORTING,
            "rollback_started",
            detail=reason,
        )
        diverged: list[str] = []
        for name in reversed(record.changed_extensions):
            participant_record = record.participants.get(name)
            if participant_record is None:
                continue
            if not (participant_record.prepared or participant_record.committed):
                continue
            participant = self.participants.get(name)
            if participant is None or not participant_record.supports_rollback:
                diverged.append(name)
                participant_record.error = participant_record.error or "扩展不支持回退"
                continue
            try:
                participant.rollback()
            except Exception as e:  # noqa: BLE001 - 回退失败也要继续回退其余参与者
                diverged.append(name)
                participant_record.error = f"回退失败: {type(e).__name__}: {e}"
                continue
            participant_record.rolled_back = True
            self.journal.append("participant_rolled_back", record, participant=name)

        if diverged:
            record.error = (record.error or "") + f"; 以下扩展未能回退: {', '.join(diverged)}"
            artifact = {
                "reason": reason,
                "record": record.to_dict(),
                "diverged_extensions": diverged,
                "base_configs": record.base_configs,
                "candidate_deltas": {
                    name: record.delta_for(name) for name in record.changed_extensions
                },
                "instructions": (
                    "以上扩展不支持回退或回退失败;请依据 base_configs 手工恢复其配置, "
                    "或以新的变更单将其改回 base_configs 中的取值。"
                ),
            }
            record.recovery_path = self.journal.save_recovery_artifact(record.change_id, artifact)
            self._transition(
                record,
                ChangeStage.DIVERGED,
                "diverged",
                detail=f"未能回退的扩展: {', '.join(diverged)}; 恢复工件: {record.recovery_path}",
            )
            return

        # 全部回退成功:若已切换过生效快照,恢复为变更前基线。
        if any(p.committed for p in record.participants.values()) or reached_commit:
            restored = self.store.acquire_view().replaced(record.base_configs)
            self.store.swap(restored)
            self.journal.save_snapshot(restored)
        self._transition(
            record,
            ChangeStage.ROLLED_BACK if reached_commit else ChangeStage.ABORTED,
            "rolled_back" if reached_commit else "aborted",
            detail=reason,
        )

    # -- 工具 ------------------------------------------------------------

    def _require_record(self, change_id: str) -> ChangeRecord:
        """按单号取变更单,不存在时抛出 :class:`UnknownChangeOrder`。"""
        record = self.changes.get(change_id)
        if record is None:
            msg = f"变更单 {change_id} 不存在"
            raise UnknownChangeOrder(msg)
        return record

    def _transition(
        self, record: ChangeRecord, stage: ChangeStage, event: str, detail: str = ""
    ) -> None:
        """推进阶段、记录历史并追加持久化日志。"""
        record.set_stage(stage, detail)
        self.journal.append(event, record, detail=detail)

    @staticmethod
    def _normalize_changes(
        changes: t.Mapping[str, t.Mapping[str, t.Any]]
        | t.Iterable[ExtensionConfigChange | t.Mapping[str, t.Any]],
    ) -> tuple[ExtensionConfigChange, ...]:
        """把 {扩展名: 配置} 映射或变更条目序列规范化为元组。"""
        if isinstance(changes, t.Mapping):
            return tuple(
                ExtensionConfigChange(extension=name, config=dict(config))
                for name, config in changes.items()
            )
        return tuple(
            change
            if isinstance(change, ExtensionConfigChange)
            else ExtensionConfigChange.from_dict(change)
            for change in changes
        )
