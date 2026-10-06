"""扩展配置变更的两阶段提交协调器。

针对“一次变更多个扩展配置，部分扩展启动失败后进程残留不一致配置”的问题，
本模块把每一张变更单（``ChangeOrder``）建模为一个两阶段提交事务：

1. **冻结（freeze）**：以当前已提交快照为基线，叠加变更单内容，冻结出一份
   候选快照；候选快照对读者不可见。
2. **预检（preflight）**：检查依赖、冲突、模块可导入性与配置项合法性；
   任一检查失败，变更单进入 ``REJECTED`` 终态，不产生任何副作用。
3. **准备（prepare）**：各扩展按稳定顺序（依赖拓扑序，名称字典序兜底）
   执行准备钩子；准备期间当前请求继续读取旧的已提交快照。
4. **提交（commit）**：全部准备成功后，先把“提交决定”写入 journal，
   再落盘配置文件、原子切换已提交快照、同步运行时配置，最后调用各扩展的
   提交钩子。提交决定一旦落盘，失败不再回退，而是由重启恢复继续完成。
5. **回退（rollback）**：任一扩展准备失败，按相反顺序回退已准备的扩展；
   声明不支持回退的扩展会被标记为 ``unsupported``，变更单进入
   ``RECOVERY_REQUIRED`` 终态并完整保留现场（journal、候选快照、基线条目）。

崩溃恢复：进程重启时调用 ``recover()`` 重放 journal——

- 已记录提交决定但未完成的变更单：幂等地重新应用候选快照，完成提交；
- 未到达提交决定的变更单：标记为已回退（配置文件从未被它修改过）。

幂等性：``change_id`` 是幂等键。相同 ``change_id`` + 相同内容重复提交，
直接返回已有记录；相同 ``change_id`` + 不同内容，抛出 ``ChangeConflictError``。
"""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import inspect
import json
import os
import re
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass

from jupyter_core.paths import jupyter_data_dir
from traitlets import Any, Unicode, default
from traitlets.config import Config, LoggingConfigurable

from .models import ChangeOrder, ChangeState, Stage, StageResult, StageStatus
from .store import ChangeJournal, ConfigSnapshot, SnapshotStore

__all__ = [
    "ChangeConflictError",
    "ChangeInProgressError",
    "ChangeNotFoundError",
    "ExtensionConfigTransactionManager",
    "ExtensionHooks",
]

#: 变更单号允许使用的字符（同时保证可安全用作文件名）。
CHANGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-:]{0,127}$")

#: 单个扩展变更中允许出现的键。
KNOWN_CHANGE_KEYS = frozenset({"enabled", "config", "config_section"})

#: 启用标记落盘时使用的 section 名。
ENABLE_SECTION = "jupyter_server_config"


class ChangeConflictError(Exception):
    """相同变更单号承载了不同的变更内容。"""


class ChangeInProgressError(Exception):
    """已有变更单正在执行，拒绝并发变更。"""

    def __init__(self, active_change_id: str) -> None:
        self.active_change_id = active_change_id
        super().__init__(f"变更单 {active_change_id!r} 正在执行，请等待其完成后再提交")


class ChangeNotFoundError(Exception):
    """变更单号不存在。"""


@dataclass
class ExtensionHooks:
    """扩展参与两阶段提交的钩子集合。

    任一钩子为 ``None`` 表示该扩展未实现对应阶段；``supports_rollback``
    为 ``False`` 表示扩展明确声明不支持回退——一旦需要回退，
    该扩展会被标记为 ``unsupported`` 并保留现场。
    """

    prepare: t.Callable[..., t.Any] | None = None
    commit: t.Callable[..., t.Any] | None = None
    rollback: t.Callable[..., t.Any] | None = None
    supports_rollback: bool = True


async def _maybe_await(result: t.Any) -> t.Any:
    """等待可能是协程的钩子返回值。"""
    if inspect.isawaitable(result):
        return await result
    return result


class ExtensionConfigTransactionManager(LoggingConfigurable):
    """扩展配置变更的事务管理器。

    负责变更单的受理、冻结、预检、分阶段准备、共同提交、回退与崩溃恢复，
    并向读者提供快照隔离的配置视图。
    """

    state_dir = Unicode(
        "",
        help="变更单 journal 与配置快照的存储目录。",
    ).tag(config=True)

    @default("state_dir")
    def _default_state_dir(self) -> str:
        return os.path.join(jupyter_data_dir(), "extension-configtxn")

    #: 配置中心（jupyter_server.services.config.ConfigManager），负责落盘。
    config_manager = Any(allow_none=True)

    #: 扩展管理器（可选），提供扩展元数据（依赖、冲突、App 类）用于预检。
    extension_manager = Any(allow_none=True)

    #: 所属 ServerApp（可选），提交时同步运行时配置。
    serverapp = Any(allow_none=True)

    def __init__(self, **kwargs: t.Any) -> None:
        super().__init__(**kwargs)
        os.makedirs(self.state_dir, exist_ok=True)
        self._journal = ChangeJournal(os.path.join(self.state_dir, "journal.jsonl"))
        self._snapshots = SnapshotStore(self.state_dir)
        # 重放 journal，重建全部变更单记录与提交决定集合
        self._orders: dict[str, ChangeOrder] = {}
        self._commit_decisions: set[str] = set()
        self._replay()
        self._committed = self._snapshots.load_committed()
        self._active_change_id: str | None = None
        self._hooks: dict[str, ExtensionHooks] = {}
        self._readers: dict[int, int] = {}

    # ------------------------------------------------------------------
    # 读者视图（快照隔离）
    # ------------------------------------------------------------------

    def current_snapshot(self) -> ConfigSnapshot:
        """返回当前已提交快照。

        返回的是不可变对象：提交只会整体替换该指针，进行中的请求
        持有的旧快照对象不受任何影响。
        """
        return self._committed

    @contextmanager
    def read_snapshot(self) -> t.Iterator[ConfigSnapshot]:
        """为一次请求固定一份已提交快照（并记录在读读者数）。"""
        snapshot = self._committed
        self._readers[snapshot.version] = self._readers.get(snapshot.version, 0) + 1
        try:
            yield snapshot
        finally:
            remaining = self._readers.get(snapshot.version, 1) - 1
            if remaining <= 0:
                self._readers.pop(snapshot.version, None)
            else:
                self._readers[snapshot.version] = remaining

    @property
    def active_readers(self) -> dict[str, int]:
        """各快照版本当前的在读读者数（供运维观测）。"""
        return {str(version): count for version, count in sorted(self._readers.items())}

    def get_extension_config(self, name: str) -> dict[str, t.Any]:
        """从已提交快照读取单个扩展的配置视图。"""
        return self._committed.get_extension(name)

    # ------------------------------------------------------------------
    # 变更单查询
    # ------------------------------------------------------------------

    @property
    def transaction_active(self) -> bool:
        """是否有变更单正在执行。"""
        return self._active_change_id is not None

    @property
    def active_change_id(self) -> str | None:
        """正在执行的变更单号。"""
        return self._active_change_id

    def get_order(self, change_id: str) -> ChangeOrder:
        """按变更单号查询记录；不存在时抛出 ``ChangeNotFoundError``。"""
        try:
            return self._orders[change_id]
        except KeyError:
            raise ChangeNotFoundError(f"变更单 {change_id!r} 不存在") from None

    def has_order(self, change_id: str) -> bool:
        """变更单号是否已存在。"""
        return change_id in self._orders

    def list_orders(self) -> list[ChangeOrder]:
        """按受理顺序列出全部变更单。"""
        return list(self._orders.values())

    # ------------------------------------------------------------------
    # 钩子注册
    # ------------------------------------------------------------------

    def register_hooks(self, name: str, hooks: ExtensionHooks) -> None:
        """为扩展注册两阶段提交钩子（测试与未走 ExtensionApp 的扩展使用）。"""
        self._hooks[name] = hooks

    def unregister_hooks(self, name: str) -> None:
        """注销扩展钩子。"""
        self._hooks.pop(name, None)

    # ------------------------------------------------------------------
    # 变更单受理
    # ------------------------------------------------------------------

    async def submit(self, change_id: str, changes: dict[str, t.Any]) -> ChangeOrder:
        """受理并执行一张变更单，返回变更单记录。

        幂等：相同 ``change_id`` 且内容相同的重复提交直接返回已有记录；
        相同 ``change_id`` 但内容不同抛出 ``ChangeConflictError``；
        已有其他变更单在执行时抛出 ``ChangeInProgressError``。
        """
        self._validate_change_id(change_id)
        self._validate_changes(changes)
        if self.config_manager is None:
            msg = "未配置 config_manager，无法执行配置变更"
            raise RuntimeError(msg)
        payload_hash = self._payload_hash(changes)

        existing = self._orders.get(change_id)
        if existing is not None:
            if existing.payload_hash != payload_hash:
                msg = f"变更单 {change_id!r} 已存在且内容不同，拒绝重复使用该单号"
                raise ChangeConflictError(msg)
            if not existing.terminal:
                raise ChangeInProgressError(change_id)
            # 幂等重放：直接返回已有记录，不重复执行
            return existing

        if self._active_change_id is not None:
            raise ChangeInProgressError(self._active_change_id)

        order = ChangeOrder(change_id=change_id, changes=changes, payload_hash=payload_hash)
        self._orders[change_id] = order
        self._journal.append(
            {
                "kind": "proposed",
                "change_id": change_id,
                "changes": changes,
                "payload_hash": payload_hash,
            }
        )
        # 标记活跃（检查与赋值之间无 await，单事件循环下是原子的）
        self._active_change_id = change_id
        try:
            await self._run(order)
        except Exception as e:
            # 未预期异常（钩子异常已在流水线内捕获处理）：尽力把变更单
            # 标记为待恢复，保证运维可查询到这张单的真实状态后再向上抛。
            if not order.terminal:
                try:
                    self._set_state(
                        order,
                        ChangeState.RECOVERY_REQUIRED,
                        error=f"事务执行中断（{type(e).__name__}: {e}），现场已保留",
                    )
                except Exception:
                    self.log.exception("变更单 %s 的恢复状态落盘失败", change_id)
            raise
        finally:
            self._active_change_id = None
        return order

    def preflight(self, changes: dict[str, t.Any]) -> dict[str, t.Any]:
        """仅执行冻结与预检（dry-run），不写入任何持久化状态。"""
        self._validate_changes(changes)
        if self.config_manager is None:
            msg = "未配置 config_manager，无法执行预检"
            raise RuntimeError(msg)
        current = {name: self._read_effective_entry(name, c) for name, c in changes.items()}
        candidate = self._committed.apply_changes(changes, current)
        results = self._run_preflight_checks(changes, candidate)
        ok = all(r.status != StageStatus.FAILED.value for r in results)
        return {
            "ok": ok,
            "base_version": self._committed.version,
            "candidate_version": candidate.version,
            "checks": [r.to_dict() for r in results],
            "candidate": candidate.to_dict(),
        }

    async def rollback_committed(self, change_id: str) -> ChangeOrder:
        """回退一张已提交的变更单。

        通过构造反向变更单（单号 ``<change_id>:rollback``）恢复冻结时记录的
        基线配置；反向变更单同样走完整事务流水线，且天然幂等。
        """
        original = self.get_order(change_id)
        if original.state != ChangeState.COMMITTED:
            msg = (
                f"变更单 {change_id!r} 当前状态为 {original.state.value}，仅已提交的变更单可以回退"
            )
            raise ChangeConflictError(msg)
        inverse_id = f"{change_id}:rollback"
        existing = self._orders.get(inverse_id)
        if existing is not None:
            # 幂等：反向变更单已存在，直接返回其记录（无论内容是否需按当下状态重算）
            if not existing.terminal:
                raise ChangeInProgressError(inverse_id)
            return existing
        inverse: dict[str, t.Any] = {}
        for name, change in original.changes.items():
            base = original.base_entries.get(name, {})
            entry: dict[str, t.Any] = {}
            if "config_section" in change:
                entry["config_section"] = change["config_section"]
            if "enabled" in change:
                # 基线中的 enabled 可能为 None（原本未列出），写回 None 即删除该标记
                entry["enabled"] = base.get("enabled")
            if "config" in change:
                # 变更内容采用递归合并语义，因此回退需要构造“反向增量”：
                # 与当前有效配置对比，显式删除多出的键、写回被改动的键，
                # 使合并结果精确等于冻结时记录的基线内容。
                current = self._read_effective_entry(name, change)
                entry["config"] = self._inverse_delta(base.get("config", {}), current["config"])
            inverse[name] = entry
        return await self.submit(inverse_id, inverse)

    # ------------------------------------------------------------------
    # 崩溃恢复
    # ------------------------------------------------------------------

    def recover(self) -> list[ChangeOrder]:
        """进程重启后的崩溃恢复，返回被恢复处理的变更单列表。

        - 已记录提交决定但未完成：幂等重放候选快照，完成提交；
        - 未到达提交决定且未终止：标记为已回退（它从未触碰过配置文件）。
        """
        recovered: list[ChangeOrder] = []
        for order in self._orders.values():
            decided = order.change_id in self._commit_decisions
            if decided and order.state != ChangeState.COMMITTED:
                candidate = self._snapshots.read_candidate(order.change_id)
                if candidate is None:
                    self._journal_stage(
                        order,
                        StageResult(
                            stage=Stage.RECOVERY.value,
                            status=StageStatus.FAILED.value,
                            detail="候选快照丢失，无法完成提交，现场已保留",
                        ),
                    )
                    self._set_state(
                        order,
                        ChangeState.RECOVERY_REQUIRED,
                        error="候选快照丢失，无法完成崩溃后的提交",
                        recovered=True,
                    )
                else:
                    try:
                        self._persist_candidate(order, candidate)
                        self._snapshots.write_committed(candidate)
                    except Exception as e:
                        # 恢复本身失败：保留现场并继续处理其他变更单，
                        # 不阻塞服务启动；运维可查询该单的真实状态。
                        self.log.exception("变更单 %s 的崩溃恢复失败", order.change_id)
                        self._journal_stage(
                            order,
                            StageResult(
                                stage=Stage.RECOVERY.value,
                                status=StageStatus.FAILED.value,
                                error=f"{type(e).__name__}: {e}",
                                detail="崩溃恢复失败，现场已保留",
                            ),
                        )
                        self._set_state(
                            order,
                            ChangeState.RECOVERY_REQUIRED,
                            error=f"崩溃恢复失败（{type(e).__name__}: {e}），需人工介入",
                            recovered=True,
                        )
                        recovered.append(order)
                        continue
                    self._committed = candidate
                    self._journal_stage(
                        order,
                        StageResult(
                            stage=Stage.RECOVERY.value,
                            status=StageStatus.OK.value,
                            detail="崩溃恢复：重新应用候选快照并完成提交",
                        ),
                    )
                    self._set_state(order, ChangeState.COMMITTED, recovered=True)
                recovered.append(order)
            elif not decided and not order.terminal:
                self._journal_stage(
                    order,
                    StageResult(
                        stage=Stage.RECOVERY.value,
                        status=StageStatus.OK.value,
                        detail="崩溃恢复：未到达提交决定，按回退处理（配置文件未被修改）",
                    ),
                )
                self._set_state(order, ChangeState.ROLLED_BACK, recovered=True)
                recovered.append(order)
        if recovered:
            self.log.info("扩展配置变更崩溃恢复：处理 %d 张未完成的变更单", len(recovered))
        return recovered

    # ------------------------------------------------------------------
    # 事务流水线（内部）
    # ------------------------------------------------------------------

    async def _run(self, order: ChangeOrder) -> None:
        """执行冻结 → 预检 → 准备 → 提交/回退 的完整流水线。"""
        candidate = self._freeze(order)
        if not self._preflight(order, candidate):
            return  # 状态已置为 REJECTED
        self._set_state(order, ChangeState.VALIDATED)

        self._set_state(order, ChangeState.PREPARING)
        prepared: list[str] = []
        failed: StageResult | None = None
        for name in self._stable_order(order.changes):
            result = await self._prepare_one(order, name, candidate)
            if result.status == StageStatus.FAILED.value:
                failed = result
                break
            prepared.append(name)

        if failed is None:
            self._set_state(order, ChangeState.PREPARED)
            await self._commit(order, candidate)
            return

        # 准备失败：回退已准备的扩展（失败者也尝试回退，防御其半成品状态）
        self._set_state(order, ChangeState.ROLLING_BACK, error=failed.error)
        await self._rollback(order, prepared, failed, candidate)

    def _freeze(self, order: ChangeOrder) -> ConfigSnapshot:
        """冻结候选快照：基线 + 变更单 + 当前有效配置现场。"""
        current = {
            name: self._read_effective_entry(name, change) for name, change in order.changes.items()
        }
        candidate = self._committed.apply_changes(order.changes, current)
        order.base_version = self._committed.version
        order.candidate_version = candidate.version
        order.base_entries = current
        self._snapshots.write_candidate(order.change_id, candidate)
        self._journal.append(
            {
                "kind": "freeze",
                "change_id": order.change_id,
                "base_version": order.base_version,
                "candidate_version": order.candidate_version,
                "base_entries": current,
            }
        )
        self._journal_stage(
            order,
            StageResult(
                stage=Stage.FREEZE.value,
                status=StageStatus.OK.value,
                detail=f"候选快照 v{candidate.version} 已冻结（基线 v{order.base_version}）",
            ),
        )
        return candidate

    def _preflight(self, order: ChangeOrder, candidate: ConfigSnapshot) -> bool:
        """执行预检；失败时把变更单置为 REJECTED 并返回 False。"""
        results = self._run_preflight_checks(order.changes, candidate)
        ok = True
        for result in results:
            self._journal_stage(order, result)
            if result.status == StageStatus.FAILED.value:
                ok = False
        if not ok:
            errors = "; ".join(r.error or "" for r in results if r.error)
            self._set_state(order, ChangeState.REJECTED, error=f"预检未通过：{errors}")
        return ok

    async def _prepare_one(
        self, order: ChangeOrder, name: str, candidate: ConfigSnapshot
    ) -> StageResult:
        """准备单个扩展：调用其准备钩子或执行默认暂存校验。"""
        self._journal.append(
            {
                "kind": "stage_begin",
                "change_id": order.change_id,
                "stage": "prepare",
                "extension": name,
            }
        )
        hooks = self._hooks_for(name)
        context = self._hook_context(order, name, candidate)
        try:
            if hooks.prepare is not None:
                detail = await _maybe_await(hooks.prepare(order.changes[name], context))
            else:
                # 默认参与者：仅校验候选配置可序列化（暂存不落地）
                json.dumps(candidate.get_extension(name)["config"])
                detail = "默认暂存校验通过（扩展未注册准备钩子）"
        except Exception as e:
            result = StageResult(
                stage=Stage.PREPARE.value,
                status=StageStatus.FAILED.value,
                extension=name,
                error=f"{type(e).__name__}: {e}",
            )
        else:
            result = StageResult(
                stage=Stage.PREPARE.value,
                status=StageStatus.OK.value,
                extension=name,
                detail=str(detail) if detail else "准备完成",
            )
        self._journal_stage(order, result)
        return result

    async def _commit(self, order: ChangeOrder, candidate: ConfigSnapshot) -> None:
        """共同提交：先落提交决定，再落盘、切换快照、同步运行时、回调钩子。"""
        # 提交决定：此后任何失败都不再回退，由崩溃恢复负责完成
        self._set_state(order, ChangeState.COMMITTING)
        try:
            self._persist_candidate(order, candidate)
            self._snapshots.write_committed(candidate)
            # 原子切换读者视图：此后的读者看到新快照，旧读者不受影响
            self._committed = candidate
            self._apply_runtime(order, candidate)
        except Exception as e:
            self.log.exception("变更单 %s 提交过程中断", order.change_id)
            self._set_state(
                order,
                ChangeState.RECOVERY_REQUIRED,
                error=f"提交过程中断（{type(e).__name__}: {e}），重启后将自动完成提交",
            )
            return

        hook_failed = False
        for name in self._stable_order(order.changes):
            hooks = self._hooks_for(name)
            self._journal.append(
                {
                    "kind": "stage_begin",
                    "change_id": order.change_id,
                    "stage": "commit",
                    "extension": name,
                }
            )
            if hooks.commit is None:
                result = StageResult(
                    stage=Stage.COMMIT.value,
                    status=StageStatus.SKIPPED.value,
                    extension=name,
                    detail="扩展未注册提交钩子",
                )
            else:
                context = self._hook_context(order, name, candidate)
                try:
                    detail = await _maybe_await(hooks.commit(order.changes[name], context))
                    result = StageResult(
                        stage=Stage.COMMIT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail=str(detail) if detail else "提交钩子执行完成",
                    )
                except Exception as e:
                    # 提交决定已生效：钩子失败只记录，不回退
                    self.log.exception("变更单 %s 扩展 %s 的提交钩子失败", order.change_id, name)
                    hook_failed = True
                    result = StageResult(
                        stage=Stage.COMMIT.value,
                        status=StageStatus.FAILED.value,
                        extension=name,
                        error=f"{type(e).__name__}: {e}",
                    )
            self._journal_stage(order, result)
        self._set_state(
            order,
            ChangeState.COMMITTED,
            error="部分扩展的提交钩子执行失败，详见阶段结果" if hook_failed else None,
        )

    async def _rollback(
        self,
        order: ChangeOrder,
        prepared: list[str],
        failed: StageResult,
        candidate: ConfigSnapshot,
    ) -> None:
        """按相反顺序回退已准备的扩展；不支持回退的保留现场。"""
        recovery_needed = False
        targets = list(reversed(prepared))
        if failed.extension and failed.extension not in prepared:
            targets.insert(0, failed.extension)
        for name in targets:
            hooks = self._hooks_for(name)
            self._journal.append(
                {
                    "kind": "stage_begin",
                    "change_id": order.change_id,
                    "stage": "rollback",
                    "extension": name,
                }
            )
            if not hooks.supports_rollback:
                recovery_needed = True
                result = StageResult(
                    stage=Stage.ROLLBACK.value,
                    status=StageStatus.UNSUPPORTED.value,
                    extension=name,
                    detail="扩展声明不支持回退，其运行期状态已保留待人工处理",
                )
            elif hooks.rollback is None:
                result = StageResult(
                    stage=Stage.ROLLBACK.value,
                    status=StageStatus.SKIPPED.value,
                    extension=name,
                    detail="扩展无可回退的暂存状态",
                )
            else:
                # 上下文中的 new 是候选快照（即将被丢弃的暂存配置）
                context = self._hook_context(order, name, candidate)
                try:
                    detail = await _maybe_await(hooks.rollback(order.changes[name], context))
                    result = StageResult(
                        stage=Stage.ROLLBACK.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail=str(detail) if detail else "回退完成",
                    )
                except Exception as e:
                    self.log.exception("变更单 %s 扩展 %s 回退失败", order.change_id, name)
                    recovery_needed = True
                    result = StageResult(
                        stage=Stage.ROLLBACK.value,
                        status=StageStatus.FAILED.value,
                        extension=name,
                        error=f"{type(e).__name__}: {e}",
                    )
            self._journal_stage(order, result)

        if recovery_needed:
            self._set_state(
                order,
                ChangeState.RECOVERY_REQUIRED,
                error="部分扩展不支持或未能完成回退，现场已完整保留，需人工介入",
            )
        else:
            self._set_state(order, ChangeState.ROLLED_BACK)

    # ------------------------------------------------------------------
    # 预检（内部）
    # ------------------------------------------------------------------

    def _run_preflight_checks(
        self, changes: dict[str, t.Any], candidate: ConfigSnapshot
    ) -> list[StageResult]:
        """执行全部预检项，返回真实结果列表（不短路，给出完整报告）。"""
        results: list[StageResult] = []
        results.extend(self._check_importable(changes))
        results.extend(self._check_traits(changes))
        results.extend(self._check_dependencies(changes, candidate))
        results.extend(self._check_conflicts(changes, candidate))
        return results

    def _check_importable(self, changes: dict[str, t.Any]) -> list[StageResult]:
        """启用未知扩展时，检查其模块可导入。"""
        results = []
        for name, change in changes.items():
            if change.get("enabled") is not True:
                continue
            if self._is_known_extension(name):
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail="扩展已注册",
                    )
                )
                continue
            try:
                spec = importlib.util.find_spec(name)
            except (ImportError, ValueError):
                spec = None
            if spec is None:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.FAILED.value,
                        extension=name,
                        error=f"扩展模块 {name!r} 不可导入",
                    )
                )
            else:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail="扩展模块可导入",
                    )
                )
        return results

    def _check_traits(self, changes: dict[str, t.Any]) -> list[StageResult]:
        """对已知 ExtensionApp 校验配置项是否为已声明的 trait。"""
        results = []
        for name, change in changes.items():
            if "config" not in change:
                continue
            app_class = self._extension_app_class(name)
            if app_class is None:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.SKIPPED.value,
                        extension=name,
                        detail="无法确定扩展 App 类，跳过配置项校验",
                    )
                )
                continue
            own = change["config"].get(app_class.__name__)
            if not isinstance(own, dict) or not own:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail="未包含扩展自身类的配置项",
                    )
                )
                continue
            unknown = sorted(set(own) - set(app_class.class_trait_names()))
            if unknown:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.FAILED.value,
                        extension=name,
                        error=f"扩展 {name} 不存在配置项：{', '.join(unknown)}",
                    )
                )
            else:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail="配置项校验通过",
                    )
                )
        return results

    def _check_dependencies(
        self, changes: dict[str, t.Any], candidate: ConfigSnapshot
    ) -> list[StageResult]:
        """检查启用集中的依赖关系在变更后仍然满足。"""
        enabled_map = self._effective_enabled_map(changes, candidate)
        results = []
        checked = 0
        for name in sorted(enabled_map):
            if not enabled_map[name]:
                continue
            requires = self._extension_metadata_list(name, "requires")
            if not requires:
                continue
            checked += 1
            missing = [dep for dep in requires if not enabled_map.get(dep)]
            if missing:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.FAILED.value,
                        extension=name,
                        error=f"扩展 {name} 依赖的扩展在变更后未启用：{', '.join(missing)}",
                    )
                )
            else:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail=f"依赖检查通过（{', '.join(requires)}）",
                    )
                )
        if not checked:
            results.append(
                StageResult(
                    stage=Stage.PREFLIGHT.value,
                    status=StageStatus.SKIPPED.value,
                    detail="启用集中没有扩展声明依赖",
                )
            )
        return results

    def _check_conflicts(
        self, changes: dict[str, t.Any], candidate: ConfigSnapshot
    ) -> list[StageResult]:
        """检查启用集中不存在相互声明冲突的扩展。"""
        enabled_map = self._effective_enabled_map(changes, candidate)
        enabled = sorted(name for name, on in enabled_map.items() if on)
        results = []
        checked = 0
        for name in enabled:
            conflicts = self._extension_metadata_list(name, "conflicts")
            if not conflicts:
                continue
            checked += 1
            present = [other for other in conflicts if enabled_map.get(other)]
            if present:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.FAILED.value,
                        extension=name,
                        error=f"扩展 {name} 与已启用的扩展冲突：{', '.join(present)}",
                    )
                )
            else:
                results.append(
                    StageResult(
                        stage=Stage.PREFLIGHT.value,
                        status=StageStatus.OK.value,
                        extension=name,
                        detail="冲突检查通过",
                    )
                )
        if not checked:
            results.append(
                StageResult(
                    stage=Stage.PREFLIGHT.value,
                    status=StageStatus.SKIPPED.value,
                    detail="启用集中没有扩展声明冲突",
                )
            )
        return results

    # ------------------------------------------------------------------
    # 持久化与运行时同步（内部）
    # ------------------------------------------------------------------

    def _persist_candidate(self, order: ChangeOrder, candidate: ConfigSnapshot) -> None:
        """把候选快照中被触及的部分写入配置文件。"""
        assert self.config_manager is not None
        for name, change in order.changes.items():
            entry = candidate.get_extension(name)
            if "enabled" in change:
                # enabled 为 None 时 recursive_update 会删除该键，回到未列出状态
                self.config_manager.update(
                    ENABLE_SECTION,
                    {"ServerApp": {"jpserver_extensions": {name: entry["enabled"]}}},
                )
            if "config" in change:
                section = entry["config_section"] or self._default_section(name)
                self.config_manager.set(section, entry["config"])

    def _apply_runtime(self, order: ChangeOrder, candidate: ConfigSnapshot) -> None:
        """把已提交配置同步到 ServerApp 运行时（同步块，事件循环内原子）。"""
        app = self.serverapp
        if app is None:
            return
        for name, change in order.changes.items():
            entry = candidate.get_extension(name)
            if "enabled" in change:
                enabled = bool(entry["enabled"])
                app.update_config(Config({"ServerApp": {"jpserver_extensions": {name: enabled}}}))
                if name in app.jpserver_extensions or enabled:
                    app.jpserver_extensions[name] = enabled
            if "config" in change:
                app.update_config(Config(entry["config"]))

    # ------------------------------------------------------------------
    # 辅助方法（内部）
    # ------------------------------------------------------------------

    def _set_state(
        self,
        order: ChangeOrder,
        state: ChangeState,
        error: str | None = None,
        recovered: bool = False,
    ) -> None:
        """迁移状态并写 journal。"""
        order.transition(state, error)
        if recovered:
            order.recovered = True
        self._journal.append(
            {
                "kind": "state",
                "change_id": order.change_id,
                "state": state.value,
                "error": order.error,
                "recovered": order.recovered,
            }
        )

    def _journal_stage(self, order: ChangeOrder, result: StageResult) -> None:
        """记录一条阶段结果并写 journal。"""
        order.record(result)
        self._journal.append(
            {"kind": "stage", "change_id": order.change_id, "result": result.to_dict()}
        )

    def _replay(self) -> None:
        """重放 journal，重建变更单记录与提交决定集合。"""
        for event in self._journal.load():
            change_id = event.get("change_id")
            kind = event.get("kind")
            if not change_id or not kind:
                continue
            if kind == "proposed":
                order = ChangeOrder(
                    change_id=change_id,
                    changes=event.get("changes") or {},
                    payload_hash=event.get("payload_hash", ""),
                )
                if event.get("ts"):
                    order.created_at = event["ts"]
                    order.updated_at = event["ts"]
                self._orders[change_id] = order
                continue
            order = self._orders.get(change_id)
            if order is None:
                continue
            if kind == "freeze":
                order.base_version = event.get("base_version", 0)
                order.candidate_version = event.get("candidate_version", 0)
                order.base_entries = event.get("base_entries") or {}
            elif kind == "stage":
                order.stages.append(StageResult.from_dict(event.get("result") or {}))
            elif kind == "state":
                state = ChangeState(event["state"])
                order.state = state
                order.error = event.get("error")
                if event.get("recovered"):
                    order.recovered = True
                if state in (ChangeState.COMMITTING, ChangeState.COMMITTED):
                    self._commit_decisions.add(change_id)
            if event.get("ts"):
                order.updated_at = event["ts"]

    def _hooks_for(self, name: str) -> ExtensionHooks:
        """解析扩展的两阶段提交钩子：注册表优先，其次 ExtensionApp 协议。"""
        if name in self._hooks:
            return self._hooks[name]
        app = self._extension_app(name)
        if app is None:
            return ExtensionHooks()
        hooks = ExtensionHooks(
            supports_rollback=bool(getattr(app, "supports_config_rollback", True))
        )
        for attr, field_name in (
            ("prepare_config_change", "prepare"),
            ("commit_config_change", "commit"),
            ("rollback_config_change", "rollback"),
        ):
            # 仅当扩展类覆写了 ExtensionApp 的默认空实现时才视为真实钩子
            owner = next((k for k in type(app).__mro__ if attr in k.__dict__), None)
            if owner is None or (
                owner.__name__ == "ExtensionApp"
                and owner.__module__ == "jupyter_server.extension.application"
            ):
                continue
            setattr(hooks, field_name, getattr(app, attr))
        return hooks

    def _extension_app(self, name: str) -> t.Any | None:
        """从扩展管理器取得扩展的 ExtensionApp 实例（如有）。"""
        manager = self.extension_manager
        if manager is None:
            return None
        package = manager.extensions.get(name)
        if package is None:
            return None
        for point in package.extension_points.values():
            if point.app is not None:
                return point.app
        return None

    def _extension_app_class(self, name: str) -> type | None:
        """取得扩展的 ExtensionApp 类（用于 trait 校验）。"""
        app = self._extension_app(name)
        return type(app) if app is not None else None

    def _is_known_extension(self, name: str) -> bool:
        """扩展是否已在扩展管理器中注册。"""
        return self.extension_manager is not None and name in self.extension_manager.extensions

    def _extension_metadata_list(self, name: str, key: str) -> list[str]:
        """读取扩展元数据中的列表字段（requires / conflicts）。"""
        values: list[str] = []
        if self.extension_manager is None:
            return values
        package = self.extension_manager.extensions.get(name)
        if package is None:
            return values
        for meta in package.metadata:
            entries = meta.get(key) or []
            values.extend(str(item) for item in entries)
        return values

    def _stable_order(self, changes: dict[str, t.Any]) -> list[str]:
        """计算准备的稳定顺序：依赖拓扑序，同层按名称字典序。"""
        names = sorted(changes)
        edges: dict[str, set[str]] = {name: set() for name in names}
        for name in names:
            for dep in self._extension_metadata_list(name, "requires"):
                if dep in edges and dep != name:
                    edges[name].add(dep)
        ordered: list[str] = []
        remaining = dict(edges)
        while remaining:
            ready = sorted(name for name, deps in remaining.items() if not deps & remaining.keys())
            if not ready:
                # 依赖环：退化为字典序，保证确定性
                self.log.warning("扩展依赖存在环，准备顺序退化为字典序：%s", sorted(remaining))
                ordered.extend(sorted(remaining))
                break
            ordered.extend(ready)
            for name in ready:
                remaining.pop(name)
        return ordered

    def _effective_enabled_map(
        self, changes: dict[str, t.Any], candidate: ConfigSnapshot
    ) -> dict[str, bool]:
        """计算变更后的有效启用集：实时启用标记叠加本变更单触及的扩展。"""
        assert self.config_manager is not None
        data = self.config_manager.get(ENABLE_SECTION)
        enabled = dict(data.get("ServerApp", {}).get("jpserver_extensions", {}))
        for name in changes:
            entry = candidate.get_extension(name)
            if entry["enabled"] is None:
                enabled.pop(name, None)
            else:
                enabled[name] = entry["enabled"]
        return enabled

    def _read_effective_entry(self, name: str, change: dict[str, t.Any]) -> dict[str, t.Any]:
        """读取扩展当前的有效配置（冻结现场用）。"""
        assert self.config_manager is not None
        section = change.get("config_section") or self._default_section(name)
        enabled_data = self.config_manager.get(ENABLE_SECTION)
        enabled = enabled_data.get("ServerApp", {}).get("jpserver_extensions", {}).get(name)
        return {
            "enabled": enabled,
            "config_section": section,
            "config": self.config_manager.get(section),
        }

    def _hook_context(
        self, order: ChangeOrder, name: str, candidate: ConfigSnapshot
    ) -> dict[str, t.Any]:
        """构造传递给扩展钩子的上下文。"""
        return {
            "change_id": order.change_id,
            "extension": name,
            "old": order.base_entries.get(name, {}),
            "new": candidate.get_extension(name),
            "serverapp": self.serverapp,
            "manager": self,
        }

    @staticmethod
    def _inverse_delta(base: dict[str, t.Any], current: dict[str, t.Any]) -> dict[str, t.Any]:
        """构造把 ``current`` 精确还原为 ``base`` 的递归合并增量。

        使得 ``recursive_update(current, delta)`` 的结果等于 ``base``：
        多出键显式置 None 删除，被改动的键写回基线值，字典递归处理。
        """
        delta: dict[str, t.Any] = {}
        for key in current:
            if key not in base:
                delta[key] = None
        for key, base_value in base.items():
            if key not in current:
                delta[key] = copy.deepcopy(base_value)
            elif isinstance(base_value, dict) and isinstance(current[key], dict):
                sub = ExtensionConfigTransactionManager._inverse_delta(base_value, current[key])
                if sub:
                    delta[key] = sub
            elif current[key] != base_value:
                delta[key] = copy.deepcopy(base_value)
        return delta

    @staticmethod
    def _default_section(name: str) -> str:
        """按扩展名推导默认的配置 section 名。"""
        normalized = re.sub(r"\W+", "_", name).strip("_")
        return f"jupyter_{normalized}_config"

    @staticmethod
    def _payload_hash(changes: dict[str, t.Any]) -> str:
        """计算变更内容的规范哈希（幂等键的内容部分）。"""
        canonical = json.dumps(
            {"changes": changes}, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _validate_change_id(change_id: str) -> None:
        """校验变更单号格式。"""
        if not isinstance(change_id, str) or not CHANGE_ID_PATTERN.match(change_id):
            msg = (
                f"变更单号 {change_id!r} 不合法：需以字母、数字或下划线开头，"
                "仅含字母、数字、下划线、点、冒号、连字符，最长 128 字符"
            )
            raise ValueError(msg)

    @staticmethod
    def _validate_changes(changes: dict[str, t.Any]) -> None:
        """校验变更内容的结构。"""
        if not isinstance(changes, dict) or not changes:
            msg = "变更单必须包含至少一个扩展的变更"
            raise ValueError(msg)
        for name, change in changes.items():
            if not isinstance(name, str) or not name:
                msg = f"扩展名 {name!r} 不合法"
                raise ValueError(msg)
            if not isinstance(change, dict):
                msg = f"扩展 {name} 的变更必须是字典"
                raise ValueError(msg)
            unknown = set(change) - KNOWN_CHANGE_KEYS
            if unknown:
                msg = f"扩展 {name} 的变更包含未知键：{', '.join(sorted(unknown))}"
                raise ValueError(msg)
            if "enabled" in change and not (
                isinstance(change["enabled"], bool) or change["enabled"] is None
            ):
                msg = f"扩展 {name} 的 enabled 必须是布尔值或 null"
                raise ValueError(msg)
            if "config" in change and not isinstance(change["config"], dict):
                msg = f"扩展 {name} 的 config 必须是字典"
                raise ValueError(msg)
            if "config_section" in change and not isinstance(change["config_section"], str):
                msg = f"扩展 {name} 的 config_section 必须是字符串"
                raise ValueError(msg)
            if "enabled" not in change and "config" not in change:
                msg = f"扩展 {name} 的变更必须至少包含 enabled 或 config 之一"
                raise ValueError(msg)
        # 变更内容必须可 JSON 序列化（将写入 journal 与配置文件）
        try:
            json.dumps(changes)
        except (TypeError, ValueError) as e:
            msg = f"变更内容必须是可 JSON 序列化的：{e}"
            raise ValueError(msg) from e
