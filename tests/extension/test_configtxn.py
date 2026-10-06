"""扩展配置变更事务（预检、分阶段启用、回退、崩溃恢复）的回归测试。"""

import asyncio
import json
import os

import pytest
from tornado.httpclient import HTTPClientError
from traitlets import Unicode

from jupyter_server.extension.application import ExtensionApp
from jupyter_server.extension.configtxn import (
    ChangeConflictError,
    ChangeInProgressError,
    ChangeNotFoundError,
    ChangeState,
    ExtensionConfigTransactionManager,
    ExtensionHooks,
    Stage,
    StageStatus,
)
from jupyter_server.extension.configtxn.store import ChangeJournal, ConfigSnapshot, SnapshotStore
from jupyter_server.services.config.manager import ConfigManager

from ..utils import expected_http_error

pytestmark = pytest.mark.usefixtures("jp_environ")


# ---------------------------------------------------------------------------
# 测试辅助
# ---------------------------------------------------------------------------


class FakeExtensionApp(ExtensionApp):
    """带已知 trait 的假扩展 App，用于预检的 trait 校验。"""

    name = "fakeapp"
    known_trait = Unicode("default", config=True)


class FakeExtensionPoint:
    def __init__(self, app=None):
        self.app = app


class FakeExtensionPackage:
    def __init__(self, metadata=None, app=None):
        self.metadata = metadata or []
        self.extension_points = {"fake": FakeExtensionPoint(app)}


class FakeExtensionManager:
    """模拟 ExtensionManager，提供扩展元数据与 App 实例。"""

    def __init__(self, extensions=None):
        self.extensions = extensions or {}


@pytest.fixture
def config_dir(tmp_path):
    return str(tmp_path / "serverconfig")


@pytest.fixture
def config_manager(config_dir):
    return ConfigManager(read_config_path=[config_dir], write_config_dir=config_dir)


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path / "txn-state")


@pytest.fixture
def make_manager(config_manager, state_dir):
    """按相同状态目录构造事务管理器（可重复调用以模拟进程重启）。"""

    def _make(extension_manager=None, serverapp=None):
        return ExtensionConfigTransactionManager(
            state_dir=state_dir,
            config_manager=config_manager,
            extension_manager=extension_manager,
            serverapp=serverapp,
        )

    return _make


@pytest.fixture
def manager(make_manager):
    return make_manager()


def make_hooks(name, calls, fail_prepare=False, supports_rollback=True):
    """构造记录调用序列的扩展钩子。"""

    async def prepare(change, context):
        calls.append(("prepare", name))
        if fail_prepare:
            raise RuntimeError(f"{name} 启动失败")

    async def commit(change, context):
        calls.append(("commit", name))

    async def rollback(change, context):
        calls.append(("rollback", name))

    return ExtensionHooks(
        prepare=prepare,
        commit=commit,
        rollback=rollback if supports_rollback else None,
        supports_rollback=supports_rollback,
    )


# ---------------------------------------------------------------------------
# 存储层
# ---------------------------------------------------------------------------


def test_journal_roundtrip_and_truncation(tmp_path):
    """journal 追加后可完整读回；末尾截断（崩溃现场）被容忍。"""
    journal = ChangeJournal(str(tmp_path / "journal.jsonl"))
    journal.append({"kind": "proposed", "change_id": "c1"})
    journal.append({"kind": "state", "change_id": "c1", "state": "validated"})
    # 模拟崩溃导致的半行写入
    with open(journal.path, "a", encoding="utf-8") as f:
        f.write('{"kind": "state", "change_id": "c1", "stat')
    events = journal.load()
    assert [e["kind"] for e in events] == ["proposed", "state"]


def test_snapshot_store_atomic(tmp_path):
    """快照写入原子可恢复；候选快照按变更单隔离。"""
    store = SnapshotStore(str(tmp_path / "state"))
    assert store.load_committed().version == 0
    snap = ConfigSnapshot(version=3, extensions={"a": {"enabled": True, "config": {}}})
    store.write_committed(snap)
    assert store.load_committed().version == 3
    store.write_candidate("chg-1", snap)
    assert store.read_candidate("chg-1").version == 3
    assert store.read_candidate("missing") is None


def test_snapshot_immutable_for_readers():
    """读者拿到的内容是深拷贝，无法污染快照本体。"""
    snap = ConfigSnapshot(version=1, extensions={"a": {"enabled": True, "config": {"x": 1}}})
    view = snap.extensions
    view["a"]["config"]["x"] = 999
    assert snap.get_extension("a")["config"]["x"] == 1


# ---------------------------------------------------------------------------
# 提交流水线
# ---------------------------------------------------------------------------


async def test_commit_happy_path(make_manager, config_manager):
    """冻结 → 预检 → 按序准备 → 共同提交，阶段结果完整可查。"""
    manager = make_manager(
        extension_manager=FakeExtensionManager(
            {"ext_alpha": FakeExtensionPackage(), "ext_beta": FakeExtensionPackage()}
        )
    )
    calls = []
    manager.register_hooks("ext_beta", make_hooks("ext_beta", calls))
    manager.register_hooks("ext_alpha", make_hooks("ext_alpha", calls))
    order = await manager.submit(
        "chg-1",
        {
            "ext_beta": {"enabled": True, "config": {"Beta": {"k": "v"}}},
            "ext_alpha": {"config": {"Alpha": {"n": 1}}},
        },
    )
    assert order.state == ChangeState.COMMITTED
    assert order.terminal
    # 阶段齐全：冻结、预检、准备、提交
    stages = [r.stage for r in order.stages]
    assert Stage.FREEZE.value in stages
    assert Stage.PREFLIGHT.value in stages
    assert Stage.PREPARE.value in stages
    assert Stage.COMMIT.value in stages
    # 准备按稳定顺序（字典序）执行，提交同序
    assert calls == [
        ("prepare", "ext_alpha"),
        ("prepare", "ext_beta"),
        ("commit", "ext_alpha"),
        ("commit", "ext_beta"),
    ]
    # 配置已落盘
    data = config_manager.get("jupyter_server_config")
    assert data["ServerApp"]["jpserver_extensions"]["ext_beta"] is True
    assert config_manager.get("jupyter_ext_beta_config") == {"Beta": {"k": "v"}}
    assert config_manager.get("jupyter_ext_alpha_config") == {"Alpha": {"n": 1}}
    # 已提交快照版本递增，读者可见新配置
    assert manager.current_snapshot().version == 1
    assert manager.get_extension_config("ext_beta")["config"] == {"Beta": {"k": "v"}}


async def test_prepare_runs_in_dependency_order(manager, make_manager):
    """准备顺序满足依赖拓扑：被依赖者先准备。"""
    ext_manager = FakeExtensionManager(
        {
            "ext_top": FakeExtensionPackage(metadata=[{"requires": ["ext_mid"]}]),
            "ext_mid": FakeExtensionPackage(metadata=[{"requires": ["ext_base"]}]),
            "ext_base": FakeExtensionPackage(),
        }
    )
    manager = make_manager(extension_manager=ext_manager)
    calls = []
    for name in ("ext_top", "ext_mid", "ext_base"):
        manager.register_hooks(name, make_hooks(name, calls))
    order = await manager.submit(
        "chg-deps",
        {
            "ext_top": {"config": {"T": {}}},
            "ext_base": {"config": {"B": {}}},
            "ext_mid": {"config": {"M": {}}},
        },
    )
    assert order.state == ChangeState.COMMITTED
    prepares = [name for kind, name in calls if kind == "prepare"]
    assert prepares == ["ext_base", "ext_mid", "ext_top"]


async def test_commit_updates_runtime_serverapp_config(manager):
    """提交后 ServerApp 运行时配置同步更新。"""

    class FakeServerApp:
        def __init__(self):
            from traitlets.config import Config

            self.config = Config()
            self.jpserver_extensions = {}

        def update_config(self, config):
            self.config.merge(config)

    app = FakeServerApp()
    manager.serverapp = app
    order = await manager.submit(
        "chg-rt",
        {"jupyter_client": {"enabled": True, "config": {"Kernel": {"timeout": 30}}}},
    )
    assert order.state == ChangeState.COMMITTED
    assert app.config["ServerApp"]["jpserver_extensions"]["jupyter_client"] is True
    assert app.config["Kernel"]["timeout"] == 30
    assert app.jpserver_extensions["jupyter_client"] is True


async def test_extension_app_hook_protocol(make_manager):
    """ExtensionApp 覆写钩子协议后被事务流水线发现并调用。"""
    calls = []

    class HookedApp(FakeExtensionApp):
        name = "hookedapp"

        async def prepare_config_change(self, change, context):
            calls.append(("prepare", context["change_id"]))
            return "已暂存"

        async def commit_config_change(self, change, context):
            calls.append(("commit", context["change_id"]))

        async def rollback_config_change(self, change, context):
            calls.append(("rollback", context["change_id"]))

    class NoRollbackApp(FakeExtensionApp):
        name = "norbapp"
        supports_config_rollback = False

    ext_manager = FakeExtensionManager(
        {
            "ext_hooked": FakeExtensionPackage(app=HookedApp()),
            "ext_plain": FakeExtensionPackage(app=FakeExtensionApp()),
            "ext_norb": FakeExtensionPackage(app=NoRollbackApp()),
        }
    )
    manager = make_manager(extension_manager=ext_manager)
    order = await manager.submit("chg-hooks", {"ext_hooked": {"config": {"H": {"a": 1}}}})
    assert order.state == ChangeState.COMMITTED
    assert calls == [("prepare", "chg-hooks"), ("commit", "chg-hooks")]
    # 钩子返回的 detail 被记录为真实阶段结果
    prepare_result = next(r for r in order.stages if r.stage == Stage.PREPARE.value)
    assert prepare_result.detail == "已暂存"

    # 未覆写钩子的扩展：默认暂存，提交阶段跳过
    order = await manager.submit("chg-plain", {"ext_plain": {"config": {"P": {"b": 2}}}})
    assert order.state == ChangeState.COMMITTED
    commit_result = next(
        r for r in order.stages if r.stage == Stage.COMMIT.value and r.extension == "ext_plain"
    )
    assert commit_result.status == StageStatus.SKIPPED.value

    # 声明不支持回退的 ExtensionApp 被识别
    hooks = manager._hooks_for("ext_norb")
    assert hooks.supports_rollback is False
    assert hooks.prepare is None  # 基类默认空实现不视为真实钩子


# ---------------------------------------------------------------------------
# 预检
# ---------------------------------------------------------------------------


async def test_preflight_rejects_unimportable_extension(manager):
    """启用不可导入的扩展 → 预检拒绝，不产生任何副作用。"""
    order = await manager.submit("chg-bad", {"no_such_module_xyz": {"enabled": True}})
    assert order.state == ChangeState.REJECTED
    failed = [r for r in order.stages if r.status == StageStatus.FAILED.value]
    assert failed and "不可导入" in failed[0].error
    # 未冻结出任何生效变更
    assert manager.current_snapshot().version == 0


async def test_preflight_rejects_dependency_violation(make_manager):
    """被依赖扩展在变更后未启用 → 预检拒绝。"""
    ext_manager = FakeExtensionManager(
        {
            "ext_consumer": FakeExtensionPackage(metadata=[{"requires": ["ext_base"]}]),
            "ext_base": FakeExtensionPackage(),
        }
    )
    manager = make_manager(extension_manager=ext_manager)
    # 先启用两者
    order = await manager.submit(
        "chg-setup", {"ext_consumer": {"enabled": True}, "ext_base": {"enabled": True}}
    )
    assert order.state == ChangeState.COMMITTED
    # 再单独禁用被依赖者 → 拒绝
    order = await manager.submit("chg-violate", {"ext_base": {"enabled": False}})
    assert order.state == ChangeState.REJECTED
    assert "ext_base" in (order.error or "")


async def test_preflight_rejects_conflicting_extensions(make_manager):
    """相互声明冲突的扩展不能同时启用。"""
    ext_manager = FakeExtensionManager(
        {
            "ext_x": FakeExtensionPackage(metadata=[{"conflicts": ["ext_y"]}]),
            "ext_y": FakeExtensionPackage(),
        }
    )
    manager = make_manager(extension_manager=ext_manager)
    order = await manager.submit(
        "chg-conflict", {"ext_x": {"enabled": True}, "ext_y": {"enabled": True}}
    )
    assert order.state == ChangeState.REJECTED
    assert "冲突" in (order.error or "")


async def test_preflight_rejects_unknown_trait(make_manager):
    """配置项不是扩展已声明的 trait → 预检拒绝；合法 trait 则通过。"""
    ext_manager = FakeExtensionManager({"ext_fake": FakeExtensionPackage(app=FakeExtensionApp())})
    manager = make_manager(extension_manager=ext_manager)
    order = await manager.submit(
        "chg-trait-2",
        {"ext_fake": {"config": {"FakeExtensionApp": {"no_such_trait": 1}}}},
    )
    assert order.state == ChangeState.REJECTED
    assert "no_such_trait" in (order.error or "")
    order = await manager.submit(
        "chg-trait-3",
        {"ext_fake": {"config": {"FakeExtensionApp": {"known_trait": "ok"}}}},
    )
    assert order.state == ChangeState.COMMITTED


@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"ext": "not-a-dict"},
        {"ext": {"enabled": "yes"}},
        {"ext": {"config": [1, 2]}},
        {"ext": {"unknown_key": 1}},
        {"ext": {"config_section": 42}},
        {"ext": {}},
    ],
)
async def test_submit_rejects_invalid_structure(manager, changes):
    """结构非法的变更内容在受理时拒绝（ValueError）。"""
    with pytest.raises(ValueError):
        await manager.submit("chg-invalid", changes)


async def test_submit_rejects_invalid_change_id(manager):
    with pytest.raises(ValueError):
        await manager.submit("bad/id", {"ext": {"enabled": True}})
    with pytest.raises(ValueError):
        await manager.submit("", {"ext": {"enabled": True}})


async def test_dry_run_preflight_has_no_side_effects(manager, config_manager):
    """dry-run 只冻结与预检，不落盘、不推进状态。"""
    report = manager.preflight(
        {"jupyter_client": {"enabled": True}, "no_such_module_xyz": {"enabled": True}}
    )
    assert report["ok"] is False
    assert any(c["status"] == "failed" for c in report["checks"])
    assert "candidate" in report
    # 无任何持久化痕迹
    assert manager.list_orders() == []
    assert config_manager.get("jupyter_server_config") == {}
    assert not os.path.exists(os.path.join(manager.state_dir, "journal.jsonl"))


# ---------------------------------------------------------------------------
# 回退
# ---------------------------------------------------------------------------


async def test_prepare_failure_rolls_back_in_reverse_order(manager, config_manager):
    """任一扩展准备失败 → 已准备者按逆序回退，配置文件零污染。"""
    calls = []
    manager.register_hooks("ext_a", make_hooks("ext_a", calls))
    manager.register_hooks("ext_b", make_hooks("ext_b", calls))
    manager.register_hooks("ext_z_bad", make_hooks("ext_z_bad", calls, fail_prepare=True))
    order = await manager.submit(
        "chg-rb",
        {
            "ext_b": {"config": {"B": {"y": 2}}},
            "ext_a": {"config": {"A": {"x": 1}}},
            "ext_z_bad": {"config": {"Z": {"w": 0}}},
        },
    )
    assert order.state == ChangeState.ROLLED_BACK
    # 无人提交；回退按准备的逆序（失败者自身也收到回退，防御半成品状态）
    assert [c for c in calls if c[0] == "commit"] == []
    assert [c for c in calls if c[0] == "rollback"] == [
        ("rollback", "ext_z_bad"),
        ("rollback", "ext_b"),
        ("rollback", "ext_a"),
    ]
    # 配置文件与已提交快照均未受影响
    assert config_manager.get("jupyter_ext_a_config") == {}
    assert manager.current_snapshot().version == 0
    # 失败原因记录在案
    failed = [r for r in order.stages if r.status == StageStatus.FAILED.value]
    assert failed and "启动失败" in failed[0].error


async def test_rollback_unsupported_preserves_state(manager, state_dir):
    """扩展不支持回退 → recovery_required，现场完整保留可查。"""
    calls = []
    manager.register_hooks("aaa_norb", make_hooks("aaa_norb", calls, supports_rollback=False))
    manager.register_hooks("zzz_bad", make_hooks("zzz_bad", calls, fail_prepare=True))
    order = await manager.submit(
        "chg-norb",
        {
            "aaa_norb": {"config": {"N": {"v": 1}}},
            "zzz_bad": {"config": {"Z": {"w": 1}}},
        },
    )
    assert order.state == ChangeState.RECOVERY_REQUIRED
    unsupported = [r for r in order.stages if r.status == StageStatus.UNSUPPORTED.value]
    assert [r.extension for r in unsupported] == ["aaa_norb"]
    # 现场保留：journal 与候选快照可用于事后恢复与审计
    assert os.path.exists(os.path.join(state_dir, "journal.jsonl"))
    assert os.path.exists(os.path.join(state_dir, "candidates", "chg-norb.json"))
    # 记录可查询
    fetched = manager.get_order("chg-norb")
    assert fetched is order


async def test_rollback_of_committed_order(manager, config_manager):
    """已提交变更单可回退：反向变更单走完整事务并精确恢复基线。"""
    order = await manager.submit(
        "chg-1",
        {
            "jupyter_client": {
                "enabled": True,
                "config": {"K": {"a": 1, "nested": {"x": 1, "y": 2}}},
            }
        },
    )
    assert order.state == ChangeState.COMMITTED
    rollback = await manager.rollback_committed("chg-1")
    assert rollback.change_id == "chg-1:rollback"
    assert rollback.state == ChangeState.COMMITTED
    # 精确恢复到冻结时的基线
    assert config_manager.get("jupyter_server_config") == {}
    assert config_manager.get("jupyter_jupyter_client_config") == {}
    # 幂等：再次回退返回同一记录，不重复执行
    again = await manager.rollback_committed("chg-1")
    assert again is rollback
    # 未提交的变更单不可回退
    rejected = await manager.submit("chg-rej", {"no_such_module_xyz": {"enabled": True}})
    assert rejected.state == ChangeState.REJECTED
    with pytest.raises(ChangeConflictError):
        await manager.rollback_committed("chg-rej")
    with pytest.raises(ChangeNotFoundError):
        await manager.rollback_committed("no-such-order")


# ---------------------------------------------------------------------------
# 幂等与并发
# ---------------------------------------------------------------------------


async def test_idempotent_replay_and_payload_conflict(manager):
    """同单号同内容 → 幂等重放；同单号不同内容 → 冲突。"""
    calls = []
    manager.register_hooks("ext_a", make_hooks("ext_a", calls))
    changes = {"ext_a": {"config": {"A": {"x": 1}}}}
    order = await manager.submit("chg-1", changes)
    replayed = await manager.submit("chg-1", changes)
    assert replayed is order
    # 钩子只执行过一次
    assert calls == [("prepare", "ext_a"), ("commit", "ext_a")]
    with pytest.raises(ChangeConflictError):
        await manager.submit("chg-1", {"ext_a": {"config": {"A": {"x": 2}}}})


async def test_concurrent_change_rejected_and_readers_see_old_config(manager):
    """准备期间：新变更单被拒（409 语义），读者继续看到旧配置。"""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_prepare(change, context):
        entered.set()
        await release.wait()

    manager.register_hooks("ext_slow", ExtensionHooks(prepare=slow_prepare))
    task = asyncio.ensure_future(manager.submit("chg-1", {"ext_slow": {"config": {"S": {"v": 1}}}}))
    await entered.wait()
    assert manager.transaction_active
    assert manager.active_change_id == "chg-1"
    # 准备期间读者仍看到旧的已提交快照
    with manager.read_snapshot() as snapshot:
        assert snapshot.version == 0
        assert snapshot.extensions == {}
        assert manager.active_readers == {"0": 1}
    assert manager.active_readers == {}
    # 并发变更单被拒
    with pytest.raises(ChangeInProgressError):
        await manager.submit("chg-2", {"ext_slow": {"config": {"S": {"v": 2}}}})
    # 执行中的同号提交也不是幂等重放，而是提示正在执行
    with pytest.raises(ChangeInProgressError):
        await manager.submit("chg-1", {"ext_slow": {"config": {"S": {"v": 1}}}})
    release.set()
    order = await task
    assert order.state == ChangeState.COMMITTED
    assert not manager.transaction_active
    # 提交后新读者看到新快照
    with manager.read_snapshot() as snapshot:
        assert snapshot.version == 1
    assert manager.active_readers == {}


async def test_pinned_snapshot_isolated_from_commit(manager):
    """提交前固定的快照在提交后保持旧内容（读隔离）。"""
    with manager.read_snapshot() as pinned:
        await manager.submit("chg-1", {"ext_a": {"config": {"A": {"x": 1}}}})
        assert manager.current_snapshot().version == 1
        assert pinned.version == 0
        assert pinned.extensions == {}


async def test_unexpected_error_marks_recovery_required(manager):
    """流水线内部的未预期异常：变更单被标记为待恢复，异常继续上抛。"""

    def broken_write(change_id, snapshot):
        raise OSError("disk full")

    manager._snapshots.write_candidate = broken_write
    with pytest.raises(OSError):
        await manager.submit("chg-ioerr", {"ext_a": {"config": {"A": {"x": 1}}}})
    order = manager.get_order("chg-ioerr")
    assert order.state == ChangeState.RECOVERY_REQUIRED
    assert "disk full" in (order.error or "")


# ---------------------------------------------------------------------------
# 崩溃恢复
# ---------------------------------------------------------------------------


class SimulatedCrash(BaseException):
    """模拟进程崩溃的异常（不被事务流水线捕获）。"""


async def test_crash_during_prepare_recovered_as_rolled_back(make_manager, config_manager):
    """准备阶段崩溃：恢复后变更单标记为已回退，配置文件零污染。"""
    manager = make_manager()

    async def crashing_prepare(change, context):
        raise SimulatedCrash

    manager.register_hooks("ext_x", ExtensionHooks(prepare=crashing_prepare))
    with pytest.raises(SimulatedCrash):
        await manager.submit("chg-9", {"ext_x": {"config": {"X": {"a": 1}}}})
    assert manager.get_order("chg-9").state == ChangeState.PREPARING

    # 模拟进程重启：同一状态目录上的新管理器
    restarted = make_manager()
    recovered = restarted.recover()
    assert [o.change_id for o in recovered] == ["chg-9"]
    order = restarted.get_order("chg-9")
    assert order.state == ChangeState.ROLLED_BACK
    assert order.recovered
    # 配置文件从未被该变更单修改
    assert config_manager.get("jupyter_ext_x_config") == {}
    # 恢复动作本身也留下了真实的阶段记录
    recovery = [r for r in order.stages if r.stage == Stage.RECOVERY.value]
    assert recovery and recovery[0].status == StageStatus.OK.value


async def test_crash_during_commit_recovered_to_committed(make_manager, config_manager, config_dir):
    """提交决定落盘后崩溃（配置文件写了一半）：恢复后幂等完成提交。"""
    manager = make_manager()
    original_set = config_manager.write_config_manager.set
    calls = {"n": 0}

    def crashing_set(section_name, data):
        calls["n"] += 1
        if calls["n"] == 1:
            # 第一个文件写成功后崩溃，第二个文件未写
            original_set(section_name, data)
            raise SimulatedCrash
        return original_set(section_name, data)

    config_manager.write_config_manager.set = crashing_set
    with pytest.raises(SimulatedCrash):
        await manager.submit(
            "chg-10",
            {"jupyter_client": {"enabled": True, "config": {"P": {"v": 1}}}},
        )
    assert manager.get_order("chg-10").state == ChangeState.COMMITTING
    # 崩溃现场：启用标记已写入，扩展配置 section 未写
    assert "jupyter_client" in config_manager.get("jupyter_server_config").get("ServerApp", {}).get(
        "jpserver_extensions", {}
    )
    assert config_manager.get("jupyter_jupyter_client_config") == {}

    # 进程重启：恢复应完成提交，两个文件收敛到候选快照
    config_manager.write_config_manager.set = original_set
    restarted = make_manager()
    restarted.recover()
    order = restarted.get_order("chg-10")
    assert order.state == ChangeState.COMMITTED
    assert order.recovered
    assert config_manager.get("jupyter_jupyter_client_config") == {"P": {"v": 1}}
    assert restarted.current_snapshot().version == 1
    # 恢复后重复提交同一变更单 → 幂等重放
    again = await restarted.submit(
        "chg-10", {"jupyter_client": {"enabled": True, "config": {"P": {"v": 1}}}}
    )
    assert again is order


async def test_recovery_failure_preserves_state(make_manager, config_manager):
    """恢复过程本身失败：变更单标记为待恢复，不阻塞启动，现场保留。"""
    manager = make_manager()

    class Crash(BaseException):
        pass

    original_set = config_manager.write_config_manager.set

    def crashing_set(section_name, data):
        original_set(section_name, data)
        raise Crash

    config_manager.write_config_manager.set = crashing_set
    with pytest.raises(Crash):
        await manager.submit("chg-11", {"jupyter_client": {"enabled": True}})
    assert manager.get_order("chg-11").state == ChangeState.COMMITTING

    # 恢复时落盘仍然失败 → 标记 recovery_required 而不是让启动崩溃
    def still_broken(section_name, data):
        raise OSError("disk still full")

    config_manager.write_config_manager.set = still_broken
    restarted = make_manager()
    recovered = restarted.recover()  # 不抛异常
    assert [o.change_id for o in recovered] == ["chg-11"]
    order = restarted.get_order("chg-11")
    assert order.state == ChangeState.RECOVERY_REQUIRED
    assert order.recovered
    assert "disk still full" in (order.error or "")


async def test_journal_replay_preserves_stage_results(make_manager):
    """进程重启后，每个阶段的真实结果仍可查询。"""
    manager = make_manager()
    calls = []
    manager.register_hooks("ext_a", make_hooks("ext_a", calls))
    order = await manager.submit("chg-1", {"ext_a": {"config": {"A": {"x": 1}}}})
    assert order.state == ChangeState.COMMITTED

    restarted = make_manager()
    replayed = restarted.get_order("chg-1")
    assert replayed.state == ChangeState.COMMITTED
    assert replayed.payload_hash == order.payload_hash
    assert replayed.candidate_version == 1
    stages = {(r.stage, r.extension, r.status) for r in replayed.stages}
    assert (Stage.FREEZE.value, None, StageStatus.OK.value) in stages
    assert (Stage.PREPARE.value, "ext_a", StageStatus.OK.value) in stages
    assert (Stage.COMMIT.value, "ext_a", StageStatus.OK.value) in stages
    # 基线条目也在（回退已提交变更单依赖它）
    assert "ext_a" in replayed.base_entries


# ---------------------------------------------------------------------------
# HTTP 运维接口
# ---------------------------------------------------------------------------


@pytest.fixture
def txn_manager(jp_serverapp):
    return jp_serverapp.extension_config_txn_manager


async def test_serverapp_wires_txn_manager(jp_serverapp):
    """ServerApp 初始化后事务管理器可用，且已注入 web 设置。"""
    manager = jp_serverapp.extension_config_txn_manager
    assert isinstance(manager, ExtensionConfigTransactionManager)
    assert jp_serverapp.web_app.settings["extension_config_txn_manager"] is manager
    # 空状态下恢复是 no-op
    assert manager.list_orders() == []


async def test_api_submit_and_query(jp_fetch, txn_manager, jp_serverapp):
    """提交变更单后可查询每个阶段的真实结果。"""
    payload = {
        "change_id": "chg-api-1",
        "changes": {
            "jupyter_client": {"enabled": True, "config": {"K": {"timeout": 5}}},
            "ext_alpha": {"config": {"Alpha": {"n": 1}}},
        },
    }
    r = await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    assert r.code == 201
    record = json.loads(r.body)
    assert record["state"] == "committed"
    assert record["terminal"] is True
    stages = {(s["stage"], s["extension"], s["status"]) for s in record["stages"]}
    assert ("freeze", None, "ok") in stages
    assert ("prepare", "ext_alpha", "ok") in stages

    # 列表接口：快照版本与变更单集合
    r = await jp_fetch("api", "extension-changes")
    listing = json.loads(r.body)
    assert listing["committed_version"] == 1
    assert listing["active_change_id"] is None
    assert [o["change_id"] for o in listing["orders"]] == ["chg-api-1"]

    # 详情接口
    r = await jp_fetch("api", "extension-changes", "chg-api-1")
    detail = json.loads(r.body)
    assert detail["change_id"] == "chg-api-1"
    assert detail["base_version"] == 0
    assert detail["candidate_version"] == 1

    # 运行时配置已生效
    assert jp_serverapp.config["ServerApp"]["jpserver_extensions"]["jupyter_client"] is True
    assert jp_serverapp.config["K"]["timeout"] == 5


async def test_api_idempotent_replay_and_conflict(jp_fetch):
    """重复提交同一变更单：同内容幂等返回，不同内容 409。"""
    payload = {"change_id": "chg-api-2", "changes": {"ext_a": {"config": {"A": {"x": 1}}}}}
    r = await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    assert r.code == 201
    r = await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    assert r.code == 200  # 幂等重放
    conflicting = {"change_id": "chg-api-2", "changes": {"ext_a": {"config": {"A": {"x": 2}}}}}
    with pytest.raises(HTTPClientError) as e:
        await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(conflicting))
    assert expected_http_error(e, 409)


async def test_api_rejected_order_returns_record(jp_fetch):
    """预检未通过：422，响应体带有完整的阶段结果。"""
    payload = {"change_id": "chg-api-3", "changes": {"no_such_module_xyz": {"enabled": True}}}
    with pytest.raises(HTTPClientError) as e:
        await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    assert expected_http_error(e, 422)
    record = json.loads(e.value.response.body)
    assert record["state"] == "rejected"
    assert any(s["status"] == "failed" for s in record["stages"])


async def test_api_dry_run(jp_fetch, txn_manager):
    """dry_run 仅返回预检报告，不产生变更单。"""
    payload = {
        "dry_run": True,
        "changes": {"jupyter_client": {"enabled": True}, "no_such_module_xyz": {"enabled": True}},
    }
    r = await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    report = json.loads(r.body)
    assert report["ok"] is False
    assert txn_manager.list_orders() == []


async def test_api_rollback_endpoint(jp_fetch, txn_manager):
    """回退接口：已提交变更单恢复基线，且幂等。"""
    payload = {
        "change_id": "chg-api-4",
        "changes": {"jupyter_client": {"enabled": True, "config": {"K": {"v": 1}}}},
    }
    r = await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    assert r.code == 201
    r = await jp_fetch("api", "extension-changes", "chg-api-4", "rollback", method="POST", body="")
    assert r.code == 201
    record = json.loads(r.body)
    assert record["change_id"] == "chg-api-4:rollback"
    assert record["state"] == "committed"
    # 再次回退 → 幂等
    r = await jp_fetch("api", "extension-changes", "chg-api-4", "rollback", method="POST", body="")
    assert r.code == 200
    # 未提交的变更单不可回退 → 409；不存在 → 404
    rejected = {"change_id": "chg-api-rej", "changes": {"no_such_module_xyz": {"enabled": True}}}
    with pytest.raises(HTTPClientError):
        await jp_fetch("api", "extension-changes", method="POST", body=json.dumps(rejected))
    with pytest.raises(HTTPClientError) as e:
        await jp_fetch(
            "api", "extension-changes", "chg-api-rej", "rollback", method="POST", body=""
        )
    assert expected_http_error(e, 409)
    with pytest.raises(HTTPClientError) as e:
        await jp_fetch("api", "extension-changes", "no-such", "rollback", method="POST", body="")
    assert expected_http_error(e, 404)


async def test_api_get_unknown_order(jp_fetch):
    with pytest.raises(HTTPClientError) as e:
        await jp_fetch("api", "extension-changes", "no-such-order")
    assert expected_http_error(e, 404)


async def test_api_direct_config_write_blocked_during_transaction(jp_fetch, txn_manager):
    """变更单执行期间，绕过事务的直写被 409 拒绝；读请求不受影响。"""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_prepare(change, context):
        entered.set()
        await release.wait()

    txn_manager.register_hooks("ext_slow", ExtensionHooks(prepare=slow_prepare))
    payload = {"change_id": "chg-api-5", "changes": {"ext_slow": {"config": {"S": {"v": 1}}}}}
    submit_task = asyncio.ensure_future(
        jp_fetch("api", "extension-changes", method="POST", body=json.dumps(payload))
    )
    await entered.wait()
    try:
        # 准备期间：直写被拒
        with pytest.raises(HTTPClientError) as e:
            await jp_fetch(
                "api",
                "config",
                "jupyter_server_config",
                method="PATCH",
                body=json.dumps({"ServerApp": {"jpserver_extensions": {"x": True}}}),
            )
        assert expected_http_error(e, 409)
        # 读请求继续可用（且看到的是旧配置）
        r = await jp_fetch("api", "config", "jupyter_server_config")
        assert r.code == 200
        # 变更单状态可查
        r = await jp_fetch("api", "extension-changes")
        assert json.loads(r.body)["active_change_id"] == "chg-api-5"
    finally:
        release.set()
    r = await submit_task
    assert r.code == 201
    # 事务结束后直写恢复可用
    r = await jp_fetch(
        "api",
        "config",
        "jupyter_server_config",
        method="PATCH",
        body=json.dumps({"ServerApp": {"jpserver_extensions": {"x": True}}}),
    )
    assert r.code == 200
