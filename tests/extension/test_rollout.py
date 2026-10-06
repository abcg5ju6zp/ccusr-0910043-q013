"""扩展配置事务化变更(预检、分阶段启用与回退)的测试。"""

from __future__ import annotations

import json
import os
import threading

import pytest
from traitlets import Int, Unicode
from traitlets.config import Configurable

from jupyter_server.extension.rollout import (
    ChangeOrderConflict,
    ChangeStage,
    ConfigSnapshot,
    ExtensionConfigChange,
    ExtensionConfigRolloutCoordinator,
    InvalidStageTransition,
    RolloutError,
    TraitletConfigAdapter,
    UnknownChangeOrder,
)

# 单元测试使用 ServerApp 的环境,避免污染用户配置目录。
pytestmark = pytest.mark.usefixtures("jp_environ")


# -----------------------------------------------------------------------------
# 测试用具
# -----------------------------------------------------------------------------


class FakeParticipant:
    """记录调用轨迹、可注入失败的配置参与者。"""

    def __init__(
        self,
        name,
        *,
        supports_rollback=True,
        dependencies=(),
        conflicts=(),
        fail_at=None,
        problems=(),
    ):
        self.name = name
        self.supports_rollback = supports_rollback
        self.dependencies = dependencies
        self.conflicts = conflicts
        self.fail_at = fail_at
        self.problems = list(problems)
        self.calls: list[tuple] = []
        self.active: dict = {}

    def validate(self, candidate):
        self.calls.append(("validate", dict(candidate)))
        return list(self.problems)

    def prepare(self, candidate):
        self.calls.append(("prepare", dict(candidate)))
        if self.fail_at == "prepare":
            raise RuntimeError("prepare boom")
        self._staged = dict(candidate)

    def commit(self):
        self.calls.append(("commit",))
        if self.fail_at == "commit":
            raise RuntimeError("commit boom")
        self.active = self._staged

    def rollback(self):
        self.calls.append(("rollback",))
        if self.fail_at == "rollback":
            raise RuntimeError("rollback boom")
        self.active = {}


def calls_of(participant, action):
    """取某参与者某类调用的次数。"""
    return sum(1 for call in participant.calls if call[0] == action)


@pytest.fixture
def participants():
    return {
        "ext_a": FakeParticipant("ext_a"),
        "ext_b": FakeParticipant("ext_b"),
        "ext_c": FakeParticipant("ext_c"),
    }


@pytest.fixture
def coordinator(tmp_path, participants):
    return ExtensionConfigRolloutCoordinator(
        participants=participants.values(),
        state_dir=str(tmp_path / "rollout"),
    )


# -----------------------------------------------------------------------------
# 候选快照与版本化存储
# -----------------------------------------------------------------------------


def test_snapshot_is_frozen_for_holders():
    snap = ConfigSnapshot(version=0, configs={"ext_a": {"x": 1}})
    got = snap.config_for("ext_a")
    got["x"] = 999
    assert snap.config_for("ext_a") == {"x": 1}


def test_snapshot_merged_bumps_version_and_keeps_base():
    base = ConfigSnapshot(version=3, configs={"ext_a": {"x": 1, "nested": {"y": 2}}})
    merged = base.merged([ExtensionConfigChange("ext_a", {"nested": {"z": 3}})])
    assert merged.version == 4
    assert merged.config_for("ext_a") == {"x": 1, "nested": {"y": 2, "z": 3}}
    # 原快照不被修改
    assert base.config_for("ext_a") == {"x": 1, "nested": {"y": 2}}


def test_snapshot_freezes_runtime_objects():
    class NotJson:
        pass

    snap = ConfigSnapshot(version=0, configs={"ext_a": {"obj": NotJson()}})
    assert isinstance(snap.config_for("ext_a")["obj"], str)


def test_requests_during_prepare_keep_old_config(tmp_path):
    """prepare 期间到达的请求继续看到旧配置;commit 后新请求看到新配置。"""
    entered = threading.Event()
    release = threading.Event()

    class BlockingParticipant(FakeParticipant):
        def prepare(self, candidate):
            entered.set()
            assert release.wait(30)
            super().prepare(candidate)

    blocker = BlockingParticipant("ext_x")
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[blocker], state_dir=str(tmp_path / "rollout")
    )
    coordinator.seed_base_configs({"ext_x": {"k": "old"}})

    outcome = {}

    def run_submit():
        outcome["record"] = coordinator.submit("CHG-BLOCK", {"ext_x": {"k": "new"}})

    thread = threading.Thread(target=run_submit)
    thread.start()
    try:
        assert entered.wait(30), "参与者应已进入 prepare"
        # prepare 进行中和准备完成后、提交前,读到的都是旧配置
        view_during_prepare = coordinator.store.acquire_view()
        assert view_during_prepare.config_for("ext_x") == {"k": "old"}
    finally:
        release.set()
    thread.join(30)
    assert outcome["record"].stage is ChangeStage.COMMITTED
    # 已持有旧视图的请求不受提交影响
    assert view_during_prepare.config_for("ext_x") == {"k": "old"}
    # 新请求看到新配置
    assert coordinator.current_config("ext_x") == {"k": "new"}


# -----------------------------------------------------------------------------
# 预检
# -----------------------------------------------------------------------------


def test_precheck_rejects_unknown_extension(coordinator, participants):
    record = coordinator.submit("CHG-1", {"ghost_ext": {"x": 1}})
    assert record.stage is ChangeStage.REJECTED
    assert any("ghost_ext" in problem for problem in record.precheck_problems)
    # 预检失败不触碰任何参与者
    for participant in participants.values():
        assert calls_of(participant, "prepare") == 0


def test_precheck_rejects_duplicate_extension(coordinator):
    record = coordinator.submit(
        "CHG-2",
        [
            ExtensionConfigChange("ext_a", {"x": 1}),
            ExtensionConfigChange("ext_a", {"x": 2}),
        ],
    )
    assert record.stage is ChangeStage.REJECTED
    assert any("ext_a" in problem for problem in record.precheck_problems)


def test_precheck_rejects_missing_dependency(tmp_path):
    dependent = FakeParticipant("ext_dep", dependencies=("ghost_base",))
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[dependent], state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit("CHG-3", {"ext_dep": {"x": 1}})
    assert record.stage is ChangeStage.REJECTED
    assert any("ghost_base" in problem for problem in record.precheck_problems)


def test_precheck_rejects_conflicting_pair(tmp_path):
    left = FakeParticipant("ext_left", conflicts=("ext_right",))
    right = FakeParticipant("ext_right")
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[left, right], state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit("CHG-4", {"ext_left": {"x": 1}, "ext_right": {"y": 2}})
    assert record.stage is ChangeStage.REJECTED
    assert any("互斥" in problem for problem in record.precheck_problems)


def test_precheck_collects_validation_problems(tmp_path):
    picky = FakeParticipant("ext_picky", problems=["配置项不合法"])
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[picky], state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit("CHG-5", {"ext_picky": {"x": 1}})
    assert record.stage is ChangeStage.REJECTED
    assert "配置项不合法" in record.precheck_problems


def test_precheck_warns_about_non_rollbackable(tmp_path):
    rigid = FakeParticipant("ext_rigid", supports_rollback=False)
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[rigid], state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit("CHG-6", {"ext_rigid": {"x": 1}})
    assert record.stage is ChangeStage.COMMITTED
    assert any("不支持回退" in warning for warning in record.precheck_warnings)


# -----------------------------------------------------------------------------
# 分阶段启用与共同提交
# -----------------------------------------------------------------------------


def test_prepare_follows_stable_order_and_commits_together(coordinator, participants):
    # 变更单以乱序给出,所有参与者共同提交后生效
    record = coordinator.submit("CHG-10", {"ext_c": {"c": 1}, "ext_a": {"a": 1}, "ext_b": {"b": 1}})
    assert record.stage is ChangeStage.COMMITTED
    # 每个参与者恰好 validate/prepare/commit 各一次
    for participant in participants.values():
        assert calls_of(participant, "validate") == 1
        assert calls_of(participant, "prepare") == 1
        assert calls_of(participant, "commit") == 1
    # ext_a 的调用序列:validate -> prepare -> commit
    assert [call[0] for call in participants["ext_a"].calls] == ["validate", "prepare", "commit"]
    assert participants["ext_a"].active == {"a": 1}
    assert participants["ext_b"].active == {"b": 1}
    assert participants["ext_c"].active == {"c": 1}
    assert coordinator.current_config("ext_a") == {"a": 1}


def test_prepare_order_is_sorted(tmp_path):
    order: list[str] = []

    class OrderProbe(FakeParticipant):
        def prepare(self, candidate):
            order.append(self.name)
            super().prepare(candidate)

    probes = [OrderProbe("ext_z"), OrderProbe("ext_m"), OrderProbe("ext_a")]
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=probes, state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit(
        "CHG-11", {"ext_z": {"v": 1}, "ext_m": {"v": 1}, "ext_a": {"v": 1}}
    )
    assert record.stage is ChangeStage.COMMITTED
    assert order == ["ext_a", "ext_m", "ext_z"]


def test_staged_manual_commit(coordinator):
    record = coordinator.submit("CHG-12", {"ext_a": {"x": 1}}, auto_commit=False)
    assert record.stage is ChangeStage.PREPARED
    # 就绪但未提交:生效配置仍是旧值
    assert coordinator.current_config("ext_a") == {}
    committed = coordinator.commit("CHG-12")
    assert committed.stage is ChangeStage.COMMITTED
    assert coordinator.current_config("ext_a") == {"x": 1}


def test_staged_manual_rollback(coordinator, participants):
    record = coordinator.submit("CHG-13", {"ext_a": {"x": 1}}, auto_commit=False)
    assert record.stage is ChangeStage.PREPARED
    rolled_back = coordinator.rollback("CHG-13")
    assert rolled_back.stage is ChangeStage.ABORTED
    assert calls_of(participants["ext_a"], "rollback") == 1
    assert coordinator.current_config("ext_a") == {}


def test_prepared_change_commits_onto_latest_snapshot(coordinator, participants):
    """就绪期间其他变更单先提交,本变更提交时不得覆盖他人已生效的配置。"""
    staged = coordinator.submit("CHG-13A", {"ext_a": {"x": 1}}, auto_commit=False)
    assert staged.stage is ChangeStage.PREPARED
    # 另一张变更单在此期间提交并生效
    coordinator.submit("CHG-13B", {"ext_b": {"y": 2}})
    assert coordinator.current_config("ext_b") == {"y": 2}
    # 提交先就绪的变更单:两份变更都应生效
    coordinator.commit("CHG-13A")
    assert coordinator.current_config("ext_a") == {"x": 1}
    assert coordinator.current_config("ext_b") == {"y": 2}


def test_commit_rejects_invalid_stage(coordinator):
    coordinator.submit("CHG-14", {"ext_a": {"x": 1}})
    with pytest.raises(InvalidStageTransition):
        coordinator.commit("CHG-14")
    with pytest.raises(UnknownChangeOrder):
        coordinator.commit("CHG-NOPE")


def test_rollback_of_committed_change_restores_baseline(coordinator, participants):
    coordinator.submit("CHG-15", {"ext_a": {"x": 1}})
    assert coordinator.current_config("ext_a") == {"x": 1}
    record = coordinator.rollback("CHG-15")
    assert record.stage is ChangeStage.ROLLED_BACK
    assert calls_of(participants["ext_a"], "rollback") == 1
    assert coordinator.current_config("ext_a") == {}


# -----------------------------------------------------------------------------
# 失败与回退
# -----------------------------------------------------------------------------


def test_prepare_failure_aborts_and_rolls_back_in_reverse_order(tmp_path):
    rollback_order: list[str] = []

    class Probe(FakeParticipant):
        def rollback(self):
            rollback_order.append(self.name)
            super().rollback()

    first = Probe("ext_a")
    second = Probe("ext_b")
    broken = Probe("ext_c", fail_at="prepare")
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[first, second, broken], state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit("CHG-20", {"ext_a": {"v": 1}, "ext_b": {"v": 1}, "ext_c": {"v": 1}})
    assert record.stage is ChangeStage.ABORTED
    # 已就绪的参与者按相反顺序回退
    assert rollback_order == ["ext_b", "ext_a"]
    # 失败的扩展有错误记录,生效配置未被污染
    assert record.participants["ext_c"].error is not None
    assert coordinator.current_config("ext_a") == {}
    assert coordinator.current_config("ext_b") == {}


def test_commit_failure_rolls_back_and_restores_store(tmp_path):
    good = FakeParticipant("ext_a")
    broken = FakeParticipant("ext_b", fail_at="commit")
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[good, broken], state_dir=str(tmp_path / "rollout")
    )
    coordinator.seed_base_configs({"ext_a": {"keep": "baseline"}})
    record = coordinator.submit("CHG-21", {"ext_a": {"x": 1}, "ext_b": {"y": 2}})
    assert record.stage is ChangeStage.ROLLED_BACK
    assert calls_of(good, "rollback") == 1
    # 生效存储恢复到变更前基线
    assert coordinator.current_config("ext_a") == {"keep": "baseline"}
    assert coordinator.current_config("ext_b") == {}


def test_non_rollbackable_participant_diverges_with_recovery_artifact(tmp_path):
    good = FakeParticipant("ext_a")
    rigid = FakeParticipant("ext_b", supports_rollback=False, fail_at="commit")
    state_dir = str(tmp_path / "rollout")
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[good, rigid], state_dir=state_dir
    )
    record = coordinator.submit("CHG-22", {"ext_a": {"x": 1}, "ext_b": {"y": 2}})
    assert record.stage is ChangeStage.DIVERGED
    # 可回退的参与者仍被回退,不可回退的被如实记录
    assert calls_of(good, "rollback") == 1
    assert record.participants["ext_b"].error is not None
    # 恢复工件落盘,保留可恢复状态
    assert record.recovery_path is not None
    assert os.path.exists(record.recovery_path)
    with open(record.recovery_path, encoding="utf-8") as f:
        artifact = json.load(f)
    assert artifact["diverged_extensions"] == ["ext_b"]
    assert "base_configs" in artifact and "candidate_deltas" in artifact
    assert artifact["record"]["change_id"] == "CHG-22"


def test_rollback_error_also_diverges(tmp_path):
    flaky = FakeParticipant("ext_a", fail_at="rollback")
    broken = FakeParticipant("ext_b", fail_at="commit")
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[flaky, broken], state_dir=str(tmp_path / "rollout")
    )
    record = coordinator.submit("CHG-23", {"ext_a": {"x": 1}, "ext_b": {"y": 2}})
    assert record.stage is ChangeStage.DIVERGED
    assert record.recovery_path is not None
    assert "回退失败" in (record.participants["ext_a"].error or "")


# -----------------------------------------------------------------------------
# 幂等提交
# -----------------------------------------------------------------------------


def test_duplicate_submit_is_idempotent(coordinator, participants):
    first = coordinator.submit("CHG-30", {"ext_a": {"x": 1}})
    second = coordinator.submit("CHG-30", {"ext_a": {"x": 1}})
    assert second is first
    # 不会重复执行任何阶段
    assert calls_of(participants["ext_a"], "prepare") == 1
    assert calls_of(participants["ext_a"], "commit") == 1


def test_duplicate_submit_with_different_payload_conflicts(coordinator, participants):
    coordinator.submit("CHG-31", {"ext_a": {"x": 1}})
    with pytest.raises(ChangeOrderConflict):
        coordinator.submit("CHG-31", {"ext_a": {"x": 2}})
    # 冲突提交不触发任何执行
    assert calls_of(participants["ext_a"], "prepare") == 1


def test_idempotency_survives_restart(tmp_path, participants):
    state_dir = str(tmp_path / "rollout")
    first = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    first.submit("CHG-32", {"ext_a": {"x": 1}})
    # 模拟进程重启:新协调器重放日志后,同一变更单仍幂等
    second = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    second.recover()
    replayed = second.submit("CHG-32", {"ext_a": {"x": 1}})
    assert replayed.stage is ChangeStage.COMMITTED
    assert calls_of(participants["ext_a"], "commit") == 1


# -----------------------------------------------------------------------------
# 崩溃恢复
# -----------------------------------------------------------------------------


def test_recovery_finishes_interrupted_prepared_change(tmp_path, participants):
    state_dir = str(tmp_path / "rollout")
    first = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    first.submit("CHG-40", {"ext_a": {"x": 1}}, auto_commit=False)
    # 模拟进程崩溃:直接丢弃协调器,不做任何收尾
    del first

    second = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    interrupted = second.recover()
    assert [record.change_id for record in interrupted] == ["CHG-40"]
    record = second.status("CHG-40")
    assert record["stage"] == ChangeStage.ABORTED.value
    assert any(entry["stage"] == "aborted" for entry in record["stage_history"])
    # 未提交的变更不会出现在生效配置中
    assert second.current_config("ext_a") == {}


def test_recovery_finishes_change_interrupted_mid_commit(tmp_path, participants):
    state_dir = str(tmp_path / "rollout")

    class CrashingParticipant(FakeParticipant):
        def commit(self):
            # 模拟提交进行到一半时进程崩溃
            raise KeyboardInterrupt

    crashing = CrashingParticipant("ext_a")
    first = ExtensionConfigRolloutCoordinator(
        participants=[crashing], state_dir=state_dir
    )
    with pytest.raises(KeyboardInterrupt):
        first.submit("CHG-41", {"ext_a": {"x": 1}})
    del first

    second = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    interrupted = second.recover()
    assert [record.change_id for record in interrupted] == ["CHG-41"]
    assert second.status("CHG-41")["stage"] == ChangeStage.ABORTED.value
    assert second.current_config("ext_a") == {}


def test_recovery_restores_last_committed_snapshot(tmp_path, participants):
    state_dir = str(tmp_path / "rollout")
    first = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    first.submit("CHG-42", {"ext_a": {"x": 1}})
    first.submit("CHG-43", {"ext_a": {"x": 2}, "ext_b": {"y": 3}})
    del first

    second = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    assert second.recover() == []
    # 重启后生效配置恰好是最后一份共同提交的快照
    assert second.current_config("ext_a") == {"x": 2}
    assert second.current_config("ext_b") == {"y": 3}
    assert second.status("CHG-43")["stage"] == ChangeStage.COMMITTED.value


def test_recovery_tolerates_truncated_journal(tmp_path, participants):
    state_dir = str(tmp_path / "rollout")
    first = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    first.submit("CHG-44", {"ext_a": {"x": 1}})
    del first
    # 模拟崩溃在日志写入中途:末尾留下半行
    with open(os.path.join(state_dir, "journal.jsonl"), "a", encoding="utf-8") as f:
        f.write('{"event": "commit_started", "at": "2026-10-06T00:00:00", "cha')

    second = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    second.recover()
    assert second.current_config("ext_a") == {"x": 1}


def test_recovery_is_idempotent(tmp_path, participants):
    state_dir = str(tmp_path / "rollout")
    first = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    first.submit("CHG-45", {"ext_a": {"x": 1}}, auto_commit=False)
    del first

    second = ExtensionConfigRolloutCoordinator(
        participants=participants.values(), state_dir=state_dir
    )
    assert len(second.recover()) == 1
    assert second.recover() == []


# -----------------------------------------------------------------------------
# 状态查询
# -----------------------------------------------------------------------------


def test_status_reports_real_per_stage_results(coordinator, participants):
    coordinator.submit("CHG-50", {"ext_a": {"x": 1}, "ext_b": {"y": 2}})
    status = coordinator.status()
    assert status["store_version"] == 1
    record = status["changes"]["CHG-50"]
    assert record["stage"] == "committed"
    # 每个阶段都有带时间戳的真实轨迹
    stages = [entry["stage"] for entry in record["stage_history"]]
    assert stages == ["received", "prechecked", "preparing", "prepared", "committing", "committed"]
    # 每个参与者的真实结果可查
    assert record["participants"]["ext_a"]["prepared"] is True
    assert record["participants"]["ext_a"]["committed"] is True
    assert record["participants"]["ext_b"]["committed"] is True
    # 单条查询
    single = coordinator.status("CHG-50")
    assert single["change_id"] == "CHG-50"
    with pytest.raises(UnknownChangeOrder):
        coordinator.status("CHG-NOPE")


# -----------------------------------------------------------------------------
# traitlets 适配器
# -----------------------------------------------------------------------------


class DummyApp(Configurable):
    mode = Unicode("dev").tag(config=True)
    workers = Int(1).tag(config=True)


class RigidApp(DummyApp):
    supports_config_rollback = False


def test_adapter_validate_unknown_and_invalid_keys():
    adapter = TraitletConfigAdapter(DummyApp(), name="dummy")
    assert adapter.validate({"mode": "prod"}) == []
    assert any("不存在配置项" in p for p in adapter.validate({"nope": 1}))
    assert any("校验失败" in p for p in adapter.validate({"workers": "not-an-int"}))


def test_adapter_commit_applies_and_rollback_restores():
    app = DummyApp()
    adapter = TraitletConfigAdapter(app, name="dummy")
    adapter.prepare({"mode": "prod", "workers": 4})
    # prepare 不激活
    assert app.mode == "dev"
    adapter.commit()
    assert app.mode == "prod"
    assert app.workers == 4
    adapter.rollback()
    assert app.mode == "dev"
    assert app.workers == 1


def test_adapter_commit_before_prepare_rejected():
    adapter = TraitletConfigAdapter(DummyApp(), name="dummy")
    with pytest.raises(RolloutError):
        adapter.commit()


def test_adapter_reads_capabilities_from_target():
    adapter = TraitletConfigAdapter(RigidApp(), name="rigid")
    assert adapter.supports_rollback is False
    assert TraitletConfigAdapter(DummyApp(), name="x").supports_rollback is True


def test_adapter_integrates_with_coordinator(tmp_path):
    app_a = DummyApp()
    app_b = DummyApp()
    coordinator = ExtensionConfigRolloutCoordinator(
        participants=[
            TraitletConfigAdapter(app_a, name="ext_a"),
            TraitletConfigAdapter(app_b, name="ext_b"),
        ],
        state_dir=str(tmp_path / "rollout"),
    )
    record = coordinator.submit("CHG-60", {"ext_a": {"mode": "prod"}, "ext_b": {"workers": 8}})
    assert record.stage is ChangeStage.COMMITTED
    assert app_a.mode == "prod"
    assert app_b.workers == 8
    # 校验失败的变更单不会触碰运行态
    rejected = coordinator.submit("CHG-61", {"ext_a": {"workers": "bad"}})
    assert rejected.stage is ChangeStage.REJECTED
    assert app_a.mode == "prod"


# -----------------------------------------------------------------------------
# REST 接口(端到端)
# -----------------------------------------------------------------------------


@pytest.fixture
def jp_server_config(jp_template_dir):
    return {
        "ServerApp": {"jpserver_extensions": {"tests.extension.mockextensions": True}},
        "MockExtensionApp": {"template_paths": [str(jp_template_dir)]},
    }


async def _post_change(jp_fetch, payload, expected=None):
    r = await jp_fetch(
        "api",
        "extension-config",
        "changes",
        method="POST",
        body=json.dumps(payload),
        raise_error=False,
    )
    if expected is not None:
        assert r.code == expected, r.body.decode()
    return r


async def test_api_submit_commits_and_propagates(jp_fetch, jp_serverapp):
    r = await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-1", "changes": {"mockextension": {"mock_trait": "updated"}}},
        expected=201,
    )
    record = json.loads(r.body)
    assert record["stage"] == "committed"
    assert record["participants"]["mockextension"]["committed"] is True
    # 共同提交后,扩展处理器通过自身配置看到新值
    r = await jp_fetch("mock", method="GET")
    assert r.body.decode() == "updated"
    # 生效中的 ServerApp 扩展 trait 也被更新
    app = jp_serverapp.extension_manager.extension_points["mockextension"].app
    assert app.mock_trait == "updated"


async def test_api_query_stage_results(jp_fetch):
    # 提交前的生效快照版本
    r = await jp_fetch("api", "extension-config", "changes")
    version_before = json.loads(r.body)["store_version"]
    await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-2", "changes": {"mockextension": {"mock_trait": "v2"}}},
        expected=201,
    )
    # 全量登记簿
    r = await jp_fetch("api", "extension-config", "changes")
    listing = json.loads(r.body)
    assert listing["store_version"] == version_before + 1
    assert "CHG-API-2" in listing["changes"]
    # 单张变更单:每个阶段的真实轨迹可查
    r = await jp_fetch("api", "extension-config", "changes", "CHG-API-2")
    record = json.loads(r.body)
    stages = [entry["stage"] for entry in record["stage_history"]]
    assert stages == ["received", "prechecked", "preparing", "prepared", "committing", "committed"]


async def test_api_duplicate_submit_is_idempotent(jp_fetch):
    payload = {"change_id": "CHG-API-3", "changes": {"mockextension": {"mock_trait": "v3"}}}
    first = json.loads((await _post_change(jp_fetch, payload, expected=201)).body)
    r = await jp_fetch("api", "extension-config", "changes")
    version_after_first = json.loads(r.body)["store_version"]
    second = json.loads((await _post_change(jp_fetch, payload)).body)
    assert second["change_id"] == first["change_id"]
    assert second["created_at"] == first["created_at"]
    # 重复提交不会再次推进生效快照
    r = await jp_fetch("api", "extension-config", "changes")
    assert json.loads(r.body)["store_version"] == version_after_first


async def test_api_conflicting_change_id_rejected(jp_fetch):
    await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-4", "changes": {"mockextension": {"mock_trait": "a"}}},
        expected=201,
    )
    r = await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-4", "changes": {"mockextension": {"mock_trait": "b"}}},
    )
    assert r.code == 409


async def test_api_precheck_failure_returns_422(jp_fetch):
    r = await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-5", "changes": {"ghost_extension": {"x": 1}}},
    )
    assert r.code == 422
    record = json.loads(r.body)
    assert record["stage"] == "rejected"
    assert any("ghost_extension" in problem for problem in record["precheck_problems"])


async def test_api_validation_failure_leaves_runtime_untouched(jp_fetch):
    r = await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-6", "changes": {"mockextension": {"mock_trait": 123}}},
    )
    assert r.code == 422
    # 运行中的扩展配置未被污染
    r = await jp_fetch("mock", method="GET")
    assert r.body.decode() == "mock trait"


async def test_api_staged_commit_and_rollback(jp_fetch):
    # 分阶段:提交后停在 prepared,运行中的请求仍看到旧配置
    r = await _post_change(
        jp_fetch,
        {
            "change_id": "CHG-API-7",
            "changes": {"mockextension": {"mock_trait": "staged"}},
            "auto_commit": False,
        },
        expected=201,
    )
    assert json.loads(r.body)["stage"] == "prepared"
    r = await jp_fetch("mock", method="GET")
    assert r.body.decode() == "mock trait"
    # 显式共同提交后,新请求看到新配置
    r = await jp_fetch(
        "api", "extension-config", "changes", "CHG-API-7", "commit", method="POST", body="{}"
    )
    assert json.loads(r.body)["stage"] == "committed"
    r = await jp_fetch("mock", method="GET")
    assert r.body.decode() == "staged"

    # 另一张变更单演示分阶段回退
    await _post_change(
        jp_fetch,
        {
            "change_id": "CHG-API-8",
            "changes": {"mockextension": {"mock_trait": "will-rollback"}},
            "auto_commit": False,
        },
        expected=201,
    )
    r = await jp_fetch(
        "api", "extension-config", "changes", "CHG-API-8", "rollback", method="POST", body="{}"
    )
    assert json.loads(r.body)["stage"] == "aborted"
    r = await jp_fetch("mock", method="GET")
    assert r.body.decode() == "staged"


async def test_api_unknown_change_and_invalid_transition(jp_fetch):
    r = await jp_fetch("api", "extension-config", "changes", "CHG-NOPE", raise_error=False)
    assert r.code == 404
    r = await jp_fetch(
        "api",
        "extension-config",
        "changes",
        "CHG-NOPE",
        "commit",
        method="POST",
        body="{}",
        raise_error=False,
    )
    assert r.code == 404
    # 已提交的变更单不能再次提交
    await _post_change(
        jp_fetch,
        {"change_id": "CHG-API-9", "changes": {"mockextension": {"mock_trait": "v9"}}},
        expected=201,
    )
    r = await jp_fetch(
        "api",
        "extension-config",
        "changes",
        "CHG-API-9",
        "commit",
        method="POST",
        body="{}",
        raise_error=False,
    )
    assert r.code == 409


async def test_api_rejects_malformed_body(jp_fetch):
    r = await jp_fetch(
        "api", "extension-config", "changes", method="POST", body="{}", raise_error=False
    )
    assert r.code == 400
    r = await jp_fetch(
        "api",
        "extension-config",
        "changes",
        method="POST",
        body=json.dumps({"change_id": "CHG-API-10", "changes": {"mockextension": "not-a-dict"}}),
        raise_error=False,
    )
    assert r.code == 400
