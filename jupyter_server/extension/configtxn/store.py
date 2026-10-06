"""变更单与配置快照的持久化存储。

包含两部分：

- ``ChangeJournal``：追加式 JSONL 日志。每个事件写入后立即 ``fsync``，
  保证进程崩溃后已确认的事件不丢失；读取时容忍最后一条记录被截断
  （崩溃可能发生在写一半的时候），截断的记录视为未发生。
- ``SnapshotStore``：配置快照的落盘。已提交快照与候选快照都通过
  “写临时文件 + ``os.replace``” 原子替换，杜绝半写状态。

磁盘布局::

    <state_dir>/
        journal.jsonl              # 追加式事件日志
        committed.json             # 最近一次提交的配置快照
        candidates/<change_id>.json  # 冻结的候选快照（保留用于审计）
"""

from __future__ import annotations

import copy
import errno
import json
import os
import typing as t

from jupyter_server._tz import utcnow
from jupyter_server.config_manager import recursive_update

__all__ = ["ChangeJournal", "ConfigSnapshot", "SnapshotStore"]


def _atomic_write_json(path: str, data: dict[str, t.Any]) -> None:
    """以“临时文件 + 原子替换”的方式写入 JSON，并 fsync 落盘。"""
    tmp_path = f"{path}.tmp.{os.getpid()}"
    content = json.dumps(data, indent=2, sort_keys=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)
    # fsync 目录项，保证替换本身持久化
    dir_fd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class ConfigSnapshot:
    """一份不可变的扩展配置快照。

    ``extensions`` 形如::

        {
            "my_ext": {
                "enabled": true | false | null,
                "config_section": "jupyter_my_ext_config",
                "config": {...},
            }
        }
    """

    def __init__(
        self,
        version: int = 0,
        extensions: dict[str, t.Any] | None = None,
        created_at: str | None = None,
    ) -> None:
        self.version = version
        self._extensions = extensions or {}
        self.created_at = created_at or utcnow().isoformat()

    @property
    def extensions(self) -> dict[str, t.Any]:
        """返回快照内容的深拷贝，调用方无法借此修改快照本体。"""
        return copy.deepcopy(self._extensions)

    def get_extension(self, name: str) -> dict[str, t.Any]:
        """返回单个扩展条目的深拷贝；不存在时返回空条目。"""
        entry = self._extensions.get(name, {"enabled": None, "config_section": None, "config": {}})
        return copy.deepcopy(entry)

    def apply_changes(
        self,
        changes: dict[str, t.Any],
        current_entries: dict[str, dict[str, t.Any]],
    ) -> ConfigSnapshot:
        """以本快照为基线生成候选快照（纯函数，不修改自身）。

        ``current_entries`` 提供每个被触及扩展当前的有效配置
        （从配置中心实时读取），保证候选快照冻结的是完整现场。
        """
        merged = copy.deepcopy(self._extensions)
        for name, change in changes.items():
            entry = merged.get(name) or current_entries.get(name) or {}
            new_entry = {
                "enabled": entry.get("enabled"),
                "config_section": entry.get("config_section"),
                "config": copy.deepcopy(entry.get("config", {})),
            }
            if "config_section" in change:
                new_entry["config_section"] = change["config_section"]
            if "enabled" in change:
                new_entry["enabled"] = change["enabled"]
            if "config" in change:
                section_content = copy.deepcopy(new_entry["config"])
                recursive_update(section_content, change["config"])
                new_entry["config"] = section_content
            merged[name] = new_entry
        return ConfigSnapshot(
            version=self.version + 1,
            extensions=merged,
        )

    def to_dict(self) -> dict[str, t.Any]:
        """序列化。"""
        return {
            "version": self.version,
            "extensions": copy.deepcopy(self._extensions),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> ConfigSnapshot:
        """反序列化。"""
        return cls(
            version=data.get("version", 0),
            extensions=data.get("extensions") or {},
            created_at=data.get("created_at"),
        )


class ChangeJournal:
    """追加式变更事件日志（JSONL，写后 fsync）。"""

    def __init__(self, path: str) -> None:
        self.path = path

    def append(self, event: dict[str, t.Any]) -> None:
        """追加一条事件并立即 fsync。事件会自动带上 ``ts`` 时间戳。"""
        record = dict(event)
        record.setdefault("ts", utcnow().isoformat())
        line = json.dumps(record, sort_keys=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def load(self) -> list[dict[str, t.Any]]:
        """读取全部事件。

        容忍最后一条记录不完整（进程崩溃导致截断）：丢弃该记录。
        中间的损坏记录同样跳过——恢复流程宁可少一条事件，
        也不能因为单条损坏而放弃整个日志。
        """
        events: list[dict[str, t.Any]] = []
        if not os.path.exists(self.path):
            return events
        with open(self.path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                if i != len(lines) - 1:
                    # 非末尾的损坏行：跳过，但保留其余事件
                    continue
                # 末尾截断：崩溃现场，视为未发生
        return events


class SnapshotStore:
    """配置快照的磁盘存储。"""

    def __init__(self, state_dir: str) -> None:
        self.state_dir = state_dir
        self.candidates_dir = os.path.join(state_dir, "candidates")
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        for path in (self.state_dir, self.candidates_dir):
            try:
                os.makedirs(path, 0o755)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    @property
    def committed_path(self) -> str:
        """已提交快照的文件路径。"""
        return os.path.join(self.state_dir, "committed.json")

    def candidate_path(self, change_id: str) -> str:
        """候选快照的文件路径。"""
        return os.path.join(self.candidates_dir, f"{change_id}.json")

    def load_committed(self) -> ConfigSnapshot:
        """读取已提交快照；不存在时返回版本 0 的空快照。"""
        if not os.path.exists(self.committed_path):
            return ConfigSnapshot()
        with open(self.committed_path, encoding="utf-8") as f:
            return ConfigSnapshot.from_dict(json.load(f))

    def write_committed(self, snapshot: ConfigSnapshot) -> None:
        """原子写入已提交快照。"""
        _atomic_write_json(self.committed_path, snapshot.to_dict())

    def write_candidate(self, change_id: str, snapshot: ConfigSnapshot) -> None:
        """原子写入候选快照（冻结）。"""
        _atomic_write_json(self.candidate_path(change_id), snapshot.to_dict())

    def read_candidate(self, change_id: str) -> ConfigSnapshot | None:
        """读取候选快照；不存在时返回 None。"""
        path = self.candidate_path(change_id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            return ConfigSnapshot.from_dict(json.load(f))
