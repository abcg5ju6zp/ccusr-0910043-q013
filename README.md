# Jupyter Server 内容服务

本项目提供服务端内容、目录、检查点、会话和鉴权接口。生产源码位于 `jupyter_server/`，核心回归测试位于 `tests/`。

## 安装

`python3 -m pip install --break-system-packages --no-build-isolation -e '.[test]'`

## 测试

`python3 -m pytest -q`

## 构建

`python3 -m compileall -q jupyter_server`

`python3 -m build --wheel --no-isolation`

## 使用

内容管理器可在本地目录上执行保存、复制、改名、删除和检查点操作，HTTP 处理器提供对应服务端接口。

## 扩展配置变更单

`jupyter_server/extension/configtxn/` 为多个 ServerApp 扩展的配置变更提供事务化机制：每张变更单先冻结候选快照并预检（依赖、冲突、可导入性、配置项合法性），再让扩展按稳定顺序准备、共同提交；任一扩展准备失败即按逆序回退，不支持回退的扩展保留现场待人工恢复。journal 落盘保证进程崩溃后重启可恢复：已记录提交决定的变更单继续完成，未到达提交决定的按回退处理。提交前当前请求始终读取旧的已提交快照；同一变更单号重复提交具有幂等性。

运维接口：

- `POST /api/extension-changes`：提交变更单（`{"change_id", "changes"}`；`"dry_run": true` 时仅预检）
- `GET /api/extension-changes`：列出全部变更单、已提交快照版本与在读读者数
- `GET /api/extension-changes/<change_id>`：查询一张变更单每个阶段、每个扩展的真实结果
- `POST /api/extension-changes/<change_id>/rollback`：回退一张已提交的变更单

扩展可通过覆写 `ExtensionApp` 的 `prepare_config_change` / `commit_config_change` / `rollback_config_change` 钩子参与两阶段提交，并以 `supports_config_rollback = False` 声明不支持回退。
