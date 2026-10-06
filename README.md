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

## 扩展配置的事务化变更

多个 ServerApp 扩展的配置可通过"变更单"一次性提交，具备预检、分阶段启用与回退能力：

* 提交时先冻结候选快照并预检（配置校验、扩展间依赖与互斥），预检失败不触碰运行态；
* 各扩展按稳定顺序 prepare 暂存变更，全部就绪后共同 commit；进行中与已持有的请求始终看到旧配置；
* 任一阶段失败按相反顺序回退；扩展不支持回退时变更单进入 `diverged` 并落盘恢复工件；
* 每次阶段迁移写入持久化日志（默认 `jupyter_data_dir()/server/config-rollout`），进程崩溃后重启自动恢复到最后一份共同提交的快照；
* 同一 `change_id` 重复提交且负载一致时幂等返回既有结果，负载不一致返回 409。

REST 接口（`jupyter_server/extension/rollout_handlers.py`）：

* `POST /api/extension-config/changes` —— 提交变更单 `{"change_id", "changes": {扩展名: 配置}, "auto_commit"?}`；
* `GET /api/extension-config/changes[/<change_id>]` —— 查询每张变更单每个阶段、每个参与者的真实结果；
* `POST /api/extension-config/changes/<change_id>/commit|rollback` —— 对 `auto_commit=false` 停在 `prepared` 的变更单做人工推进或回退。
