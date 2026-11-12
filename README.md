# Parquet Snapshot Studio

同一分区内零散 Parquet 小文件的在线合并、快照查询、乐观并发发布与引用感知回收。

后端：Python · PyArrow · DuckDB · FastAPI  
前端：React · Vite  
存储：不可变 Parquet 数据文件 + 逐版本 JSON manifest + SQLite catalog

## 快速启动

```bash
./scripts/run.sh
```

打开 <http://127.0.0.1:8000>。首次启动会自动生成 `orders` 分区的 32 个零散 Parquet 文件（每文件 24 行），并发布为 `v1`。API 文档在 <http://127.0.0.1:8000/docs>。

也可以手动启动：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

重置演示数据（服务停止后执行；下次启动会自动重新生成）：

```bash
rm -rf runtime
```

## 页面能做什么

- **分区文件分布**：物理文件数量、大小、类型、被哪些版本引用、是否 current、是否有孤儿文件。
- **版本列表**：每一版的 operation、文件数、行数、金额、manifest 哈希、固定状态、清理状态；可固定 / 解除固定。
- **合并任务**：每次尝试的 base version、选中的输入文件、输出文件、DuckDB 完整校验结果、冲突和重新规划时间线。
- **快照查询**：固定一个 manifest 版本后查询；可在同一面板与 current 对比。查询期间登记活动租约，旧版文件不会被 GC 删除。
- **操作台**：追加写入、发起合并、设置发布延迟，以及五类一键演示。
- **清理面板**：执行“保留最近三版 + 固定版本”的清理，列出受保护版本、被清理版本和实际删除的文件。

## 一键演示路线

页面上按顺序点击，或在服务运行时执行脚本：

```bash
.venv/bin/python scripts/demo_flow.py
```

脚本会调用相同 API 并逐项断言：

1. **合并中追加**
   合并任务先基于 `vN` 规划并校验临时输出；发布前追加写入发布为 `vN+1`。
   合并发布时检测到版本冲突，丢弃本轮输出，从新 current 重新规划。
   结果检查追加数据未丢失、金额精确、旧快照仍完整。
2. **双轮并发合并**
   两轮任务从同一 base 规划；只有一轮能优先发布，另一轮冲突后重新规划。
   当前数据行数、金额不变，`event_id` 无重复。
3. **发布前失败**
   输出已通过校验但还没发布时注入失败；任务失败、current 不推进、临时输出删除。
4. **旧快照读取**
   对旧版本发起 DuckDB 查询，逐文件对账行数和金额；与 current 对比展示 delta。
5. **固定 → 清理 → 解除固定**
   固定旧版，追加多版，执行保留最近三版的清理：固定版及其文件仍在，旧快照仍可查。
   解除固定后再次清理：旧版 manifest 被 prune，仅被它引用的文件被回收，旧查询返回 410。
   整个过程中 current 数据始终保持完整。

每次操作后刷新：
- 合并任务会展示选中的文件清单与输出校验；
- 快照查询会展示 manifest 引用的每个文件、期望 / 实际行数与金额；
- 清理会展示保护原因和实际删除文件。

## 并发与一致性设计

```text
                       ┌────────────────────────────┐
  append new parquet ─▶│ staging (临时，查询不可见) │
                       └──────────────┬─────────────┘
                                      │ validate + fsync
                                      ▼
   ┌──────────┐   BEGIN IMMEDIATE   ┌────────────────┐
   │ SQLite   │◀────────────────────│ publish attempt│
   │ catalog  │  current == base ?  └────────────────┘
   └────┬─────┘         │ yes
        │               ▼
        │        manifest vN+1 (json)
        │        data file (read-only)
        ▼
  current pointer atomically advanced
```

### 文件不可变

- 每次追加 / 合并写出新的唯一文件，发布后 `chmod 0444`。
- 普通查询只读 manifest 的 `files` 列表，绝不扫描 staging 目录。
- 未发布版本没有 catalog 记录，也没有 current 指针。

### Manifest 与发布

- 每个版本一份不可变 JSON：文件路径、行数、金额、SHA-256、输入 / 输出。
- manifest 文件本身有 SHA-256，SQLite 行保存摘要；读取时再次校验。
- 发布事务为 `BEGIN IMMEDIATE`，同时比较 base version 与 current。
- DuckDB 在发布前执行：
  - 行数、金额、distinct `event_id`；
  - 输入与输出双向 `EXCEPT ALL`，精确比较行多重集；
  - 输入文件 SHA-256 与 manifest 对比。
- 只有全部通过，`current_version` 才切换到新版本。

### 追加与合并的冲突规则

- 追加写入总在新事务里合并当前 manifest，不覆盖别人刚发布的数据。
- 合并是乐观发布：任务记录规划时的 `base_version`。
- 如果期间有追加或另一轮合并发布，current 已推进，发布直接抛出 `VersionConflict`。
- 合并任务丢弃临时输出，基于新 current 重新选文件、重新校验、再发布（最多 5 次）。
- 合并只替换本轮 manifest 中选中的文件；其余文件引用原样保留。

### 快照查询

- 查询开始时在一个短事务里解析 current（或调用方指定版本）并登记 `lease`。
- 租约期间 GC 会保留该版本及其引用文件。
- DuckDB 在一个查询里读取 manifest 列出的全部不可变文件，返回行数、金额和逐文件对账。
- 合并发布不会改变正在读取的 manifest 或文件字节，因此旧快照的 row count / amount 保持稳定。

### 回收

清理规则 `keep=3`：

1. 保护版本号最新的 3 个已发布版本；
2. 保护所有活动 pin、活动查询 lease、运行中任务的 base version；
3. 其余旧版本在 SQLite 中写 `pruned_at`；
4. 只删除不被任何保护版本引用的物理 Parquet；
5. 最后删除被 prune 的 manifest JSON。

因为先提交元数据、再删文件，进程中途退出最多留下无害孤儿文件，下一轮清理会再次发现并删除；永远不会先删掉仍被任何保留版本引用的文件。

重复发起合并、发布前失败、进程退出都不会造成行数 / 金额重复或缺失：current 只指向通过校验且完整引用每一份数据的 manifest。

## 数据模型

```text
runtime/
  catalog.sqlite3          # partitions, versions, version_files, pins, leases, tasks
  data/
    orders/*.parquet       # 只读数据文件
  manifests/
    v00000001.json         # 不可变版本 manifest
  staging/
    *.parquet              # 未发布输出，普通查询不可见，启动时清理
```

Manifest 示例：

```json
{
  "version_id": 3,
  "partition": "orders",
  "parent_version": 2,
  "operation": "compact",
  "aggregates": {"file_count": 17, "rows": 576, "amount_cents": 12345678},
  "files": [
    {"path": "orders/seed-01-....parquet", "rows": 24, "amount_cents": 1234,
     "sha256": "…", "kind": "small"}
  ],
  "inputs": [{"path": "orders/seed-01-....parquet", "rows": 24}],
  "outputs": [{"path": "orders/compact-....parquet", "rows": 144}],
  "validation": {"valid": true, "difference_rows": 0}
}
```

## 主要 API

| Method | Path | 说明 |
|---|---|---|
| GET | `/api/health` | 服务与当前版本 |
| GET | `/api/state` | 分区总览、文件分布、版本、pin、任务 |
| GET | `/api/versions?partition=orders` | 版本列表 |
| GET | `/api/versions/{id}` | 单个版本与 manifest 引用文件 |
| POST | `/api/versions/{id}/verify` | 重算文件 SHA-256 与汇总 |
| POST | `/api/versions/{id}/pin` | 固定版本 |
| POST | `/api/versions/{id}/unpin` | 解除固定 |
| GET | `/api/snapshot?version=N` | 固定版本快照查询 |
| GET | `/api/compare?version=N` | 旧快照 vs current |
| POST | `/api/append` | 追加一个新 Parquet 文件 |
| POST | `/api/compactions` | 启动合并任务 |
| POST | `/api/cleanup` | 保留最近 N 版 + pin 的回收 |
| POST | `/api/demo/append-during-compaction` | 合并中追加演示 |
| POST | `/api/demo/dual-compaction` | 双轮并发冲突演示 |
| POST | `/api/demo/failure-before-publish` | 发布前失败演示 |
| POST | `/api/demo/repeat-compaction` | 重复发起合并演示 |
| POST | `/api/demo/pin-and-gc` | 固定、清理、解固定、回收演示 |

## 测试

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q
```

覆盖：固定快照精确性、合并中追加冲突、双轮冲突、发布前失败、pin 保留与解固定回收、staging 不可见、重启恢复、孤儿文件不可查询。

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `PQS_DATA_ROOT` | `<repo>/runtime` | catalog、data、manifest 根目录 |
| `PQS_PARTITION` | `orders` | 演示分区 |
| `PQS_SEED_FILES` | `32` | 首次生成的小文件数 |
| `PQS_SEED_ROWS` | `24` | 每个小文件行数 |
| `PQS_MAX_COMPACT_FILES` | `16` | 每轮最多选择文件数 |
| `PQS_KEEP_VERSIONS` | `3` | 清理保留最近版本数 |
| `PQS_AUTO_SEED` | `1` | 空 catalog 自动生成演示数据 |
| `PQS_HOST` / `PQS_PORT` | `127.0.0.1` / `8000` | 服务监听地址 |

## 说明

金额使用整数“分”（`amount_cents`），避免浮点汇总误差。查询和合并都通过 DuckDB 扫描 Python/PyArrow 写出的 Parquet；发布只在完整校验成功后发生。
