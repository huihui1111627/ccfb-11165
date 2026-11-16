# 支付交易数据分层归档平台

面向支付公司交易库（Postgres）的冷热分层归档平台：按策略把历史数据转成高压缩列存、从源库分批清除，查询侧自动合并冷热结果，提供保全（legal hold）与可审计销毁清单，并附带一套管理界面。

## 实测结果摘要

30,000,000 行 / 7 年数据、3 年转冷：归档 2 分 39 秒，17,152,953 行转冷为 18 个段共 56.9 MB；归档前后 21 个查询用例逐行一致；归档期间源库在线写入 1,085 次零错误（p99 21.6 ms）；销毁 5,863,512 行并保留 2,136,488 行保全数据，清单 SHA-256 可验证。

完整数据见 [REPORT.md](REPORT.md)，原始 JSON 在 `data/report/`。

## 技术要点

| 需求 | 实现 |
|---|---|
| 按表/时间字段配置转冷与销毁期限 | `policies` 表 + 调度器；销毁期限强制不低于监管最低留存（默认 1825 天） |
| 迁移分批，先写冷存校验通过再删源 | 流式 keyset 分页读取 → 写段 → 全量解码校验 + SHA-256 → 才进入删除阶段 |
| 冷数据压缩列存 | 自研 `PAYCOL01` 段格式：按列编码（zigzag varint / delta / delta-of-delta / 字典）+ 块级 zstd(-9) |
| 查询冷热一起返回、调用方不改 | `/api/query` 单一接口；结果按 `(created_at,id)` 倒序归并并按主键去重 |
| 诉讼数据保全标记 | `txn` / `txn_range` / `merchant` / `time_range` 四种范围；源库删除、冷存重写两处都强制校验 |
| 销毁后给可审计清单 | 每个销毁任务生成 JSON 清单：逐行 ID 的 SHA-256 摘要、段级明细、清单文件自身 SHA-256 |
| 归档时源库正常写入 | 读取使用 `updated_at <= fence` 快照；删除按 2000 行 id 批 + 停顿，避免长事务锁表 |
| 冷查询独立并发/流量限制 | 并发信号量 + 令牌桶 QPS + 队列超时，超限返回 HTTP 429 |

### 冷存段格式

```
[magic "PAYCOL01"][version u32][metaLen u32][meta JSON]
  block × N: [rawLen u32][compLen u32][crc32c u32][zstd(columnar payload)]
[footer JSON][footerLen u32][footerCRC u32]["PAYIDX01"]
```

- 每 8192 行一个块，块头记录 `min/max` 的 `created_at`、`id` 以及各列 min/max，用于**块级剪枝**。
- 列编码自动择优：整数用 plain / delta / delta-of-delta，字符串在低基数时用字典编码。
- 打开段时校验头部、footer CRC 与块偏移；读取块时校验 CRC32C；归档/销毁流程再做完整解码校验。
- 段不可变：销毁保全行时采用**重写新段 → 校验 → 删旧段**，不会原地修改。

## 快速开始

前置：Go 1.24+、PostgreSQL 14+（本地）。

```bash
createdb payarchive
go build -o bin/server ./cmd/server
go build -o bin/seed ./cmd/seed
go build -o bin/bench ./cmd/bench

# 1) 造 3000 万行、7 年跨度的交易表（约 3 分钟）
./bin/seed -rows 30000000 -years 7

# 2) 启动管理台
./bin/server -listen :18080
#    打开 http://localhost:18080
```

在界面「归档策略」新建策略（3 年转冷 / 5 年销毁），即可点「转冷」。

### 生成基准报告

```bash
# 归档前：采集 25 个查询用例的源库结果
./bin/bench -phase pre
# 触发归档（界面按钮或 API）
curl -X POST localhost:18080/api/policies/1/archive
# 归档后：冷热合并查询与基线逐行比对 + 延迟基准
./bin/bench -phase post
# 输出：data/report/bench-report.json（另有 baseline.json / baseline-anchor.json）
```

## 配置

`config.json`（或环境变量 `PAYARCHIVE_*`）：

```json
{
  "listen": ":8080",
  "data_dir": "./data",
  "control_db": "./data/control.db",
  "source_dsn": "postgres:///payarchive?sslmode=disable",
  "regulatory_min_retention_days": 1825,
  "scheduler_interval": "5m",
  "auto_archive": true,
  "auto_destroy": true,
  "archive": {
    "batch_rows": 50000,
    "delete_chunk_rows": 2000,
    "delete_pause_ms": 5,
    "target_segment_rows": 1000000,
    "workers": 8
  },
  "cold_query": {
    "max_concurrent": 4,
    "max_qps": 10,
    "queue_timeout_ms": 1000,
    "max_bytes_per_query": 134217728,
    "max_limit": 1000,
    "default_limit": 100
  }
}
```

环境变量：`PAYARCHIVE_LISTEN`、`PAYARCHIVE_DATA_DIR`、`PAYARCHIVE_SOURCE_DSN`、`PAYARCHIVE_CONTROL_DB`、`PAYARCHIVE_SCHEDULER`、`PAYARCHIVE_DEMO_ROWS`。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/policies` | 策略列表（含待归档行数、可销毁段数） |
| POST/PUT | `/api/policies[/{id}]` | 新建/更新策略 |
| POST | `/api/policies/{id}/archive` | 触发转冷任务 |
| POST | `/api/policies/{id}/destroy` | 触发销毁任务 |
| GET | `/api/jobs?limit=N` | 任务列表 |
| GET | `/api/jobs/{id}` | 任务详情 + 日志 |
| POST | `/api/jobs/{id}/cancel` | 取消运行中任务 |
| GET | `/api/storage` | 源库/冷存占用、月分布、段列表 |
| GET | `/api/query` | 统一冷热查询 |
| GET/POST | `/api/holds` | 保全记录 |
| POST | `/api/holds/{id}/release` | 解除保全 |
| GET | `/api/manifests[/{id}]` | 销毁清单 |
| GET | `/api/manifests/{id}/download` | 下载清单 JSON |

查询参数（`/api/query`）：`from`/`to`（RFC3339 或 `720h` 形式）、`merchant_id`、`status`、`channel`、`region`、`currency`、`txn_no`、`min_amount`、`max_amount`、`limit`、`cursor`。

## 目录结构

```
cmd/server        管理台与 API
cmd/seed          数据生成（generate_series，确定性、可压缩）
cmd/bench         归档前后一致性比对与延迟/压缩率基准
cmd/liveprobe     归档期间源库写入延迟探针
internal/cold         列存段格式（编码、CRC、footer、校验）
internal/coldquery    冷查询引擎（剪枝、缓存、并发/QPS 限制）
internal/archive      归档任务（快照、写段、校验、分块删除）
internal/destruction  销毁任务（保全重写、清单）
internal/unifiedquery 冷热归并查询
internal/control      SQLite 元数据（策略/任务/段/保全/清单）
internal/source       Postgres 适配（快照读取、keyset 删除、保全谓词）
internal/server       HTTP API
web                  管理界面（嵌入式单页应用）
test/integration      端到端测试
```

## 测试

```bash
createdb payarchive_test      # 集成测试库（会自动建表并灌入 6 万行）
go test ./...
```

集成测试覆盖：归档前后 6 类查询逐行一致；分页跨冷热无重复/遗漏；保全行在归档与销毁后仍可查；销毁清单哈希可验证；冷查询并发+QPS 限流；销毁期限低于监管底线时拒绝执行。

## 已知边界

- 演示数据由 `generate_series` 生成，压缩率偏高（27× 对解码后、约 69× 对源库行长），真实交易数据会明显更低；报告以同一数据集的前后测量为准。
- 源库适配器当前只实现了 Postgres `transactions` 表；新增表需要实现对应的 schema 与读取 SQL。
- 元数据（策略、任务、段索引、保全、清单）存放在本地 SQLite；生产多节点部署应换成可共享的元数据库。
- 段缓存为进程内 LRU（默认 512MB），多实例部署时各自独立。
