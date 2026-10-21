# z-etl

ETL 引擎仓。job = **reader → transform 链 → writer**，用 JSON / yml 描述（DataX 式）。

> 需求 008「数据集成（z-dms + z-etl）」的 M1 主体之一。本仓是 **z-dms（数据源管控）** 的第一个重度消费者
> （D2 拍板）：z-dms 落地即被 z-etl 消费。

## 结构

```
z-etl/
├── pom.xml                    顶层 parent（自包含，CI-friendly ${revision} + flatten oss）
├── z-etl-core/                引擎核心：job 模型 + reader/transform/writer + 执行器 + 水位 + 对账
│   └── src/main/java/com/zifang/z/etl/core/
│       ├── job/               作业描述模型（JobDescriptor + 解析器 + 校验器）
│       ├── spi/               DataSourceProvider（数据源解析 SPI，z-dms 落地后由它提供实现）
│       ├── reader/            取数器（分页游标 + 水位过滤）
│       ├── transform/         变换链（字段映射 + 类型归一）
│       ├── writer/            写出器（批量 upsert + 幂等键）
│       ├── checkpoint/        水位存储（落库 / 内存两实现）
│       ├── reconcile/         对账器（行数 + 校验和双口径）
│       ├── engine/            执行器 EtlEngine + 作业仓储（落库）+ 引擎自身库表
│       └── util/              校验和、行内取值助手
└── z-etl-web/                 管控面：job 登记 / 手动触发 / 水位与对账查询（REST）
    └── src/main/java/com/zifang/z/etl/web/
        ├── config/            EtlModuleDataSource（z.base.db.etl.* + @MapperScan 绑本域 sqlSessionFactory）
        └── api/                EtlJobController + EtlJobService
```

## 作业描述（JSON / yml 等价）

```yaml
job:
  name: trade-order-to-snapshot
  mode: auto                    # auto（首跑全量→自动切增量）/ init（强制全量）/ incremental（强制增量）
  source:
    dataSourceRef: ds-oc-main   # 引用 z-dms 的 DataSourceDef
    table: z_trade_order
    columns: [id, order_no, user_id, amount, status, updated_at]
    watermarkColumn: updated_at
    pageSize: 200
  transform:
    fieldMappings:
      - {source: id,         target: order_id,   type: LONG}
      - {source: order_no,   target: order_no,   type: STRING}
      - {source: amount,     target: amount,     type: BIGDECIMAL}
      - {source: updated_at, target: updated_at, type: TIMESTAMP}
  target:
    dataSourceRef: ds-etl-landing
    table: etl_trade_order_snapshot
    batchSize: 200
    writeMode: upsert           # upsert（幂等）/ insert
  idempotencyKey: [order_id]    # 重跑幂等靠它
  watermark:
    column: updated_at
    type: TIMESTAMP             # LONG（自增 id 游标）/ TIMESTAMP（update_time 游标）
```

JSON 形态写成同一份内容即可（`{"job": {...}}` 或直接是描述对象）。

## 管控 REST

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/etl/jobs` | 登记作业（body = JSON 或 yml 描述原文） |
| GET | `/api/etl/jobs` | 作业名列表 |
| GET | `/api/etl/jobs/{name}` | 作业描述 |
| POST | `/api/etl/jobs/{name}/run` | 触发作业 |
| GET | `/api/etl/jobs/{name}/watermark` | 水位记录 |
| GET | `/api/etl/jobs/{name}/runs` | 运行履历 |

## 运行模式（两段模式）

| 声明 | 行为 |
|---|---|
| `mode: auto`（默认） | 无水位记录 ⇒ **初始化模式**（首跑全量）；跑完落水位 ⇒ 下次**自动切增量** |
| `mode: init` | 强制全量重跑（幂等重跑验证用） |
| `mode: incremental` | 强制增量；**无水位直接失败**，不静默退化成全量 |

水位落在引擎自身库的 `etl_job_checkpoint`（持久化 ⇒ 断点续传）；作业定义落在 `etl_job_def`
（同一份描述存 JSON + yml 两份文本）；运行履历落在 `etl_job_run`。

## 对账口径（搬运完自动核对，双口径）

- **行数**：源侧 `COUNT(*)` vs 目标侧 `COUNT(*)`；
- **校验和**：两侧各扫全表，按**幂等键多重集**算 FNV-1a 求和（与顺序无关）⇒ 能同时抓住漏行与重行。

两侧都做整表扫描，所以「首跑全量」与「增量接力」口径一致可比（M1 代价，增量省扫描的做法留后续）。

## 构建与验收

```bash
mvn clean install        # 2 个模块，Java 8；发版改根 pom <revision> 一处
mvn -o test              # 11 个用例（core 10 + web 1）
```

三项验收的落点与复跑命令见 [`_doc/002_acceptance/001_验收复跑.md`](_doc/002_acceptance/001_验收复跑.md)。

> ⚠️ 验收全部跑在**本地 H2 内存库**上（源库 / 目标库 / 引擎元库三个独立库）：
> 不连生产 RDS、不在 `oc` 库建表、不对任何真实库写入。

## 边界（防功能漂移）

| 邻居 | 它管什么 | 本仓的分界 |
|---|---|---|
| `z-mq` | 运行时事件流 | 本仓管**存量与批量**，不管实时流 |
| `z-lc` pipeline | 应用内处理器链 | 本仓管**跨系统、跨库**的搬运 |
| `z-lc` 报表子域 dataset | 取数渲染（查询态） | 本仓管**物化落地**（存储态） |
| `z-dms` | 数据源管控（连接工厂 / 探活 / 变更推送） | 本仓只**消费** `DataSourceDef`，不自己管数据源生命周期 |
| `z-schedule` | 定时调度 | 本仓不自造调度器，触发交 z-schedule |

> 上表是简化版；D5 要求的**正式分工文案**（进两仓 README）由 TASK-20261012-016 收口。

**明确不做**：流式 CDC、Kettle 级拖拽可视化、触发器方案、BPMN 化编排、分布式 master-worker
（M1 单机线程池起步）、自造调度器。

## 坐标与发版口径

- `groupId` = `io.github.yuku123`，`parent` = `io.github.yuku123:z-boot-parent:1.1.0`，
  版本走 `${revision}`（现 `1.0.0`）+ `flatten-maven-plugin`（oss 模式），`-Pcentral` profile **dormant**。
- ⚠️ **本仓尚未发布到 Maven Central**（本轮不发布）。远端仓（`z-opc-foundation/z-etl`）**尚未创建**，
  本地仓已建好并完成提交，推送待人工/调度者拍板（建远端仓 + 定可见性属对外动作）。
- ⚠️ `groupId` 口径的由来见根 pom 注释：卡面写的是 `com.zifang`，但同一句又要求「对齐 z-dms 的发版口径，
  先读 z-dms 的实际做法再定」——而 z-dms 仓本轮**尚未落地**（TASK-20261012-011 仍在 `pending/`），
  无可照抄的现成做法，故按卡给的形状参考 z-trade 的实际 pom 做法落定（`io.github.yuku123` + z-boot-parent）。
  z-dms 落地后如口径不同，只需改三处（根 pom groupId + 两个子模块的 `<parent>`）。

## 相关任务

| 任务 | 内容 | 状态 |
|---|---|---|
| TASK-20261012-011 / 012 | z-dms 仓骨架与形态 B 管控 | 未落地（本仓用 `DataSourceProvider` SPI 承接） |
| TASK-20261012-014 | 增量手段：水位轮询 + 持久化断点续传（D3） | 未开始（本仓已有水位骨架，细节待该卡） |
| TASK-20261012-015 | 转换深度：z-util-expr 行级 + z-script 沙箱（D4） | 未开始（transform 链已留算子挂载点） |
| TASK-20261012-016 | 与 z-mq / z-lc 的分工成文进两仓 README（D5） | 未开始 |
