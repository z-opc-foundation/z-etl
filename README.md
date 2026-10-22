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
│       ├── reader/            取数器（分页游标 + 水位过滤 + SQL 下推）
│       ├── transform/         变换链（字段映射 → 行级表达式 → 脚本沙箱 → 类型归一）
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

## 转换链（四类算子 + 下推）

链上顺序固定：**字段映射 → 行级表达式 → 脚本沙箱 → 类型归一（链尾收口）**。
「SQL pushdown」不在链上，它作用在**取数阶段**（源库把活干完，引擎只拿结果）。

| # | 类别 | 声明位置 | 引擎 / 执行器 | 安全口径 |
|---|---|---|---|---|
| ① | 字段映射 | `transform.fieldMappings` | 引擎层 | 源列 / 目标列过 `Identifiers` 白名单 |
| ② | SQL pushdown | `source.pushdown` | 源库 | 过滤下推 + 列裁剪下推 + **聚合下推**（`groupBy` + `aggregates`，函数白名单 `SUM/COUNT/AVG/MIN/MAX`） |
| ③ | 行级表达式 | `transform.expressions` | `EL`（z-util-expr，默认）/ `GROOVY`（**z-script 沙箱**） | `LUA`/`JS` **硬拒绝**（无沙箱可逃逸 / 无 JS 引擎） |
| ④ | 类型归一 | `transform.typeNormalizations` | 引擎层 | 类型 + 定点小数位（`scale`）+ 空值策略 |
| — | 脚本沙箱（复杂清洗） | `transform.scripts` | `GROOVY`（默认）/ `EL`（z-script 沙箱） | 墙钟超时 + 单步产出上限；`LUA` **硬拒绝** |

```yaml
  source:
    pushdown:
      filter: "status = 1"        # 过滤下推
      groupBy: [status]           # 聚合下推（可选）
      aggregates:
        - {function: COUNT, column: "*",   alias: order_cnt}
        - {function: SUM,   column: amount, alias: total_amount}
  transform:
    fieldMappings:
      - {source: id,     target: order_id, type: LONG}
    expressions:
      - {name: status_text, engine: EL,    expression: "status == 1 ? 'PAID' : 'CLOSED'", type: STRING}
      - {name: gross_amount, engine: GROOVY, expression: "amount * 1.13", type: BIGDECIMAL}
    scripts:
      - name: pay_channel
        engine: GROOVY
        timeoutMs: 2000
        maxOutputChars: 65536
        script: "def raw = payment_method\n(raw == null ? '' : raw.toString().trim().toUpperCase())\n"
    typeNormalizations:
      - {column: gross_amount, type: BIGDECIMAL, scale: 2}
```

> ⚠️ **金额别用 EL**：z-util-expr 的 EL 算术走 double（`100.00*1.13 = 112.99999999999999`），
> 金额表达式请用 `GROOVY`（BigDecimal 精确，且经 z-script 沙箱），再用类型归一 `scale` 收口。
>
> ⚠️ **Groovy 沙箱有 JDK 版本边界**：z-script 用 Groovy 3.0.9，实测 **JDK 8 ✓ / JDK 17 ✓ / JDK 23 ✗**
> （脚本引用 JDK 类时报 `Unsupported class file major version 67`）⇒ 运行 JDK 需 ≤ 17。

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
mvn -o test              # 24 个用例（core 23 + web 1）
```

> ⚠️ **跑测试请用 JDK 8 或 17**：脚本沙箱用例依赖 z-script 的 Groovy 3.0.9，
> 在 JDK 21+ 上会因读不了高版本 class 文件而失败（`Unsupported class file major version 67`）。
> 例：`JAVA_HOME=$(/usr/libexec/java_home -v 1.8) mvn -o test`。

三项验收的落点与复跑命令见 [`_doc/002_acceptance/001_验收复跑.md`](_doc/002_acceptance/001_验收复跑.md)；
转换链（D4）三项验收见 [`_doc/002_acceptance/002_验收复跑-转换链.md`](_doc/002_acceptance/002_验收复跑-转换链.md)。

> ⚠️ 验收全部跑在**本地 H2 内存库**上（源库 / 目标库 / 引擎元库三个独立库）：
> 不连生产 RDS、不在 `oc` 库建表、不对任何真实库写入。

## 与邻居的边界（防功能漂移）

<!-- D5-BOUNDARY-BEGIN：本块与 z-etl/README.md、z-lc/README.md 的同名小节**逐字一致**，改一处必须改另一处 -->

三条分工边界（2026-10-12 D5 拍板）—— 每条都配一个「这个需求该找谁」的判例，
防止后来者按感觉选仓。

| # | 主题 | 谁管 | 边界一句话 |
|---|---|---|---|
| 1 | 运行时事件流 | `z-mq` | 事件**产生即消费**；**实时流不归 ETL**，ETL 只做**存量与批量**。 |
| 2 | 应用内处理器链 | `z-lc` pipeline | 表单 / 数据流的**应用内**处理器链归 z-lc；**ETL 不做应用内处理**，只做**跨系统、跨库**搬运。 |
| 3 | 取数渲染 vs 物化落地 | `z-lc` 报表子域 dataset（原 z-report） | dataset 管**取数渲染（查询态）**；**ETL 管物化落地（存储态）**。 |

### 判例：这个需求该找谁

| # | 需求（具体形态） | 找谁 | 不找谁 | 为什么 |
|---|---|---|---|---|
| 1 | 订单支付成功后，下游要**秒级**收到通知并立刻处理（持续流量） | `z-mq`（`z-mq-client` 发 / `z-mq-broker` 消费） | `z-etl` | 产生即消费的运行时事件流是 z-mq 的活；ETL 只做存量与批量，不做实时流。 |
| 2 | 表单提交前，按配置依次做**字典解析、类型转换、必填校验、外键引用检查**（都在同一个应用内） | `z-lc` 的 `core/pipeline`（processor 链；配置端点 `PipelineConfigController` `/api/lc/pipeline-config`） | `z-etl` | 应用内处理器链归 z-lc pipeline；ETL 是跨系统、跨库搬运，不承接应用内处理。 |
| 3 | 每晚把 A 库的订单**历史全量**搬进 B 库的汇总表并**落库** | `z-etl`（`z-etl-core` 的 reader → transform → writer 作业；管控端点 `EtlJobController` `/api/etl/jobs`） | `z-lc` 的 `z-lc-report-dataset`（`/api/lc/report/dataset`） | 物化落地是存储态，归 ETL；dataset 只做查询态取数渲染，不负责把结果写回存储。 |

<!-- D5-BOUNDARY-END -->

### 其它邻居（D5 三条之外，但同样防漂移）

| 邻居 | 它管什么 | 本仓的分界 |
|---|---|---|
| `z-dms` | 数据源管控（连接工厂 / 探活 / 变更推送） | 本仓只**消费** `DataSourceDef`，不自己管数据源生命周期 |
| `z-schedule` | 定时调度 | 本仓不自造调度器，触发交 z-schedule |

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
| TASK-20261012-015 | 转换深度：z-util-expr 行级 + z-script 沙箱（D4） | **已完成（2026-10-22）**：四类转换各一条真实规则端到端跑通 + 沙箱超时/越限拦截 + 重跑幂等；LUA/JS 硬拒绝；沙箱无内存配额与 Groovy 的 JDK 版本边界已记入 `_doc/001_arch/00-overview.md` §6 |
| TASK-20261012-016 | 与 z-mq / z-lc 的分工成文进两仓 README（D5） | 未开始 |
