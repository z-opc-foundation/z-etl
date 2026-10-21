# 001_arch · z-etl 架构总览

> 落点：需求 008「数据集成（z-dms + z-etl）」的 z-etl 侧 M1 骨架（TASK-20261012-013）。

## 1. 一句话

z-etl 是一个**单机线程池起步**的 ETL 引擎：把「作业描述（JSON/yml）」翻译成
**reader → transform 链 → writer** 的执行流水线，管住「全量首跑 → 自动切增量」两段模式、
**幂等重跑**与**搬运后双口径对账**。

## 2. 分层

```
┌──────────────────────────────────────────────────────────────┐
│ z-etl-web（管控面）                                            │
│   REST: 登记 / 触发 / 看水位 / 看履历                          │
│   EtlModuleDataSource: z.base.db.etl.* + @MapperScan → 本域    │
└───────────────────────────┬──────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│ z-etl-core（引擎）                                              │
│                                                               │
│   job/       JobDescriptor + Parser(JSON/yml) + Validator      │
│   spi/       DataSourceProvider（z-dms 的接入点）              │
│   reader/    JdbcRowReader：分页游标 + 水位过滤                │
│   transform/ TransformChain → FieldMappingTransformer(类型归一) │
│   writer/    JdbcUpsertWriter：批量 upsert + 幂等键             │
│   checkpoint/水位存储（落库 etl_job_checkpoint）               │
│   reconcile/ Reconciler：行数 + 校验和双口径                    │
│   engine/    EtlEngine（编排）+ JobRepository（落库）+ EtlSchema│
└──────────────────────────────────────────────────────────────┘
```

## 3. 数据流

```
z-dms DataSourceDef ──(DataSourceProvider)──► 源库
                                                │
                          JdbcRowReader（分页 + 水位过滤）
                                                │  Map<列,值>
                          FieldMappingTransformer（改名 + 类型归一）
                                                │  Map<目标列,值>
                          JdbcUpsertWriter（按幂等键 upsert）
                                                │
                                                ▼
                                             目标库
```

跑完做**对账**（源/目标各扫一次：行数 + 幂等键多重集校验和），再落**水位**（首跑落全量水位 ⇒ 下次自动切增量）。

## 4. 引擎自身的三张表

| 表 | 用途 | 落点 |
|---|---|---|
| `etl_job_def` | 作业定义（描述 JSON + yml 两份文本） | 「可落库、可重跑」 |
| `etl_job_checkpoint` | 水位（列 / 类型 / 值 / 上次模式 / 时间） | 「首跑后自动切增量」「断点续传」 |
| `etl_job_run` | 运行履历（读/写行数、对账结果、水位前后值） | 观测与排障 |

DDL 见 `com.zifang.z.etl.core.engine.EtlSchema`（`CREATE TABLE IF NOT EXISTS`，幂等）。

> ⚠️ 这三张表属于**引擎自身的元数据库**（`z.base.db.etl.*`），与「被搬运的业务表」是两回事：
> 前者本仓自建自管；后者本仓**只读不建**。

## 5. 关键设计取舍

| 取舍 | 决定 | 理由 |
|---|---|---|
| 分页 vs 流式 | 分页游标（`LIMIT ? OFFSET ?`），每页物化 | 零件面（`DynamicQuery.list()`）没有流式口（notes §9.6.1 E3）；三方言同写法，不需要逐方言分叉 |
| 幂等 | 目标表幂等键列上**唯一约束** + upsert | 零件面只有 insert/update，无 upsert、无幂等键（notes §9.6.1 E7） |
| 类型归一 | 引擎层声明式规则（`type: LONG/STRING/BIGDECIMAL/...`） | JDBC 给原生类型、Excel 给全 String、内存 SQL 给浮点，三套语义必须各自落规则（notes §9.6.1 E1） |
| 金额 | 一律 `BIGDECIMAL` | 内存 SQL 是浮点语义，实测 `80.00*1.13=90.39999999999999`（notes §9.2.3） |
| 水位 | 引擎层自建（`etl_job_checkpoint`） | 五个零件面无位点概念；`maxRows` 是静默截断不是水位（notes §9.6.1 E2 / E5） |
| 对账 | 两侧整表扫，行数 + 键多重集校验和 | 与顺序无关的校验和能同时抓住漏行与重行 |
| 标识符 | 全过 `Identifiers.require()` 白名单 | 表名/列名只能拼进 SQL 文本，注入面在名字域（notes §9.1.1） |
| 调度 | 不自造 | 触发交 z-schedule（需求 008「不包含」） |

## 6. 已知边界（M1 口径）

- 增量目前只有「水位列 > 上次水位」这一种形态（LONG / TIMESTAMP），**断点续传的批内位点**属 TASK-20261012-014；
- transform 链目前只有字段映射一种算子，**行级表达式（z-util-expr）与脚本沙箱（z-script）**属 TASK-20261012-015
  （链骨架已留算子挂载点：新增算子实现 `RowTransformer` 挂进 `TransformChain` 即可）；
- **LUA 不开放**（沙箱无超时、`os.execute` 实测能逃逸），沙箱无内存配额 ⇒ 内存约束靠批量上限 + 作业超时兜底
  （notes §9.5.3）；
- 不做分布式 master-worker，不做 CDC，不做可视化编辑器。
