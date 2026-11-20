# z-etl

ETL 引擎仓。两种玩法，一套零件：

1. **线性作业**（DataX 式）：job = **reader → transform 链 → writer**，用 JSON / yml 描述；
2. **Flink 式数据流管道**（D2 起）：把整个处理过程声明成一张 **DAG**（`jdbc.source → filter/map/script → aggregate/window.aggregate/dedup/join → jdbc.sink`），
   引擎按 **记录一条一条推** 的流处理口径执行——键控状态、事件时间水位、窗口、旁路输出、
   状态快照恢复、断点续传全都有（详见下文「数据流管道（Flink 式）」）。

> 需求 008「数据集成（z-dms + z-etl）」的 M1 主体之一。本仓是 **z-dms（数据源管控）** 的第一个重度消费者
> （D2 拍板）：z-dms 落地即被 z-etl 消费。

## 结构

```
z-etl/
├── pom.xml                    顶层 parent（自包含，CI-friendly ${revision} + flatten oss）
├── z-etl-core/                引擎核心
│   └── src/main/java/com/zifang/z/etl/core/
│       ├── job/               作业描述模型（JobDescriptor + 解析器 + 校验器）—— 线性作业
│       ├── pipeline/          流水线声明模型（PipelineSpec/Node/Edge + 解析 + 校验 + DAG 拓扑）
│       ├── runtime/           流式执行内核（Record/Operator/StateBackend/TimerService/水位 + PipelineExecutor）
│       ├── operator/          算子集（source/filter/map/script/aggregate/window.aggregate/dedup/join/union/sinks）
│       ├── connector/         连接器（文件 CSV/JSON/XLSX、HTTP、内联、脚本、消息 + PullSource 骨架 + 类型推断；线性层与 DAG 层共用）
│       ├── optimize/          计划优化器（过滤下推 / 投影下推 / 聚合下推 / 合并映射 / 死节点摘除 + explain()）
│       ├── spi/               DataSourceProvider（数据源解析 SPI）+ MessageBus（消息总线 SPI）
│       ├── reader/            取数器（分页游标 + 水位过滤 + SQL 下推 + 流式拉取 fetchNextPage）
│       ├── transform/         变换链（字段映射 → 行级表达式 → 脚本沙箱 → 类型归一）—— 线性作业用
│       ├── writer/            写出器（批量 upsert + 幂等键）
│       ├── checkpoint/        水位存储（落库 / 内存两实现）
│       ├── reconcile/         对账器（行数 + 校验和双口径）
│       ├── engine/            旧执行器 EtlEngine + 作业仓储（落库）+ 引擎自身库表
│       └── util/              校验和、行内取值、时长解析助手
└── z-etl-web/                 管控面：job 登记 / 手动触发 / 水位与对账查询（REST）
    └── src/main/java/com/zifang/z/etl/web/
        ├── config/            EtlModuleDataSource（z.base.db.etl.* + @MapperScan 绑本域 sqlSessionFactory）
        └── api/                EtlJobController + EtlJobService + EtlPipelineController + EtlPipelineService
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

### 作业层的输入 / 输出类型（与 DAG 层同一批连接器）

`source.type` / `target.type` 缺省是 `jdbc`；写成别的类型时，**跑的就是 DAG 层那个连接器算子**
（不是另写一套实现），连接器私有配置放 `options`：

```yaml
job:
  name: csv-to-api
  mode: auto
  source:
    type: file.csv                       # jdbc（默认）/ file.csv / file.json / file.xlsx / file.xml / http / inline / script / message
    watermarkColumn: updated_at          # 非 JDBC 源的事件时间列 ⇒ 也能按水位自动切增量
    options:                             # 连接器私有配置
      path: /data/orders.csv
      columnTypes: {amount: BIGDECIMAL, updated_at: TIMESTAMP}
  transform:
    fieldMappings:
      - {source: id,         target: id,     type: LONG}
      - {source: amount,     target: amount, type: BIGDECIMAL}
  target:
    type: http
    options:
      url: https://api.internal/orders
  idempotencyKey: [id]
  watermark:
    column: updated_at
    type: TIMESTAMP
```

- 非 JDBC 侧**不解析数据源引用**（`dataSourceRef` / `table` 不用写）；JDBC 专属配置
  （`where` / `orderBy` / `pushdown` / `columns`）在非 JDBC 侧会被**直接报错**，不静默忽略。
- 对账：两侧都是 JDBC 时走整表扫描（行数 + 键多重集校验和）；任一侧不是 JDBC 时走**本轮行内口径**
  （源行 vs 写出行的幂等键多重集），履历里会注明。
- 断点续传口径不变：页级提交（写成功才推进水位），文件 / HTTP / 消息源同样按「水位 + 平局列」接力。

### 调度登记（cron）

z-etl **只登记，不起常驻定时线程** —— 定时器由宿主驱动（z-ops 定时任务 / k8s CronJob 调
`POST /api/etl/pipelines/{name}/run`），ETL 进程不背调度职责，也不用管自己的存活。

| 项 | 口径 |
|---|---|
| 表达式 | 5 段 cron（分 时 日 月 周）；支持 `*`、`*/n`、`a-b`、`a-b/n`、`a,b,c` |
| 落点 | `etl_pipeline_schedule`（pipeline_name 主键 + cron + 上次触发时间） |
| 可观测 | 登记里带**下一次触发时间**（本仓自算，供宿主对表与排障） |
| 记账 | 每次成功触发后更新「上次触发时间」 |
| 日 / 周 | 都写了且都不是 `*` 时取**并**（cron 惯例） |

### 失败重试（作业级 / 流水线级）

```yaml
retry:                        # 作业级：写在 job 顶层；流水线级：写在流水线声明顶层
  maxAttempts: 3              # 总尝试次数（含第一次）；默认 1 = 不重试
  backoffMs: 1000            # 首次重试前等待
  multiplier: 2.0            # 指数退避倍数
  maxBackoffMs: 60000        # 退避上限
```

- 不声明 `retry` ⇒ 行为与从前**完全一致**（异常原样抛出，类型与信息都不变）。
- 重试是**整体重跑**：水位只在本页写成功后推进 ⇒ 重跑从上次成功页接力；目标侧是幂等 upsert ⇒ 不重不漏。
- 重试用尽 ⇒ 抛「连续 N 次失败」并带上最后一次的原始原因。

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

## 数据流管道（Flink 式）

旧线性 job 之外的第二种玩法：整条处理过程声明成一张 **DAG**，引擎按 **记录一条一条推** 的流处理口径执行
（设计参考：Flink 的 DataStream/窗口/水位/键控状态，Kestra/n8n 的命名空间算子类型串，
Kafka Streams 的滚动聚合口径，Polars 的 `explain()`）。

```yaml
name: order-minute-rollup
mode: auto
nodes:
  - id: src
    type: jdbc.source                       # 源：分页游标 + SQL 下推 + 复合水位游标（与线性作业同一套零件）
    config:
      dataSourceRef: ds-oc-main
      table: z_trade_order
      pageSize: 200
      eventTimeColumn: updated_at           # 事件时间 ⇒ 水位 = 最大事件时间 − maxOutOfOrderness
      eventTimeType: TIMESTAMP
      maxOutOfOrderness: 30s
      resume: {column: updated_at, type: TIMESTAMP, tiebreakColumn: id, overlap: 0}
  - id: paid
    type: filter                            # 过滤：EL/GROOVY 表达式；未命中可走 rejectedTo 旁路（死信）
    config: {expression: "status == 1", rejectedTo: dead}
  - id: enriched
    type: map                               # 表达式产新列（EL 默认 / GROOVY）
    config:
      columns:
        - {name: amount_cny, engine: GROOVY, expression: "amount * 1.13", type: BIGDECIMAL}
  - id: cleaned
    type: script                            # 自由脚本（z-script 沙箱；产一列或整行替换 mode: row）
    config:
      name: pay_channel
      engine: GROOVY
      script: "def raw = payment_method\n(raw == null ? '' : raw.toString().trim().toUpperCase())\n"
  - id: rollup
    type: window.aggregate                  # 窗口聚合：事件时间窗口 + 水位触发 + 允许迟到
    config:
      keyBy: [pay_channel]
      window: {type: tumbling, size: 5m}    # tumbling / sliding / session / global
      allowedLateness: 1m
      aggregates:
        - {fn: COUNT, column: "*", alias: order_cnt}
        - {fn: SUM, column: amount, alias: total_amount}
  - id: sink
    type: jdbc.sink
    config:
      dataSourceRef: ds-etl-landing
      table: etl_order_rollup
      writeMode: upsert
      idempotencyKey: [pay_channel, windowStart]
  - id: dead
    type: console.sink
edges:
  - {from: src, to: paid}
  - {from: paid, to: enriched}
  - {from: enriched, to: cleaned}
  - {from: cleaned, to: rollup}
  - {from: rollup, to: sink}
```

执行（旧线性 job 也能编译成管道跑，`PipelineSpec.fromJobDescriptor`）：

```java
PipelineSpec spec = PipelineParser.parse(yamlOrJson);
PipelineRunResult result = new PipelineExecutor(dataSourceProvider).execute(spec);
result.getRecordsRead();          // 读出行数
result.getRecordsWritten();       // 写出行数
result.getNodeMetrics();          // 每节点计数（records-in / elapsed-nanos 逐节点耗时 / side-output 等）
result.getWatermarkAfter();       // 本轮落下的水位
```

**混合输入输出**（文件进 → HTTP 出，一样是一张 DAG）：

```yaml
name: file-to-api
nodes:
  - id: csv
    type: file.csv.source                 # 输入：CSV 文件（表头 + 类型推断 + 事件时间增量）
    config:
      path: /data/orders.csv
      columnTypes: {amount: BIGDECIMAL, updated_at: TIMESTAMP}
      eventTimeColumn: updated_at
  - id: clean
    type: script                          # 中间：自由脚本清洗
    config: {name: channel, engine: GROOVY, script: "payment_method.toUpperCase()"}
  - id: out
    type: http.sink                       # 输出：POST 给外部接口
    config: {url: "https://api.internal/orders", format: json}
edges:
  - {from: csv, to: clean}
  - {from: clean, to: out}
```

### 算子表（类型串：`家族.算子`，下划线/中划线视作点）

| 类型串 | 族 | 干什么 | 关键 config |
|---|---|---|---|
| `jdbc.source` | 源 | 分页游标拉数 + SQL 下推 + 事件时间水位 + 复合游标断点续传 | `table/columns/where/orderBy/pageSize/eventTimeColumn/eventTimeType/maxOutOfOrderness/resume/pushdown` |
| `file.csv.source` | 源 | CSV 文件（表头 / 分隔符 / 引号可配；未声明列类型按内容推断） | `path/delimiter/quote/header/encoding/columns/columnTypes/skipRows/limit/eventTimeColumn` |
| `file.json.source` | 源 | JSON 文件（`ndjson` 一行一个 / `json` 数组；`itemPath` 定位数组） | `path/format/itemPath/columns/columnTypes/encoding/eventTimeColumn` |
| `file.xlsx.source` | 源 | Excel 第一张工作表（**零依赖**：JDK 自带 zip + StAX；日期样式还原成 Timestamp） | `path/sheet/header/columns/columnTypes` |
| `file.xml.source` | 源 | XML 文件（**零依赖** StAX 流式解析；`recordPath` 定位记录元素；属性成 `@列`，嵌套成 Map，重复子元素成 List） | `path/recordPath/encoding/columns/columnTypes/limit` |
| `http.source` | 源 | REST / JSON 接口当流拉（`pageParam` 翻页到空页停，可限主机） | `url/method/headers/body/itemPath/pageParam/pageSize/maxPages/allowedHosts` |
| `inline.source` | 源 | 声明里写死的静态行（字典 / 维表 / 夹具） | `rows: [{...}]/columnTypes` |
| `script.source` | 源 | z-script 沙箱脚本**生成**数据 | `script/engine/name/timeoutMs/maxOutputChars` |
| `message.source` | 源 | 从消息总线消费（Kafka / RocketMQ / 库表队列） | `topic/pollSize` |
| `pipeline.ref` | 源 | **子管道引用**（管道当视图）：引用已登记管道，子管道的汇全部换成 collect（只取产出不落地）、不读不写水位与状态；`params` 透传占位符；嵌套超 8 层报错 | `name/params/eventTimeColumn` |
| `filter` | 变换 | 表达式过滤；未命中可走旁路 | `expression/engine/negated/rejectedTo` |
| `map` | 变换 | 表达式产新列 | `columns: [{name,expression,engine,type}]` |
| `script` | 变换 | 自由脚本（z-script 沙箱）；产一列或整行替换 | `script/engine/mode/name/timeoutMs/maxOutputChars` |
| `async.map` | 变换 | **异步映射**（Flink AsyncFunction 口径）：每行异步外呼 HTTP 补列；`capacity` 容量背压（在途满阻塞等一个）、`outputMode` ordered/unordered、流结束 flush 把在途全发出；`onError` fail/skip | `url({列}模板)/method/headers/bodyColumns/fields/timeoutMs/capacity/outputMode/onError/allowedHosts` |
| `field.mapping` | 变换 | 字段映射（改名 + 落库类型） | `mappings: [{source,target,type}]` |
| `type.normalize` | 变换 | 类型归一（链尾收口） | `rules: [{column,type,scale}]` |
| `assert` | 变换 | **数据质量闸**：逐行判定 + 流结束结算；违规可失败 / 走旁路 / 只计数 | `mode/rejectedTo/expression/engine/inRange/allowedValues/notNullRate/unique/minRows/maxRows/exactRows` |
| `union` | 变换 | 多路合并（不写也行，多入边天然合并） | — |
| `explode` | 变换 | **炸行**（一行拆多行，Flink SQL 的 UNNEST 口径）：数组列逐元素出一行 / 字符串按 `delimiter` 切分；标量当单元素原样过；空数组/null 缺省丢并计数（`keepEmpty: true` 保留原行）；`target` 可把元素写新列、`indexColumn` 顺带写下标 | `column/target/indexColumn/delimiter/keepEmpty` |
| `topn` | 变换 | **截断**（按键 / 全局 Top-N，Flink Rank 的保守子集；批口径：缓存到流尾才排序产出，适合文件/有限流）：数值列按数值比、其余按字符串比、null 视为最小；同值稳定；产出行原样 | `orderBy/order/limit/keyBy` |
| `aggregate` | 有状态 | 键控聚合，滚动累计（Kafka Streams 口径：每条输入发一次） | `keyBy/aggregates: [{fn,column,alias}]` |
| `window.aggregate` | 有状态 | 事件时间窗口聚合：tumbling/sliding/session/global，水位触发，迟到重发；`lateTo` 把**超窗丢弃的迟到行**原样走侧输出（有去处，不只丢计数） | `keyBy/window{type,size,slide,gap,offset}/allowedLateness/lateTo/aggregates` |
| `dedup` | 有状态 | 按键去重（保留 first/last，带存活时间）；`duplicateTo` 把被挤掉的行原样走侧输出（有去处，不只丢计数） | `keyBy/keep/ttl/duplicateTo` |
| `join` | 有状态 | 两路按键关联（inner/left），右表列冲突加后缀；`leftKey`/`rightKey` 列名允许不同（按位置取值关联） | `left/right/leftKey/rightKey/joinType/rightSuffix` |
| `interval.join` | 有状态 | **区间连接**（Flink IntervalJoin 口径）：两路按「关联键 + 事件时间之差 ≤ within」两两配对，每对只发一次；水位到期清理缓存 | `left/right/leftKey/rightKey/within/joinType/rightSuffix` |
| `dim.join` | 有状态 | **维表关联**（Flink broadcast-state 口径）：右流即维表（同键替换**热更新**、只进状态不下传），声明 `ttl` 按水位过期陈旧条目，`select` 投影维表列 | `left/right/leftKey/rightKey/select/ttl/joinType` |
| `cep.pattern` | 有状态 | **模式匹配**（Flink CEP 口径的保守子集）：单流步骤序列 + 次数量词（`times` 缺省 1 / `n` / `n,` / `n,m`）+ 可选步骤（`optional` 0 或 1 次，跳过/吃到两条路径都活）+ 紧邻度（`strict` 声明在下一步上：中间插事件匹配即死，缺省宽松）+ 否定步骤（`notNext`：违禁事件一到匹配即死，不消费事件，拖尾的到流尾封口）+ `until` 终止条件（命中的事件不吃进该步，不销毁匹配）+ `within` 时间窗（水位过期清理）+ 流尾封口（剩余步骤全 optional/notNext 的匹配在 flush 时补产出）；产出行 = 最后事件的行 + 每步 `步骤名.列` 与 `步骤名#`；每键部分匹配上限防爆炸 | `keyBy/within/maxPartials/steps: [{name,expression,times,optional,strict,notNext,until}]` |
| `jdbc.sink` | 汇 | 批量 upsert/insert（幂等键） | `table/batchSize/writeMode/columns/idempotencyKey` |
| `file.csv.sink` | 汇 | 写 CSV 文件（表头 + 列顺序；可追加） | `path/columns/delimiter/quote/encoding/append` |
| `file.json.sink` | 汇 | 写 JSON 文件（`ndjson` / `json` 数组两种形态） | `path/format/columns/encoding/append` |
| `file.xlsx.sink` | 汇 | 写 Excel 文件（**零依赖**：zip + worksheet；时间列写回序列数 + 日期样式 ⇒ 用 xlsx 源读回来还是 Timestamp） | `path/sheetName/header/columns` |
| `file.xml.sink` | 汇 | 写 XML 文件（XMLStreamWriter 流式写；标量成子元素，null 成空元素，嵌套结构跳过） | `path/rootName/recordName/encoding/declaration/columns` |
| `http.sink` | 汇 | POST 到外部接口（webhook / 业务 API） | `url/method/headers/format(json\|form)/timeoutMs` |
| `message.sink` | 汇 | 发到消息总线（Kafka / RocketMQ / 库表队列） | `topic/keyColumn/keyField/format(json\|text)` |
| `console.sink` | 汇 | 打日志（调试） | — |
| `collect.sink` | 汇 | 收进内存列表（测试/预览） | — |

类型串别名（写起来更省事）：`csv.source` / `json.source` / `excel.source` / `rest.source` /
`kafka.source` / `kafka.sink` / `webhook.sink` / `mq.source` / `mq.sink` 等。

**输入输出两侧共用一条水位口径**：所有源都实现 `PullSource`（拉一条 + 报水位 + 报游标），
所有汇都实现 `CountingSink`（报写了多少行）⇒ 执行器对「换一种输入 / 输出」零改动。

**消息类输入输出走 SPI**（`com.zifang.z.etl.core.spi.MessageBus`）：宿主注入自己的实现即可换成
Kafka / RocketMQ；不注入时用默认实现 `JdbcMessageBus`（落 `etl_message_outbox` 表当队列，跨进程可读）。

**两条路共用同一批连接器**：DAG 层写 `type: file.csv.source`，线性作业层写 `source.type: file.csv`
—— 落到的是同一个算子类，行为一致（同一份文件解析、同一套水位口径、同一套增量过滤）。

安全口径与线性作业完全一致：LUA/JS 硬拒绝；GROOVY 走 z-script 沙箱（墙钟超时 + 产出上限）；
标识符全过 `Identifiers` 白名单；聚合函数白名单。

### 数据质量闸（`assert` 算子）

链上任一点插一道闸，**不合格的数据不许悄悄流下去**：

```yaml
  - id: gate
    type: assert
    config:
      mode: reject                # fail（默认，抛错中止）/ reject（违规走旁路）/ count（只计数）
      rejectedTo: dead            # mode=reject 时脏数据的去处
      expression: "amount > 0"   # 逐行判定（EL / GROOVY）
      inRange: {amount: [0, 1000000]}
      allowedValues: {status: [0, 1, 2]}
      notNullRate: {user_id: 1.0} # 流结束结算
      unique: [order_no]          # 撞上重复即违规
      minRows: 100                # 流结束结算
```

- **逐行判定**（边流边判）：`expression` / `inRange` / `allowedValues` / `unique`；
- **结算期判定**（流结束才知道，`close()` 时结算）：`minRows` / `maxRows` / `exactRows` / `notNullRate`；
- 违规计数落在节点指标里（`result.getNodeMetrics().get("gate").get("violations")`）；
- ⚠️ 结算期判定失败时**数据已经落库**（质量闸 ≠ 事务），但水位不落库 ⇒ 下轮重跑；
- 断言读的列会被投影下推**保活**（不会被裁掉）。

### 管道参数（`${name}` 占位符）

**同一份声明跑不同批次**：登记时写占位符，触发时给值。

```yaml
name: daily-orders
nodes:
  - id: src
    type: jdbc.source
    config:
      dataSourceRef: ds-oc-main
      table: z_trade_order
      where: "dt = '${bizdate}'"     # 占位符
      columns: [id, order_no, amount, dt]
```

- **内置参数**：`bizdate`（今天）/ `yesterday`（昨天）/ `now`（当前时间）；
- **触发时传参**：`POST /api/etl/pipelines/{name}/run` body `{"bizdate": "2026-11-19"}`；
  代码里 `engine.run(name, params)` 或 `PipelineParser.parse(text, params)`；
- **缺参数直接报错**（不拿空串糊过去 —— 空串会让 `WHERE` 变成恒真，增量悄悄变全量）；
- **参数值过白名单**（字母数字与 `_ . : / + - =`，无引号、无分号、无空格）⇒ 拼进 `where`
  也不会变成注入面；需要更自由的值就用 `script` 算子（Groovy 沙箱）；
- **落库的定义保留占位符** ⇒ 一份模板天天跑不同批次；参数只作用于本次运行；
- 流水线名**不支持**占位符（名字是水位 / 状态快照的键）。

### 试跑（`dryRun`）

定义完一条流水线，先跑 N 行看看链路通不通、产出对不对：

```java
PipelineRunResult trial = engine.dryRun("daily-orders", params, 100L);
trial.getRecordsWritten();   // 「试跑下来会写多少行」，不是真写了
```

- **汇只计数不落地**（不会把试验数据写进目标表）；
- **不读不写水位与状态快照**（试跑不能当增量起点，也不算正式运行、不落履历）；
- 最多处理 N 行（`?rows=100`）；源行数不足就跑完为止；
- 试跑**从全量跑**（不读水位 ⇒ 可预测：「第一次跑会是什么样」）。

### 保存点（savepoint：作业迁移）

口径照抄 Flink：把**键控状态快照 + 水位游标**打包成自描述 JSON 报文，搬去别的环境灌回（可换名）——A 环境跑到一半 → 导出 → B 环境灌回 → B 直接增量续跑：

```java
Savepoint sp = engine.exportSavepoint("daily-orders");   // 纯读，不动现场
String json = Savepoints.toJson(sp);
engineB.importSavepoint(json);            // 灌回同名管道（覆盖写）
engineB.importSavepoint(json, "daily-v2"); // 或换名迁移（作业复制）
```

- 报文自带版本号（当前 v1）+ 导出名 + 导出时间，`fromJson` 校验版本与基本形状，坏报文明确拒绝；
- **导出纯读**（状态与水位纹丝不动），**灌回覆盖写**（目标管道原有的状态与水位被替换）；
- 空管道（状态与水位都没有）导不出；无状态管道可以只搬水位；
- REST：`GET /api/etl/pipelines/{name}/savepoint` 导出、`POST .../savepoint/import` 灌回（body 可带 `targetName` 换名）。

### 执行语义（与 Flink 对齐的部分）

| 语义 | 口径 |
|---|---|
| 执行模型 | 单线程推送（源拉一条 → 沿 DAG 推到底），定序确定 ⇒ 结果可复现；多源轮询拉取 |
| 水位 | 全局水位 = 各**活跃**源水位的最小值；源结束不再拖住水位；无事件时间的记录不阻塞（归全局窗口） |
| 窗口 | `[start, end)` 左闭右开；session 窗口相交自动合并；窗口触发后同键再迟到 ⇒ `allowedLateness` 内重算重发，之外丢弃并计数；`lateTo` 声明后丢弃的迟到行**原样走侧输出**（标签 id = 目标节点 id，与 filter 的 `rejectedTo` 同一套旁路口径） |
| 键控状态 | `ValueState/ListState/MapState`（堆实现，按算子隔离，`setCurrentKey` 口径与 Flink 一致） |
| 定时器 | 事件时间定时器（水位驱动触发）+ 处理时间定时器 |
| 区间连接 | `interval.join`：两边都缓存，事件时间相差 ≤ `within` 的行按关联键两两配对（后到触发，每对只发一次）；水位 − within 之前的缓存清掉；没事件时间的行丢弃并计数 |
| 模式匹配 | `cep.pattern`：能延续旧匹配就延续、能开新匹配就开新匹配（同一事件两件事都做）；**推进即取代**（不做跳跃策略细分）；`optional` 步骤 0 或 1 次 —— 下一步是 optional 不吃/吃到都开「跳过」分支，两条路径都活着（宁多配不少配）；`strict` 紧邻度（声明在下一步上）—— 当前事件既不吃进也不推进 ⇒ 匹配死掉，缺省宽松；`notNext` 否定步 —— 不消费事件，未越过时违禁事件一到匹配即死（否定优先于续吃），拖尾的到流尾 flush 确认后才产出；`until` 命中的事件不吃进该步（部分匹配留着等推进，不销毁）；剩余步骤全 optional/notNext 的部分匹配在流尾 flush 时补产出（`flush-emitted`）；最后一步次数到下限即产出；`within` 声明后起步早于「水位 − within」的部分匹配清掉；每键部分匹配上限（缺省 64）防爆炸 |
| 异步 IO | `async.map`：Flink AsyncFunction 口径 —— 请求并发在途，行到达时先发已完成的、在途到 `capacity` 才阻塞等一个（容量背压）；`ordered` 按行到达序 / `unordered` 完成即发；流结束 `flush(Output)`（endOfInput）把在途全部发出；失败缺省 fail fast，`onError: skip` 丢行并计数 |
| 状态快照 | 跑完把全部键控状态序列化存进 `PipelineStateStore`（内存 / JDBC 落库），下轮恢复 |
| 断点续传 | 复用线性作业的水位游标（复合游标 + 回溯窗口），跑完落 `Checkpoint`，下轮从水位接力 |
| 模式 | `auto`（有快照/水位续跑，否则全量）/ `init`（清掉重跑）/ `incremental`（无水位直接失败） |

### 计划优化器（`optimize` 包，DataFusion 式固定规则循环）

| 规则 | 干什么 | 保守边界 |
|---|---|---|
| `PushDownFilterRewriter` | filter 节点显式声明的 SQL 谓词（`config.sql`）并入源 WHERE | 只沿「行保持」链（filter/map/script(落列)/type.normalize）上溯；链上有 `field.mapping` 就不推；EL/GROOVY **不翻译**成 SQL，不猜 |
| `MergeMapRewriter` | 相邻两个 `map` 并成一个（列清单合并，语义等价：表达式都在累积行上求值） | 上游有扇出/扇入不并 |
| `EliminateUnusedRewriter` | 到不了任何汇的死节点摘掉 | `rejectedTo` 指名的节点不算死 |
| `ProjectionPushdownRewriter` | 下游用得上的列回推到源库 SELECT 列表 | 只对 `jdbc.source` 生效；源必须**已声明列清单**；链上必须有 `field.mapping` 收敛输出列；**水位列 / 平局列必保**；join / 整行脚本 / 多路异构合并不裁 |
| `AggregatePushdownRewriter` | 源声明了 `pushdown.groupBy/aggregates` ⇒ 摘掉引擎侧重复计算的 `aggregate` 节点 | 下推**必须用户在源上显式声明**；聚合声明要与节点**逐条一致**（分组列集合 + fn/column/alias）；**下游列守卫**：下游用到的列必须都在「分组列 + 聚合别名」里，否则不摘；聚合下推与 `eventTimeColumn` **互斥** |

**投影下推的保守边界**（错了就是静默丢数据，所以逐条设卡）：
1. 只对 `jdbc.source` 生效 —— SQL 投影是确定的；文件 / HTTP / 消息源的 `columns` 语义各不相同，不碰；
2. 源**必须已声明列清单**：裁剪结果是「声明列 ∩ 需要的列」⇒ SQL 一定有效；没声明（全列）就不裁；
3. 链上必须有 `field.mapping`（整行替换语义 ⇒ 输出列确定 ⇒ 下游要什么才清晰）；没有它就不裁；
4. `eventTimeColumn` 与 `resume.tiebreakColumn` **必保** —— 裁掉 ⇒ 水位失效 ⇒ 增量悄悄变全量；
5. `join` / `script(mode=row)` / 多路异构合并 ⇒ 列集合不可知，不裁；
6. 表达式 / 脚本里的列引用按**候选**提取（不排除关键字），最终与源声明列求交 ⇒ 误判只会多留一列，不会少留。

**聚合下推的保守边界**（口径从「每条输入发一次」变成「每组一行」，错了就是静默丢数）：
1. 下推**由用户在源节点显式声明**（`pushdown.groupBy` + `pushdown.aggregates`）—— 引擎不猜「这个聚合能不能下推」；
2. 引擎侧 `aggregate` 节点的 `keyBy` / `aggregates` 必须与源声明**逐条一致**（分组列集合相同、fn/column/alias 相同）⇒ 不一致就保留引擎侧计算，只在备注里提示；
3. **下游列守卫**：引擎侧 aggregate 的输出是「整行 + 聚合别名」，下推后的源只输出「分组列 + 聚合别名」⇒ 下游若引用了别的列会拿到空值。所以摘节点前要确认下游链路用到的列全在输出列集里：精确列名（`field.mapping` 的 source、汇的 `columns`/`idempotencyKey`、`keyBy`、聚合列、类型归一列）不在 ⇒ 不摘；表达式里的标识符按**源表列清单**判定（运行时用 JDBC 元数据取；取不到 ⇒ 不摘，不猜）；整行脚本 / join ⇒ 引用不可知 ⇒ 不摘；
4. 聚合下推与 `eventTimeColumn` **互斥**（聚合结果是分组口径，水位过滤对它没意义）⇒ 声明了就报错；
5. 聚合表达式**不是自由 SQL**：函数名走白名单（`SUM/COUNT/AVG/MIN/MAX`）、列名走标识符白名单。

**运行时就会优化**：执行器跑之前先做一遍优化（改写副本，不动调用方的声明），优化备注写进运行结果。
优化器会拿 **JDBC 元数据**里的源表列清单（`JdbcTableSchemaResolver`）来判定「下游引用的列还在不在」——
拿不到就按「不知道」保守处理。
计划长什么样跑之前可以看：`new PlanOptimizer().explain(spec)`（拓扑序文本计划 + 优化备注）。

### 与 z-wf 的编排分工（管道之间归流程引擎）

z-etl 的 DAG 管**单条管道内部**的数据流；**管道与管道之间**的编排归 z-wf（自研流程引擎，坐标
`io.github.yuku123:z-wf-core`，**必须锁 2.x** —— 同坐标 1.x 是 Camunda 线引擎，见 z-wf README）：

- z-wf 流程的 `serviceTask` 挂一个 `WfJavaDelegate`，委托里调
  `new PipelineExecutor(provider).execute(PipelineParser.parse(yaml))`；
- 排他网关按流程变量分支：delegate 把搬运行数写回变量（如 `lastWritten`），
  网关条件 `lastWritten > 0` ⇒ 「搬到了东西才跑下游汇总，颗粒无收直接收工」；
- 注意：数据源 SPI / DataSource 这类连接性对象**不能放进流程变量**
  （z-wf 持久化会深拷贝变量 ⇒ `NotSerializableException`），走 delegate 构造注入。

跨仓集成验收：[`WfPipelineOrchestrationTest`](z-etl-core/src/test/java/com/zifang/z/etl/core/WfPipelineOrchestrationTest.java)
（正路：搬运 → 网关 → 汇总管道跑通落表；兜路：空源 ⇒ default 分支 ⇒ 汇总不跑）。
core 主代码不耦合 z-wf（依赖 test scope）；生产编排时把依赖升为 compile、delegate 交给 Spring 装配。

## 管控 REST

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/etl/jobs` | 登记作业（body = JSON 或 yml 描述原文） |
| GET | `/api/etl/jobs` | 作业名列表 |
| GET | `/api/etl/jobs/{name}` | 作业描述 |
| POST | `/api/etl/jobs/{name}/run` | 触发作业 |
| GET | `/api/etl/jobs/{name}/watermark` | 水位记录 |
| GET | `/api/etl/jobs/{name}/runs` | 运行履历 |

流水线（Flink 式 DAG）管控面 —— [`EtlPipelineController`](z-etl-web/src/main/java/com/zifang/z/etl/web/api/EtlPipelineController.java)：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/etl/pipelines` | 登记流水线（body = JSON 或 yml 声明原文） |
| GET | `/api/etl/pipelines` | 流水线名列表 |
| GET | `/api/etl/pipelines/{name}` | 流水线定义 |
| GET | `/api/etl/pipelines/{name}/plan` | 计划解释（拓扑序文本计划 + 优化备注，text/plain） |
| POST | `/api/etl/pipelines/{name}/run` | 触发流水线（body 可选 = 运行参数，如 `{"bizdate": "2026-11-19"}`） |
| POST | `/api/etl/pipelines/{name}/dry-run` | **试跑**（`?rows=100`：最多处理 N 行收工；汇只计数不落地；不读不写水位与状态快照；body 可选 = 运行参数） |
| GET | `/api/etl/pipelines/{name}/savepoint` | **导出保存点**（键控状态快照 + 水位游标打包成自描述 JSON；纯读不动现场） |
| POST | `/api/etl/pipelines/{name}/savepoint/import` | **灌回保存点**（覆盖写；body 可带 `targetName` 换名迁移） |
| GET | `/api/etl/pipelines/{name}/watermark` | 水位记录 |
| GET | `/api/etl/pipelines/{name}/runs` | 运行履历 |
| POST | `/api/etl/pipelines/{name}/schedule` | 登记调度（body = cron 表达式） |
| GET | `/api/etl/pipelines/{name}/schedule` | 调度登记（含下一次触发时间） |
| DELETE | `/api/etl/pipelines/{name}/schedule` | 取消调度 |
| GET | `/api/etl/pipelines/schedules` | 全部调度登记 |

流水线定义落 `etl_pipeline_def`（JSON + yml 双份）；**键控状态快照**落 `etl_pipeline_state`
（base64 TEXT，跨重启恢复 ⇒ 定时触发 / 崩溃后重跑从上次状态接力）；运行履历落 `etl_pipeline_run`。
装配见 [`EtlEngineConfiguration`](z-etl-web/src/main/java/com/zifang/z/etl/web/config/EtlEngineConfiguration.java)。

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
mvn clean install        # 2 个模块，Java 17 编译（代码保持 Java 8 风格）；发版改根 pom <revision> 一处
mvn -o test              # 195 个用例（core 192 + web 3）
```

> ⚠️ **跑测试请用 JDK 17**（pom target 17）：脚本沙箱用例依赖 z-script 的 Groovy 3.0.9，
> 在 JDK 21+ 上会因读不了高版本 class 文件而失败（`Unsupported class file major version 67`）。
> 例：`JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test`。

三项验收的落点与复跑命令见 [`_doc/005_testing/001_验收复跑.md`](_doc/005_testing/001_验收复跑.md)；
转换链（D4）三项验收见 [`_doc/005_testing/002_验收复跑-转换链.md`](_doc/005_testing/002_验收复跑-转换链.md)。

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

**明确不做**：流式 CDC、运行时事件流（产生即消费，归 z-mq —— 管道的「流式」指**记录级推送执行**，
数据仍是存量与批量）、Kettle 级拖拽可视化、触发器方案、BPMN 化编排、分布式 master-worker
（单机线程池起步）、自造调度器。

## 坐标与发版口径

- `groupId` = `io.github.yuku123`，`parent` = `io.github.yuku123:z-boot-parent:1.2.2`，
  版本走 `${revision}`（现 `1.0.0`）+ `flatten-maven-plugin`（oss 模式），`-Pcentral` profile **dormant**。
- ✅ **已发布到 Maven Central**（2026-10-30）：`z-etl-core:1.0.0` / `z-etl-web:1.0.0`。
  发版走 `mvn -Pcentral deploy`（**不要带 `-o`**，离线会静默跳过 publish goal），凭证与 gpg 口令在仓根 `.env`（不入库）。
- ⚠️ `groupId` 口径的由来见根 pom 注释：卡面写的是 `com.zifang`，但同一句又要求「对齐 z-dms 的发版口径，
  先读 z-dms 的实际做法再定」——而 z-dms 仓本轮**尚未落地**（TASK-20261012-011 仍在 `pending/`），
  无可照抄的现成做法，故按卡给的形状参考 z-trade 的实际 pom 做法落定（`io.github.yuku123` + z-boot-parent）。
  z-dms 落地后如口径不同，只需改三处（根 pom groupId + 两个子模块的 `<parent>`）。

## 文档目录

本项目文档统一收口在 `_doc/` 下（八类固定编号表，桶按需存在）:

- [`_doc/001_arch/`](_doc/001_arch/) — 架构总览
- [`_doc/003_script/`](_doc/003_script/) — 构建与发布脚本
- [`_doc/005_testing/`](_doc/005_testing/) — 验收复跑记录

## 相关任务

| 任务 | 内容 | 状态 |
|---|---|---|
| TASK-20261012-011 / 012 | z-dms 仓骨架与形态 B 管控 | 未落地（本仓用 `DataSourceProvider` SPI 承接） |
| TASK-20261012-014 | 增量手段：水位轮询 + 持久化断点续传（D3） | **已完成（2026-10-22）**：复合游标（`update_time` + 平局列 `id`，同秒多条不漏行）+ 页级提交推进水位（kill -9 后从断点续传）+ 游标越界明确失败不推进 + 时钟回拨滞留行可查可兜底（`overlap` 回溯窗口）；三项验收在本地 H2 实测通过，边界口径见 `_doc/005_testing/003_验收复跑-水位与断点续传.md`；binlog CDC 一行未写 |
| TASK-20261012-015 | 转换深度：z-util-expr 行级 + z-script 沙箱（D4） | **已完成（2026-10-22）**：四类转换各一条真实规则端到端跑通 + 沙箱超时/越限拦截 + 重跑幂等；LUA/JS 硬拒绝；沙箱无内存配额与 Groovy 的 JDK 版本边界已记入 `_doc/001_arch/00-overview.md` §6 |
| TASK-20261012-016 | 与 z-mq / z-lc 的分工成文进两仓 README（D5） | 未开始 |
