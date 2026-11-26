# 042 验收复跑 —— 路由 / 透视 / 画像 / flatMap 灵活性四件套

- 日期：2026-11-26
- 波次：z-etl 优化战役（灵活性向）——「完全自由的各种处理各种数据」
- 结论：**全绿**。core 253 + web 13 = **266 用例**（上波 252，本波 +14），BUILD SUCCESS。

## 本波落了什么

| 增量 | 类型串 / 形态 | 设计参考 | 一句话 |
|---|---|---|---|
| 路由 | `route`（变换，无主输出） | Kafka Streams `branch()` / Flink 侧输出 split | `rules` 声明序判定**命中即走**（一条行只进一个分支），`fallback` 兜底；分流全走旁路标签，声明出边校验器报错 |
| 一进多出 | `script` 的 `mode: flat` | Flink FlatMapFunction | 脚本返回数组逐元素出一行：Map 元素整行替换、标量落 `name` 列、null 元素跳过计数、非数组返回值当单元素 |
| 行转列 | `pivot`（变换，物化） | Spark `pivot`（unpivot 的对偶） | `keyBy` 分组 + 透视列值变列名 + `value`/`fn` 聚合；缺格补 null；显式 `columns` 定列序；fn=COUNT 可省 value |
| 列画像 | `profile`（变换，透传） | Spark `describe` / pandas `describe()` | 行原样透传不改一字，流尾按列出 `column/rows/nulls/distinct/min/max/mean` 画像行走 `reportTo` 旁路 |

算子总数 51 → **54**（源 11 / 变换 27 / 有状态 7 / 汇 9）。

## 复跑

```bash
cd z-etl
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o clean test
# Tests run: 253 (core) + 13 (web), BUILD SUCCESS
```

新增用例（14）：

- `RouteOperatorTest`（4）：顺序判定命中即走 + 逐目标计数 / fallback 兜底与无 fallback 丢行计数 / GROOVY 按条声明引擎 / 校验（缺 rules、rule 缺 to、声明出边）。
- `ScriptFlatModeTest`（4）：Map 数组整行多出 / 标量数组落 name 列（原行保留）/ null 结果丢行 + null 元素跳过 + 非数组当单元素 / 校验（flat 与 type 互斥、标量没 name 报错）。
- `PivotOperatorTest`（3）：基本透视（BigDecimal 聚合 + 缺格补 null + 首现序）/ 显式 columns + COUNT 免 value + 全局透视 / 校验（缺 pivot、fn=SUM 缺 value、COLLECT_LIST 不在白名单）。
- `ProfileOperatorTest`（3）：透传不改行 + 画像行全字段（含中文码点序 min/max 与数值 mean）/ distinctCap 封顶 + 无 reportTo 只落指标 / 校验（distinctCap 非法）。

## 实施中发现并根修的两个坑

1. **死节点摘除误杀「无主输出」算子的上游**（`EliminateUnusedRewriter`）：
   route 的目标节点（vip/normal）不连声明边、只靠旁路标签收行 ⇒ 反向可达铺开到不了
   route 节点与其上游，整段被当死节点摘掉 ⇒ 运行报「没有源节点」。
   **根修**：旁路声明（`rejectedTo`/`lateTo`/`duplicateTo`/`route.rules[].to`+`fallback`/
   `profile.reportTo`）当**虚拟边**（目标 → 声明节点）参与反向铺开；旁路目标自身也保活。
2. **声明校验跑在优化之后**：配置错误（如 route rule 缺 `to`）会先被死节点摘除搅局，
   最后报出「没有源节点」这种迷惑性错误。**根修**：`PipelineExecutor` 先
   `PipelineValidator.validate(spec)`（原始声明，报「错在哪」），优化后的最终计划再校验一道兜底。

## 口径备注

- `route` 节点**无主输出**：分流全走旁路标签（标签 id = 目标节点 id，与 filter.rejectedTo
  同一套机制）；声明出边 = 配置错误（校验器直接报错，因为出边永远收不到行）。
- `script` `mode: flat` 与 `type` 互斥（要类型归一在下游挂 `type.normalize`）；标量元素必须
  声明 `name`（明确报错，不造 `value` 之类的魔法列）。
- `pivot` 聚合函数白名单不含 COLLECT_*（每个透视格收数组太容易撑爆）；数值统一走
  Accumulator 的 BigDecimal 语义。
- `profile` 画像行只走 `reportTo` 旁路，**绝不混进主输出**（主输出永远是透传的数据行）；
  distinct 超 `distinctCap`（缺省 10000）后不再计数（宁可少报不撑内存）。
