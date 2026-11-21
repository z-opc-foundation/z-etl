# CEP 与 Flink CEP 的口径对照

> 结论先行：z-etl 的 `cep.pattern` 是 Flink CEP 口径的**保守子集** —— 组合语义齐全
> （次数量词 / optional / strict / notNext / until / within / 流尾封口），
> 但匹配策略取「宁多配不少配」的保守线，不做 Flink `afterMatchSkipStrategy` 的细分。

## 一、逐项对照

| Flink 概念 | z-etl 对应 | 口径差异 |
| --- | --- | --- |
| 次数量词 `times(n)` / `oneOrMore` / `timesOrMore` | `times: "n"` / `"n,"` / `"n,m"` | 一致；末步未封顶时部分匹配继续活着吃同类事件 |
| `optional()` | `optional: true` | z-etl **两条路径都活**：吃到与跳过各开分支（Flink 走 NFA 同样双路径，语义等价） |
| `next()`（严格紧邻） | `strict: true`（声明在下一步上） | 一致：中间插事件 ⇒ 部分匹配死掉；当前步续吃不算插事件 |
| `followedBy()`（宽松） | 缺省（不声明 strict） | 一致：插事件照样等 |
| `notNext()` | `notNext: true` | z-etl **不销毁匹配，只挡消费**——比 Flink 更保守；否定优先于续吃 |
| `notFollowedBy()` | 未提供 | 可用 `notNext` 置于段中近似；全段否定暂不做 |
| `within()` | `within`（首尾事件时间差上限） | 一致；水位过期清理（起步早于「水位 − within」清掉） |
| `until()` | `until`（步骤级表达式） | Flink 销毁匹配，z-etl 只挡消费（部分匹配留着等推进）—— 保守线 |
| 流结束语义 | `flush` 流尾封口 | Flink 无此概念；z-etl 面向有限流：剩余步骤全 optional/notNext 的匹配在流尾补产出 |
| `afterMatchSkipStrategy` | **未提供** | 见下节：`推进即取代` + 多路径并活，宁可多配 |

## 二、为什么不做 skip 策略细分

Flink 的 `skip-till-next / skip-till-any / no-skip` 决定的是「同一事件序列命中多条路径时，
**只留哪一条**」。z-etl 的保守口径是反过来的：

- **推进即取代**：同一事件把部分匹配推进到下一步 ⇒ 原匹配丢弃（沿一条路径不重复产出）；
- **多路径并活**：同一事件既能让旧匹配续吃、又能开新匹配 ⇒ 两条都保留（CEP 单流多路径本来就有歧义，歧义时宁可多配）；
- **去重交给下游**：不想多配就在 `cep.pattern` 后面接 `dedup`（键口径去重）或 `topn`。

这条线的代价是可能多出匹配行，收益是**永远不漏配** —— ETL 场景里漏配的代价（告警漏报、
账务漏单）远高于多配（下游去重即可兜住）。

## 三、验收索引

- 基础序列 / 次数量词 / within / maxPartials：`CepPatternOperatorTest`（018）
- optional 双路径 / 流尾封口：`CepOptionalStepTest`（022）
- strict 紧邻度：`CepStrictContiguityTest`（023）
- notNext 否定：`CepNotNextStepTest`（024）
- 中步量词组合：021
- 状态迁移兼容（快照恢复续配）：`CepStateMigrationTest`（031）
