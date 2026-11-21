# 029 验收复跑 —— COLLECT_LIST / COLLECT_SET 聚合

## 口径

1. `COLLECT_LIST` 把值收成数组（滚动累计）：`aggregate` 每条输入发一次，产出为**当时快照**（给拷贝，不给内部活列表引用）。
2. `COLLECT_SET` 去重收集：同值只收一次、保持插入序；其余口径与 COLLECT_LIST 完全一致（封顶 / null 跳过 / 快照 / 合并）。
2. null 不收进数组（SQL collect_list 口径）；数组元素经 normalize（数值转 BigDecimal、其余转字符串）保证可序列化。
3. 封顶防状态撑爆：单键收集上限 65536（`Accumulator.MAX_COLLECT`），超出丢并计数 `collect-truncated`（增量口径，读后清零）。
4. 状态合并（会话窗口合并 / 检查点恢复）：collected 逐项并入，同样受封顶约束。
5. 校验：`COLLECT_LIST` 必须指名收哪一列（收 `*` 报错）；白名单报错文案带上 COLLECT_LIST。
6. SQL 聚合下推**不支持**收集 —— `AggregateSpec.SAFE_FUNCTIONS`（线性层 / 源下推）仍限 `SUM/COUNT/AVG/MIN/MAX`。
7. 配下游 `explode` 可以「聚合再还原」；`COUNT(*)` 数所有行（含空值行），与收集口径互不影响。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core \
  -Dtest='CollectListAggregateTest'
# 预期：Tests run: 4, Failures: 0, Errors: 0
```

## 用例

| 用例 | 输入 | 断言 |
| --- | --- | --- |
| rollingCollectSkipsNull | 甲: tag a / null / b | 3 行滚动产出：[a] → [a]（null 跳过）→ [a,b]；COUNT(*) = 3 |
| collectSetDedup | 甲: tag a / a / b | 3 行滚动产出：[a] → [a]（同值只收一次）→ [a,b]（插入序） |
| cappedCollect | 直接驱动 Accumulator 70000 次 | 收集封顶 65536；collectDroppedDelta = 4464；读后清零 |
| validation | COLLECT_LIST / COLLECT_SET 收 * | IllegalArgumentException 点名对应函数 |

全量：`mvn -o test` → 204 个用例（core 199 + web 5）。

## 用法示例

```yaml
- id: agg
  type: aggregate
  config:
    keyBy: [user]
    aggregates:
      - {fn: COLLECT_LIST, column: tag, alias: tags}   # 收集成数组
      - {fn: COUNT, column: "*", alias: n}
- id: split
  type: explode            # 聚合再还原：数组炸回多行
  config:
    column: tags
    target: tag
```

## 注意

- 快照口径是本档修正的关键：产出给拷贝，否则滚动聚合每行的数组都会「一起长大」。
- 收集列基数大就别收（封顶只是兜底，不是容量承诺）。
