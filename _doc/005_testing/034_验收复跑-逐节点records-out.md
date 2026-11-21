# 034 验收复跑 —— 逐节点 records-out

## 口径

1. `records-out`：节点自身经主输出口发出的行数（执行器在 FanoutOutput 主输出处打点），与下游视角的 `records-in` 对称 —— 两者相减就是行数变化。
2. 对行数膨胀 / 收缩算子一眼可见：explode 进 2 行出 3 行 ⇒ `records-in=2 / records-out=3`（配合 `exploded` 计数定位膨胀来源）。
3. 汇节点没有出边 ⇒ 无 `records-out`（主输出口没被调用）。
4. 旁路输出另有 `side-output-sent` / `side-output-dropped` 计数，不混进 `records-out`。
5. web 层 `history` 的 nodeMetrics 同步可见（fm `records-in=5 / records-out=5` 对称）。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core -Dtest='ExplodeOperatorTest'
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-web -Dtest='EtlPipelineMetricsTest'
# 预期：4 + 1 个用例全绿
```

## 用例

| 用例 | 断言 |
| --- | --- |
| ExplodeOperatorTest.arrayExplode | `records-in`=2、`records-out`=3（进 2 出 3） |
| EtlPipelineMetricsTest.historyExposesMetrics | fm `records-in`=5、`records-out`=5 |

全量：`mvn -o test` → 211 个用例（core 205 + web 6）。

## 注意

- `records-out` 在有多条出边时按「次」计（同一条记录扇出到 N 个下游计 1 次发出 —— 打点在扇出之前）。
