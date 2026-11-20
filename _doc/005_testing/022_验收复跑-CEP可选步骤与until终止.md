# 022 验收复跑 —— CEP 可选步骤与 until 终止

## 口径

1. `optional: true` 的步骤 0 或 1 次；与 `times` 同步声明 ⇒ 跑之前就报错（optional 就是 0 或 1 次）。
2. 跳过路径：下一步是 optional 且事件不吃进来 ⇒ 开「跳过它」的分支（该步 0 行），同一事件继续往下试；原匹配也保留（后续该步条件命中仍能走「吃到」路径）。
3. 吃到路径：optional 步骤吃到事件也开一条「不吃（0 次）」的分支 —— 两条路径都合法（宁多配不少配）。
4. 跳过的步骤产出里只有 `步骤名#`（= 0），没有 `步骤名.列`。
5. `until` 命中的事件不吃进该步（部分匹配留着等推进，不销毁 —— 比 Flink 的「until 销毁匹配」更保守）；计数 `until-stopped`。
6. 流尾封口：声明了 optional 步骤的管道在流结束时（flush）把「剩余步骤全是 optional」的部分匹配补产出，计数 `flush-emitted`；没有 optional 步骤的管道 flush 不动手。
7. 不变式：部分匹配的 steps 列表长度恒等于 stepIndex + 1（跳过与封口都要补空位）。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core \
  -Dtest='CepOptionalStepTest,CepPatternOperatorTest'
# 预期：Tests run: 12, Failures: 0, Errors: 0
```

## 用例

| 用例 | 喂的事件 | 断言 |
| --- | --- | --- |
| optionalSkipped | create → ship | 1 条匹配；`pay#` = 0 且无 `pay.type`；`optional-skipped` = 1 |
| optionalBothPaths | create → pay → ship | 2 条匹配；吃到路径（`pay#` = 1）先出、跳过路径（`pay#` = 0）后出 |
| untilStopsConsumption | create → PAY(cancelled=false) → PAY(cancelled=true) | 1 条匹配（`pay#` = 1，amount = 10）；`until-stopped` = 1 |
| flushSealsOptionalTail | 只有 create | 流尾补产出 1 条（`pay#` = 0）；`matched` = 0、`flush-emitted` = 1 |
| optionalWithTimesRejected | —— | IllegalArgumentException 点名 optional |

## 用法示例

```yaml
- id: cep
  type: cep.pattern
  config:
    keyBy: [order_id]
    steps:
      - {name: create, expression: "type == 'CREATE'"}
      - {name: pay, expression: "type == 'PAY'", optional: true}          # 没付也能发
      - {name: ship, expression: "type == 'SHIP'"}
      - {name: pay2, expression: "type == 'PAY'", times: "1,", until: "cancelled"}  # cancelled 的 PAY 不续吃
```

## 注意

- optional 的「两条路径都活」是有意为之的保守口径：同一事件吃进与跳过都是 NFA 合法转移，多配不少配；不想多配就在下游接 dedup。
- 跳过分支递归喂事件：深度受 steps 数量约束（stepIndex 严格递增），不会失控；部分匹配上限 `maxPartials`（缺省 64）照样兜底。
