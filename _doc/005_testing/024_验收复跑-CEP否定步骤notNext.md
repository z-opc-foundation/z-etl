# 024 验收复跑 —— CEP 否定步骤 notNext

## 口径

1. `notNext: true` 声明在步骤上（Flink `notNext()` 口径）：该步不消费事件，产出里只有 `步骤名#`（= 0）。
2. 部分匹配还没越过否定步时，违禁事件一到匹配即死（`not-broken` 计数）；否定优先于一切 —— 哪怕事件也能续吃当前步，只要命中否定条件就死。
3. 不能是第一步（否定要挂在某个正步后面）；不能与 times / optional 同点声明 —— 跑之前就报错。
4. 拖在模式末尾的 notNext：前面的正步吃满也不立刻产出，要等流尾 flush 确认「期间没等到违禁事件」才产出（`flush-emitted`）；中途违禁 ⇒ 死掉且流尾不产出。
5. 不变式：部分匹配的 steps 列表长度恒等于 stepIndex + 1（被越过的否定步补 0 行空位）。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core \
  -Dtest='CepNotNextStepTest,CepStrictContiguityTest,CepOptionalStepTest,CepPatternOperatorTest'
# 预期：Tests run: 22, Failures: 0, Errors: 0
```

## 用例

| 用例 | 喂的事件 | 断言 |
| --- | --- | --- |
| notNextHolds | create → ship | 1 条匹配；`cancel#` = 0 且无 `cancel.type` |
| notNextViolated | create → CANCEL → ship | 0 条匹配；`not-broken` ≥ 1 |
| trailingNotNextFlush | create → pay（拖尾 not CANCEL） | 流尾封口产出 1 条（`cancel#` = 0）；`matched` = 0、`flush-emitted` = 1 |
| trailingNotNextViolated | create → pay → CANCEL | 0 条匹配；`not-broken` ≥ 1、`flush-emitted` = 0 |
| notNextFirstRejected | —— | IllegalArgumentException 点名 notNext |
| notNextWithTimesRejected | —— | IllegalArgumentException 点名 notNext |

全量：`mvn -o test` → 187 个用例（core 184 + web 3）。

## 用法示例

```yaml
- id: cep
  type: cep.pattern
  config:
    keyBy: [order_id]
    steps:
      - {name: create, expression: "type == 'CREATE'"}
      - {name: cancel, expression: "type == 'CANCEL'", notNext: true}   # 中间不许来 CANCEL
      - {name: ship, expression: "type == 'SHIP'"}
```

## 注意

- 否定步只守「它前面的正步被吃满之后」的区段；越过之后（后面正步的事件已吃进）不再回头查。
- 拖尾 notNext 的产出时机是流尾（flush），不是正步吃满的那一刻 —— 等不到流尾确认就别想提前拿行。
