# 023 验收复跑 —— CEP 紧邻度 strict

## 口径

1. `strict: true` 声明在「下一步」上（Flink `.next()` 口径）：该步的事件必须紧跟当前步最后一条被吃进的事件。
2. record-at-a-time 落地：部分匹配在当前步，事件既不吃进当前步（次数未封顶且条件命中）也不推进到 strict 步 ⇒ 部分匹配死掉，计数 `strict-broken`。
3. 缺省宽松：不声明 strict，中间插事件照样等（宁多配不少配的既有口径不变）。
4. 当前步续吃（`times` 未封顶的同条件事件）不算插事件 —— strict 链条不断。
5. strict 与 optional 同点声明不冲突：optional 的跳过分支照开（那是「0 次路径」，不吃进也不算插事件）。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core \
  -Dtest='CepStrictContiguityTest,CepOptionalStepTest,CepPatternOperatorTest'
# 预期：Tests run: 16, Failures: 0, Errors: 0
```

## 用例

| 用例 | 喂的事件 | 断言 |
| --- | --- | --- |
| strictFollowMatched | create → pay | 1 条匹配（strict 步直接跟着来） |
| strictFollowBroken | create → OTHER → pay | 0 条匹配；`strict-broken` ≥ 1 |
| relaxedByDefault | create → OTHER → pay（无 strict） | 1 条匹配（缺省宽松） |
| loopGrowthKeepsStrict | create → pay → pay → ship(strict) | 1 条匹配（`pay#` = 2，续吃不算插事件） |

全量：`mvn -o test` → 181 个用例（core 178 + web 3）。

## 用法示例

```yaml
- id: cep
  type: cep.pattern
  config:
    keyBy: [order_id]
    steps:
      - {name: create, expression: "type == 'CREATE'"}
      - {name: pay, expression: "type == 'PAY'", strict: true}   # 中间插任何别的事件即死
```

## 注意

- strict 只约束「当前步 → 下一步」这一跳；再往后的 strict 在匹配推进到那一跳时才生效。
- strict 死掉的是部分匹配，不是整个键 —— 后续再来 create 还能开新匹配。
