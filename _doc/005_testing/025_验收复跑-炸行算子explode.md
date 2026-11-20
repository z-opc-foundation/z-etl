# 025 验收复跑 —— 炸行算子 explode

## 口径

1. `explode` 一行拆多行（Flink SQL 的 UNNEST 口径）：数组列（`List`）逐元素出一行；声明 `delimiter` 后字符串按分隔符切分。
2. 标量值（既不是数组也没声明 delimiter）按单元素原样出一行；null 按空处理。
3. 空数组 / null / 空串：缺省丢并计数（`dropped-empty`）；`keepEmpty: true` 保留原行、目标列置 null（`kept-empty` 计数）。
4. `target` 缺省覆盖原列；声明了就把元素写到新列（原列不动）；`indexColumn` 顺带写元素下标（0 起）。
5. 缺 `config.column` ⇒ 跑之前就报错。
6. 投影下推：explode 读的 `column` 会并进上游列需求（重写器 operatorColumns 有 EXPLODE 分支），不会被上游投影剪掉。
7. 类型别名：`unnest` / `flat.map` ≡ `explode`。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core -Dtest='ExplodeOperatorTest'
# 预期：Tests run: 4, Failures: 0, Errors: 0
```

## 用例

| 用例 | 输入 | 断言 |
| --- | --- | --- |
| arrayExplode | tags [a,b] / [c]，indexColumn tag_idx | 3 行；下标 0/1/0；`exploded` = 1 |
| delimiterSplit | "a,b,c" / "x"，delimiter ","，target item | 4 行；item 列新写、原 csv 列不动 |
| emptyHandling | tags [] / [z] | 缺省 1 行（dropped-empty = 1）；keepEmpty 2 行（目标列 null、kept-empty = 1） |
| scalarAndValidation | n = 5 / 缺 column | 标量 1 行原样过；缺 column 报 IllegalArgumentException |

全量：`mvn -o test` → 191 个用例（core 188 + web 3）。

## 用法示例

```yaml
- id: split
  type: explode
  config:
    column: tags           # 数组列
    target: tag            # 元素写到新列
    indexColumn: tag_idx   # 顺带写下标
- id: splitCsv
  type: explode
  config:
    column: csv
    delimiter: ","         # 字符串切分
```

## 注意

- 行数会膨胀（N 个元素出 N 行），接下游聚合 / 落库前先想清楚基数。
- keepEmpty 保留的行目标列为 null —— 下游有空值口径（type.normalize 等）接着处理。
