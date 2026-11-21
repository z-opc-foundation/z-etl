# 036 验收复跑 —— 列转行 unpivot

- 日期：2026-11-21
- 模块：z-etl-core
- 用例：`UnpivotOperatorTest`（4 个）

## 口径

- 声明 `columns` 逐列竖排：每条输入按声明列序各出一行，行 = 原行去掉熔炼列
  + 名字列（缺省 `column`，列名）+ 值列（缺省 `value`，原值）；
- 未声明的列原样保留（melt 语义）；
- 值为 null：缺省丢掉并计数 `null-skipped`，`keepNull: true` 也出一行；
- 无状态，与 `explode`（纵向炸开）互补；`columns` 缺省/空构造期拒绝。

## 复跑

```bash
cd z-etl
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core -Dtest=UnpivotOperatorTest
```

- 2026-11-21：4/4 通过。
- 全套：`mvn -o test` ⇒ 219 个用例（core 213 + web 6）全绿。

## 用例清单

1. 基础 melt：{id, jan, feb} ⇒ 两行（column/value），id 原样保留，records-out=2。
2. 自定义 `nameColumn: month` / `valueColumn: amount` + `keepNull: true`：null 值也出一行。
3. 缺省丢 null：两行四列里三个 null ⇒ 只出 1 行，`null-skipped=3`。
4. 校验：`columns` 空抛 IllegalArgumentException（含 "columns"）。

## 关联提交

- 本笔：feat(etl) 列转行 unpivot。
