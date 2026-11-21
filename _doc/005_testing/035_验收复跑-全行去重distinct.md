# 035 验收复跑 —— 全行去重 distinct

- 日期：2026-11-21
- 模块：z-etl-core
- 用例：`DistinctOperatorTest`（4 个）

## 口径

- 缺省整行所有列拼指纹（列名 + 值 + 分隔符），声明 `columns` 后按列子集判重；
- 首次出现放行，重复的丢掉并计数 `duplicates`；
- 指纹数超 `cap`（缺省 100000，`cap < 1` 构造期拒绝）后新行不再跟踪但**照常放行**
  并计数 `cap-overflow` —— 宁多放不漏放，与 CEP 保守口径同线；
- 内存 set，不进键控状态（与 topn 批缓存同款取舍：检查点不背）；
- 行原样放行或丢掉，只计数不改行。

## 复跑

```bash
cd z-etl
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core -Dtest=DistinctOperatorTest
```

- 2026-11-21：4/4 通过。
- 全套：`mvn -o test` ⇒ 215 个用例（core 209 + web 6）全绿。

## 用例清单

1. 整行判重：完全相同的行只留第一条；a 相同但 b 不同的行不算重复；`duplicates=1`、`records-out=3`。
2. 列子集判重：`columns: [a]`，同 a 只留第一条，其余列差异忽略。
3. 上限兜底：`cap: 2` 三条不同行 ⇒ 全部放行，`cap-overflow=1`。
4. 校验与 null：`cap: 0` 抛 IllegalArgumentException（含 "cap"）；null 值行同样参与判重。

## 关联提交

- 本笔：feat(etl) 全行去重 distinct。
