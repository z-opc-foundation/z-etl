# 033 验收复跑 —— 多路合并 union

## 口径

1. `union` 只做透传：多条入边接成一路，不改行、不去重、不排序。
2. 不写 union 节点、多条边直插同一节点也**天然合并**（执行器逐边派发）—— README 里「不写也行」的口径落到测试。
3. 到达序：源轮询按声明序轮流拉 ⇒ 合并后的到达序按轮次交错 —— 断言按集合 / 总数，不钉具体顺序。
4. 算子清单一致性审计（本档顺带）：`OperatorType` 37 个类型 ↔ README 算子表逐一对齐，无缺口。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core \
  -Dtest='UnionOperatorTest'
# 预期：Tests run: 2, Failures: 0, Errors: 0
```

## 用例

| 用例 | 图 | 断言 |
| --- | --- | --- |
| explicitUnionMerges | 3 源 → union → collect | 4 行全到、无重复；行原样（src 列保留） |
| naturalMultiEdgeMerge | 2 源直插 collect | 3 行全到 |

全量：`mvn -o test` → 211 个用例（core 205 + web 6）。

## 注意

- union 不是去重 —— 要去重在后面接 dedup（键口径）。
- 天然合并与显式 union 语义等价；显式节点的好处是图上有个名字，方便 plan 解释与指标定位。
