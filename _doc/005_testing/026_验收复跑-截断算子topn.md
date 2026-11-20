# 026 验收复跑 —— 截断算子 topn

## 口径

1. `topn` 按键 / 全局 Top-N（批口径）：整条流缓存到流尾（flush）才排序产出 —— 适合文件 / 有限流，别接无限源。
2. `orderBy` 必填；`order` 只认 asc（缺省）/ desc；`limit` 至少 1；`keyBy` 声明后每键各取前 N 条。
3. 数值列按数值比（Integer/Long/Double 混着来都行），其余按字符串比；null 视为最小（asc 排最前、desc 排最后）。
4. 同值按到达顺序保持稳定（稳定排序）；产出行是原行原样（不加分名列）。
5. 指标：`buffered`（缓存行数）、`emitted`（产出行数）、`cut`（被截掉的行数）。
6. 类型别名：`sort` / `rank` / `top.n` ≡ `topn`。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core -Dtest='TopNOperatorTest'
# 预期：Tests run: 4, Failures: 0, Errors: 0
```

## 用例

| 用例 | 输入 | 断言 |
| --- | --- | --- |
| globalTopN | 4 行 score，desc limit 2 | 2 行有序（95, 88）；buffered=4 / emitted=2 / cut=2 |
| perKeyTopN | 甲 3 行 + 乙 2 行，keyBy user，limit 2 | 4 行（甲 95/88 + 乙 100/60）；cut=1 |
| ascWithNulls | score 5/3/null，asc limit 3 | null 排最前，然后 3、5 |
| validation | 缺 orderBy / 缺 limit | IllegalArgumentException 点名对应配置 |

全量：`mvn -o test` → 195 个用例（core 192 + web 3）。

## 用法示例

```yaml
- id: top
  type: topn
  config:
    orderBy: score
    order: desc      # 从大到小
    limit: 10
    keyBy: [user_id] # 每人各取前 10（不声明就是全局前 10）
```

## 注意

- 批口径把整条流缓在内存里：行数大就先在源头/下推里收窄，再进 topn。
- 缓存不进键控状态（有限流口径下 checkpoint 不背这些缓存）—— 中途杀掉重跑靠源侧断点，不靠算子状态。
