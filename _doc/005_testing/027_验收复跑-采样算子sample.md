# 027 验收复跑 —— 采样算子 sample

## 口径

1. `sample` 按比例放行（丢数据口径）：`rate` 必填、(0, 1] 内，越界/缺失 ⇒ 跑之前就报错。
2. 随机采样：声明 `seed` ⇒ 同流两次跑产出完全一致；不声明 ⇒ 每次跑都不一样。
3. 按键哈希采样：`hashBy` 声明后按键哈希取模判定 —— 同键永远同判定（同一键的行要么全留要么全丢，键上口径一致），且天然可复现（不依赖 seed）。
4. 行原样放行或丢掉，只计数不改行（`sampled` / `skipped`）；sampled + skipped 恒等于总行数。
5. 类型别名：`sampling` ≡ `sample`。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core -Dtest='SampleOperatorTest'
# 预期：Tests run: 3, Failures: 0, Errors: 0
```

## 用例

| 用例 | 输入 | 断言 |
| --- | --- | --- |
| seededReproducible | 6 行，rate 0.5 + seed 42 | 两次跑 id 序列完全一致；sampled + skipped = 6；1 ≤ sampled ≤ 5 |
| hashByKeyConsistent | 6 行（甲×2、乙×2），hashBy user | 两次跑一致；甲的两行同判定、乙的两行同判定 |
| rateBoundaries | rate 1.0 / 1.5 / 缺 rate | 1.0 全留（sampled=6）；越界与缺失报 IllegalArgumentException |

全量：`mvn -o test` → 198 个用例（core 195 + web 3）。

## 用法示例

```yaml
- id: sample
  type: sample
  config:
    rate: 0.1        # 放行一成
    seed: 42         # 可复现调试
    # hashBy: [user] # 或者：同键同判定的哈希采样
```

## 注意

- 采样是丢数据口径：下游要全量就别接；要「键上口径一致」的降载用 hashBy，别用纯随机。
- rate 越小不是越「准」—— 哈希采样的实际放行率受键分布影响（桶取模 10000）。
