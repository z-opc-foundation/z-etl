# 028 验收复跑 —— web 层 savepoint 往返与 dry-run

## 口径

1. savepoint 导出是**纯读**（不动状态与水位）；没有任何状态与水位可导 ⇒ 报错点名「先跑一次再导出」。
2. 换名导入是**迁移口径**：只把状态快照与水位游标灌到 `targetName` 名下 —— 目标管道必须先登记同构定义（B 环境先登记、再灌状态、直接增量续跑），导入不会替你造定义。
3. 副本灌回水位后触发 `run`：源头没有新行 ⇒ 增量续跑 0 行（水位已到头）；原名不受影响。
4. dry-run：按 `sampleRows` 采样预演，采样 3 行就写 3 行。
5. 全程本地 H2（源表 + 目标表各自 `jdbc:h2:mem:`），不连生产库。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-web \
  -Dtest='EtlPipelineSavepointTest'
# 预期：Tests run: 2, Failures: 0, Errors: 0
```

## 用例

| 用例 | 链路 | 断言 |
| --- | --- | --- |
| savepointRoundtripWithRename | 登记×2 → 跑原名 → 导出 → 换名灌回 → 跑副本 | 副本增量续跑 0 行；两个名字都在、原名不受影响 |
| dryRunSamplesRows | dryRun(sampleRows=3) | recordsWritten = 3 |

全量：`mvn -o test` → 200 个用例（core 195 + web 5）。

## 注意

- 换名导入前目标管道必须已登记，否则状态灌进了一个不存在的名字（跑的时候报「流水线不存在」）。
- 导出前必须至少跑过一次（有状态或水位），空管道导出会被拒绝。
