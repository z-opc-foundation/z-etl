# 038 验收复跑 — 集成插件包：z-mill 打通 + z-script 平台打通 + 十个开箱即用变换

- 日期：2026-11-25
- 范围：z-etl-core
- 关联：README「算子表」`mill.source` / `script.remote` / `regex.extract` / `split` / `template` / `rename` / `flatten` / `json.extract` / `hash` / `mask` / `date.format` / `sort` 行

## 口径

### 打通一：`mill.source`（源算子，z-mill 资料磨坊）

- 走 z-mill-web 的 HTTP 面，两种取料方式（必填其一）：
  - `file`：本地文件读字节，multipart `POST {baseUrl}/api/grind`（part 名 `file` + `views`）；
  - `url`：`POST {baseUrl}/api/grind-url`，body `{"url": ...}`（只支持 http/https）。
- 产出模式：
  - `mode: chunk`（默认）：按 z-mill 标题路径分块，一块一行 `source/index/heading/path/text`（path 用 `' / '` 拼接）——RAG 粒度直接进管道；
  - `mode: doc`：一文档一行 `source/format/blocks/chars/markdown/json/text/html`。
- 响应 `results[0].status != ok` 抛错不静默吞；响应缺 `results` 抛错。
- 信任边界：`baseUrl`/`file`/`url` 都来自流水线声明，只让可信的登记人写声明。

### 打通二：`script.remote`（变换算子，z-script 脚本平台）

- 契约：`POST {baseUrl}/api/script/run?scriptCode=xxx`，body = 整行 JSON 当执行参数，鉴权头 `X-Api-Key`（app + AK 口径）；响应 `{success, data, errorMessage, durationMs}`。
- `success=false` 或 HTTP 非 2xx 抛错不静默吞（带平台 errorMessage）。
- 产出与本地 `script` 算子同形：`mode: column`（默认，data 落 `name` 列，可 `type`/`nullable` 归一）｜`mode: row`（data 是 Map 整行替换）。
- 与本地 `script` 分工：本地沙箱管管道里现写的清洗逻辑；本算子管脚本中心里已上线、带版本与审计的脚本——改脚本不用改管道声明。

### 插件包：十个开箱即用变换（全部无状态，除 `sort`）

| 算子 | 要点 |
|---|---|
| `regex.extract` | 捕获组（组名或序号）→ 输出列；不匹配缺省 null 列 + `no-match` 计数，`onNoMatch: drop` 丢行 |
| `split` | 分隔符切开落 `names` 列；段不足补 null；缺省替换原列 |
| `template` | `${列名}` 占位合成；缺列/null 给空串 |
| `rename` | mapping 改名保序，其余列不动 |
| `flatten` | 嵌套 Map 拍点路径列，数组不动 |
| `json.extract` | 列内 JSON 点路径取字段；`onError` 缺省 fail，`'null'`（带引号）给 null + `parse-error` 计数 |
| `hash` | SHA-256/SHA-1/MD5 白名单；salt 防彩虹表；suffix 缺省原地覆盖 |
| `mask` | phone/email/half/full 四规则；phone 非十一位 half 兜底；null 不动 |
| `date.format` | epoch 毫秒 / ISO / fromPattern 三口径；Timestamp/Date 值直接吃；解析失败炸行 |
| `sort` | 多列 + `-` 降序；码点序；物化整流（只适合有界数据） |

### 平台适配注记（实现期踩到）

- `PipelineNode.string` 把纯空白当缺省 ⇒ `split` 的 `separator` 走 `getConfig()` 取原值（单空格可用）。
- `inline.source` 列值推断：数字串成 Long、时间串成 Timestamp ⇒ `date.format` 直接收 Date/Timestamp。
- yml 裸 `null` 被吃成缺值 ⇒ `json.extract` 的 `onError: 'null'` 要带引号。
- `sort` 曾是 `topn` 的别名（零消费者）⇒ 收回给本算子，topn 保留 `rank`/`top.n`。
- `flatten` 顶层前缀 bug（首版丢 `entry.getKey()` 前缀）已在实现期自测抓出修复。

## 复跑

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -o test -pl z-etl-core \
  -Dtest='MillSourceTest,ScriptRemoteOperatorTest,PluginPackTextTest,PluginPackDataTest'
# Tests run: 26, Failures: 0, Errors: 0
mvn -o test   # 全量 245（core 239 + web 6）全绿
```

桩：`MillSourceTest` / `ScriptRemoteOperatorTest` 内嵌 `com.sun.net.httpserver` 按 z-mill-web / z-script 平台真实契约回包（multipart 上传体也验证到了字节）。

## 链路实测（2026-11-25 真服务全链）

本地栈：z-mill-web :8891（早先已起）+ z-script-admin :8086（本笔拉起，`Z_BASE_DB_SCRIPT_DATABASE=oc`，脚本表与 etl_* 同库）+ z-etl-web :8092（新 core 重启）。

引导链：`POST /script/api/script/api-key`（唯一免鉴权引导端点）建 app+AK → `POST /script/api/script` 建脚本 `etl-doc-summaries`（GROOVY，返回 Map 供 mode=row）→ ad-hoc `run` 验证平台执行绿。

管道一（网页分支）：`mill.source(url=http://127.0.0.1:8888/, mode=doc)` → `script.remote(mode=row)` → `file.json.sink` → 读 1 写 1，clean 节点 92ms 平台往返，`/tmp/zetl-chain-out/result.json` 落地。

管道二（文件分支 + 插件入链）：`mill.source(file=产品手册.html, mode=chunk)` multipart 上传 → `script.remote(mode=row)` → `mask(preview: half)` → `file.json.sink` → 读 4 写 4，四块带标题路径（`产品手册 / 第一章 安装` 等），脱敏在链上生效。运行履历 `/runs`、计划解释 `/plan` 均正常。

注意：外网 URL（example.com）抓取有超时抖动（z-mill 侧 15s 闸），链路验证用本地页面保证确定性。

## 边界（D5-BOUNDARY）

本笔不动：`_verify`、`_chain`、其他仓；只改 z-etl 本仓（core 算子 + README + 本 doc）。
