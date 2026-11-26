package com.zifang.z.etl.core.pipeline;

/**
 * 流水线算子类型（命名空间式类型串：{@code 家族.算子}）。
 *
 * <p>类型串照抄 Kestra / n8n 的「命名空间类型」写法（{@code jdbc.source} / {@code window.aggregate}），
 * 好处是加新算子不污染平铺命名空间，也让人一眼看出算子属于哪一族。
 *
 * @author zifang
 */
public enum OperatorType {

    /** JDBC 源（分页游标 + 事件时间 + 断点续传）。 */
    JDBC_SOURCE("jdbc.source", Kind.SOURCE),

    /** CSV 文件源。 */
    FILE_CSV_SOURCE("file.csv.source", Kind.SOURCE),

    /** JSON 文件源（NDJSON / JSON 数组）。 */
    FILE_JSON_SOURCE("file.json.source", Kind.SOURCE),

    /** Excel 文件源（xlsx）。 */
    FILE_XLSX_SOURCE("file.xlsx.source", Kind.SOURCE),

    /** XML 文件源（StAX 流式解析，重复元素即记录）。 */
    FILE_XML_SOURCE("file.xml.source", Kind.SOURCE),

    /** HTTP 接口源（REST / JSON API）。 */
    HTTP_SOURCE("http.source", Kind.SOURCE),

    /** 资料磨坊源（z-mill 全格式文档 / 网页 → 分块行或整件成品行）。 */
    MILL_SOURCE("mill.source", Kind.SOURCE),

    /** 内联常量源（声明里写死的行）。 */
    INLINE_SOURCE("inline.source", Kind.SOURCE),

    /** 脚本源（z-script 沙箱生成数据）。 */
    SCRIPT_SOURCE("script.source", Kind.SOURCE),

    /** 消息源（Kafka / 库表队列，经 MessageBus SPI）。 */
    MESSAGE_SOURCE("message.source", Kind.SOURCE),

    /** 子管道引用源（把已登记的管道当视图复用：汇换收集、不碰水位状态、参数透传）。 */
    PIPELINE_REF("pipeline.ref", Kind.SOURCE),

    /** 过滤（表达式为真才放行；未命中可走旁路输出）。 */
    FILTER("filter", Kind.TRANSFORM),

    /** 路由（n 路条件分流：rules 顺序判定命中即走旁路标签，fallback 兜底；节点无主输出）。 */
    ROUTE("route", Kind.TRANSFORM),

    /** 行级映射（表达式产出新列）。 */
    MAP("map", Kind.TRANSFORM),

    /** 行号（每行编一个序号列；可按键各自编号；SQL ROW_NUMBER 无排序版的流式口径）。 */
    INDEX("index", Kind.TRANSFORM),

    /** 自由脚本（z-script 沙箱；可产出一列或整行）。 */
    SCRIPT("script", Kind.TRANSFORM),

    /** 平台脚本（调 z-script 脚本中心已上线脚本，app + AK，带版本与审计）。 */
    SCRIPT_REMOTE("script.remote", Kind.TRANSFORM),

    /** 异步映射（每行异步外呼 HTTP 补列；容量背压 + 有序/乱序产出；Flink AsyncFunction 口径）。 */
    ASYNC_MAP("async.map", Kind.TRANSFORM),

    /** 字段映射（源列 → 目标列 + 落库类型）。 */
    FIELD_MAPPING("field.mapping", Kind.TRANSFORM),

    /** 类型归一（类型 + 定点小数位 + 空值策略）。 */
    TYPE_NORMALIZE("type.normalize", Kind.TRANSFORM),

    /** 数据质量断言（行级判定 + 流结束结算；违规可失败 / 走旁路 / 只计数）。 */
    ASSERT("assert", Kind.TRANSFORM),

    /** 多路合并（把多条入边接成一路；也可以不写该节点，靠多入边天然合并）。 */
    UNION("union", Kind.TRANSFORM),

    /** 炸行（一行拆多行：数组列炸开 / 字符串按分隔符切分；Flink SQL 的 UNNEST 口径）。 */
    EXPLODE("explode", Kind.TRANSFORM),

    /** 截断（按键 / 全局 Top-N；批口径：缓存到流尾排序产出；Flink Rank 的保守子集）。 */
    TOPN("topn", Kind.TRANSFORM),

    /** 截取（前 N 条放行，边流边截不整流；SQL LIMIT 口径，可配 offset）。 */
    LIMIT("limit", Kind.TRANSFORM),

    /** 采样（按比例放行：随机可复现 / 按键哈希同键同判定）。 */
    SAMPLE("sample", Kind.TRANSFORM),

    /** 全行去重（整行 / 列子集判重；内存指纹 set + 上限兜底，超出放行不漏放）。 */
    DISTINCT("distinct", Kind.TRANSFORM),

    /** 列转行（melt：声明列逐列竖排成 名字列+值列，其余列原样保留；explode 的横向互补）。 */
    UNPIVOT("unpivot", Kind.TRANSFORM),

    /** 行转列（pivot：keyBy 分组 + 透视列值变列名 + 聚合；unpivot 的对偶，物化口径）。 */
    PIVOT("pivot", Kind.TRANSFORM),

    /** 正则提取（捕获组落列；不匹配可给 null 或丢行）。 */
    REGEX_EXTRACT("regex.extract", Kind.TRANSFORM),

    /** 分列（按分隔符切进声明的输出列名）。 */
    SPLIT("split", Kind.TRANSFORM),

    /** 模板合成（${列名} 占位符拼串落声明列）。 */
    TEMPLATE("template", Kind.TRANSFORM),

    /** 列改名（mapping 旧列名 → 新列名）。 */
    RENAME("rename", Kind.TRANSFORM),

    /** 拍平（嵌套 Map 拍成点路径列）。 */
    FLATTEN("flatten", Kind.TRANSFORM),

    /** JSON 提取（列内 JSON 文本按点路径取字段落列）。 */
    JSON_EXTRACT("json.extract", Kind.TRANSFORM),

    /** 哈希（MD5 / SHA 摘要，指纹化 / 匿名化）。 */
    HASH("hash", Kind.TRANSFORM),

    /** 脱敏（phone / email / half / full 四种打码规则）。 */
    MASK("mask", Kind.TRANSFORM),

    /** 时间格式化（epoch / ISO / 自定义格式 → 目标格式）。 */
    DATE_FORMAT("date.format", Kind.TRANSFORM),

    /** 排序（物化整流；只适合有界数据）。 */
    SORT("sort", Kind.TRANSFORM),

    /** 列画像（行原样透传，流尾按列出 count/nulls/distinct/min/max/mean 画像行走旁路）。 */
    PROFILE("profile", Kind.TRANSFORM),

    /** 键控聚合（无窗口，滚动累计值，键变化即发一次）。 */
    AGGREGATE("aggregate", Kind.STATEFUL),

    /** 窗口聚合（事件时间窗口 + 水位触发 + 允许迟到）。 */
    WINDOW_AGGREGATE("window.aggregate", Kind.STATEFUL),

    /** 去重（按键保留首条 / 末条，带存活时间）。 */
    DEDUP("dedup", Kind.STATEFUL),

    /** 连接（左右两路按键关联）。 */
    JOIN("join", Kind.STATEFUL),

    /** 差异（两流按键比对出变化流 INSERT/UPDATE/DELETE；快照 diff 口径，物化）。 */
    DIFF("diff", Kind.STATEFUL),

    /** 区间连接（左右两路按关联键 + 事件时间区间关联；Flink 的 IntervalJoin 口径）。 */
    INTERVAL_JOIN("interval.join", Kind.STATEFUL),

    /** 维表关联（右流即维表：同键替换热更新、TTL 过期、select 投影；Flink broadcast-state 口径）。 */
    DIM_JOIN("dim.join", Kind.STATEFUL),

    /** 模式匹配（单流步骤序列 + 次数量词 + within 时间窗；Flink CEP 口径的保守子集）。 */
    CEP_PATTERN("cep.pattern", Kind.STATEFUL),

    /** JDBC 落地（批量 upsert / insert）。 */
    JDBC_SINK("jdbc.sink", Kind.SINK),

    /** CSV 文件落地。 */
    FILE_CSV_SINK("file.csv.sink", Kind.SINK),

    /** JSON 文件落地（NDJSON / JSON 数组）。 */
    FILE_JSON_SINK("file.json.sink", Kind.SINK),

    /** Excel 文件落地（xlsx，零依赖写 zip + worksheet）。 */
    FILE_XLSX_SINK("file.xlsx.sink", Kind.SINK),

    /** XML 文件落地（XMLStreamWriter 流式写）。 */
    FILE_XML_SINK("file.xml.sink", Kind.SINK),

    /** HTTP 接口落地（webhook / 业务 API）。 */
    HTTP_SINK("http.sink", Kind.SINK),

    /** 消息落地（Kafka / 库表队列，经 MessageBus SPI）。 */
    MESSAGE_SINK("message.sink", Kind.SINK),

    /** 控制台落地（调试用；只打日志不落库）。 */
    CONSOLE_SINK("console.sink", Kind.SINK),

    /** 内存落地（测试 / 预览用；把行收进列表）。 */
    COLLECT_SINK("collect.sink", Kind.SINK);

    /** 算子族。 */
    public enum Kind {
        /** 源（无入边）。 */
        SOURCE,
        /** 无状态变换（可任意并行）。 */
        TRANSFORM,
        /** 有状态算子（键控状态 / 定时器 / 窗口）。 */
        STATEFUL,
        /** 汇（无出边）。 */
        SINK
    }

    private final String id;
    private final Kind kind;

    OperatorType(String id, Kind kind) {
        this.id = id;
        this.kind = kind;
    }

    /** 类型串。 */
    public String id() {
        return id;
    }

    /** 算子族。 */
    public Kind kind() {
        return kind;
    }

    /** 是否是源。 */
    public boolean isSource() {
        return kind == Kind.SOURCE;
    }

    /** 是否是汇。 */
    public boolean isSink() {
        return kind == Kind.SINK;
    }

    /** 是否需要键控状态（算子自己会开状态后端）。 */
    public boolean isStateful() {
        return kind == Kind.STATEFUL;
    }

    /**
     * 类型串 → 算子类型。
     *
     * <p>接受别名：{@code source}→{@code jdbc.source}、{@code sink}→{@code jdbc.sink}、
     * {@code expr.map}→{@code map}、{@code groovy.script}→{@code script}、{@code window.agg}→{@code window.aggregate}。
     * 下划线与中划线视作点（{@code jdbc_source} ≡ {@code jdbc.source}）。
     *
     * @param id 类型串
     * @return 算子类型；不认识返回 {@code null}
     */
    public static OperatorType fromId(String id) {
        if (id == null) {
            return null;
        }
        String normalized = id.trim().toLowerCase().replace('_', '.').replace('-', '.');
        if (normalized.isEmpty()) {
            return null;
        }
        if ("source".equals(normalized) || "jdbc".equals(normalized)) {
            return JDBC_SOURCE;
        }
        if ("sink".equals(normalized) || "jdbc.sink.upsert".equals(normalized)) {
            return JDBC_SINK;
        }
        if ("expr.map".equals(normalized) || "expression.map".equals(normalized)) {
            return MAP;
        }
        if ("unnest".equals(normalized) || "flat.map".equals(normalized)) {
            return EXPLODE;
        }
        if ("rank".equals(normalized) || "top.n".equals(normalized)) {
            return TOPN;
        }
        if ("sampling".equals(normalized) || "mod.monitor".equals(normalized)) {
            return SAMPLE;
        }
        if ("unique".equals(normalized) || "dedup.row".equals(normalized)) {
            return DISTINCT;
        }
        if ("melt".equals(normalized) || "columns.to.rows".equals(normalized)) {
            return UNPIVOT;
        }
        if ("groovy.script".equals(normalized) || "script.groovy".equals(normalized)) {
            return SCRIPT;
        }
        if ("window.agg".equals(normalized) || "windowAggregate".equals(normalized)) {
            return WINDOW_AGGREGATE;
        }
        if ("interval.join".equals(normalized) || "time.join".equals(normalized)
                || "intervalJoin".equals(normalized)) {
            return INTERVAL_JOIN;
        }
        if ("dim.join".equals(normalized) || "dim.lookup".equals(normalized)
                || "lookup.join".equals(normalized) || "broadcast.join".equals(normalized)) {
            return DIM_JOIN;
        }
        if ("aggregate.window".equals(normalized)) {
            return WINDOW_AGGREGATE;
        }
        if ("fields".equals(normalized) || "fields.mapping".equals(normalized)) {
            return FIELD_MAPPING;
        }
        if ("types".equals(normalized) || "types.normalize".equals(normalized)) {
            return TYPE_NORMALIZE;
        }
        if ("assert".equals(normalized) || "quality.assert".equals(normalized)
                || "quality.gate".equals(normalized) || "check".equals(normalized)) {
            return ASSERT;
        }
        if ("switch".equals(normalized) || "branch".equals(normalized) || "router".equals(normalized)) {
            return ROUTE;
        }
        if ("spread".equals(normalized) || "rows.to.columns".equals(normalized)
                || "cross.tab".equals(normalized) || "crosstab".equals(normalized)) {
            return PIVOT;
        }
        if ("stats".equals(normalized) || "describe".equals(normalized)
                || "data.profile".equals(normalized) || "column.stats".equals(normalized)) {
            return PROFILE;
        }
        if ("row.number".equals(normalized) || "rownum".equals(normalized)
                || "row.numbering".equals(normalized)) {
            return INDEX;
        }
        if ("head".equals(normalized) || "take".equals(normalized) || "first.n".equals(normalized)) {
            return LIMIT;
        }
        if ("delta".equals(normalized) || "changes".equals(normalized)
                || "changelog".equals(normalized) || "snapshot.diff".equals(normalized)
                || "change.capture".equals(normalized)) {
            return DIFF;
        }
        // 输入类型别名
        if ("csv.source".equals(normalized) || "csv".equals(normalized) || "file.csv".equals(normalized)) {
            return FILE_CSV_SOURCE;
        }
        if ("json.source".equals(normalized) || "ndjson.source".equals(normalized)
                || "file.json".equals(normalized)) {
            return FILE_JSON_SOURCE;
        }
        if ("xlsx.source".equals(normalized) || "excel.source".equals(normalized)
                || "file.xlsx".equals(normalized)) {
            return FILE_XLSX_SOURCE;
        }
        if ("xml.source".equals(normalized) || "file.xml".equals(normalized)) {
            return FILE_XML_SOURCE;
        }
        if ("http.source".equals(normalized) || "rest.source".equals(normalized)
                || "http.api.source".equals(normalized)) {
            return HTTP_SOURCE;
        }
        if ("inline.rows".equals(normalized) || "static.source".equals(normalized)
                || "const.source".equals(normalized)) {
            return INLINE_SOURCE;
        }
        if ("groovy.source".equals(normalized) || "script.groovy.source".equals(normalized)) {
            return SCRIPT_SOURCE;
        }
        if ("mq.source".equals(normalized) || "queue.source".equals(normalized)
                || "kafka.source".equals(normalized)) {
            return MESSAGE_SOURCE;
        }
        if ("pipeline".equals(normalized) || "sub.pipeline".equals(normalized)
                || "pipeline.include".equals(normalized)) {
            return PIPELINE_REF;
        }
        if ("cep".equals(normalized) || "pattern".equals(normalized)
                || "pattern.match".equals(normalized) || "pattern.detect".equals(normalized)) {
            return CEP_PATTERN;
        }
        if ("async".equals(normalized) || "async.http".equals(normalized)
                || "http.async".equals(normalized) || "http.lookup".equals(normalized)) {
            return ASYNC_MAP;
        }
        // 输出类型别名
        if ("csv.sink".equals(normalized) || "file.csv.out".equals(normalized)) {
            return FILE_CSV_SINK;
        }
        if ("json.sink".equals(normalized) || "ndjson.sink".equals(normalized)
                || "file.json.out".equals(normalized)) {
            return FILE_JSON_SINK;
        }
        if ("xlsx.sink".equals(normalized) || "excel.sink".equals(normalized)
                || "file.xlsx.out".equals(normalized)) {
            return FILE_XLSX_SINK;
        }
        if ("xml.sink".equals(normalized) || "file.xml.out".equals(normalized)) {
            return FILE_XML_SINK;
        }
        if ("http.api.sink".equals(normalized) || "webhook.sink".equals(normalized)
                || "rest.sink".equals(normalized)) {
            return HTTP_SINK;
        }
        if ("mq.sink".equals(normalized) || "queue.sink".equals(normalized)
                || "kafka.sink".equals(normalized)) {
            return MESSAGE_SINK;
        }
        for (OperatorType type : values()) {
            if (type.id.equals(normalized)) {
                return type;
            }
        }
        return null;
    }

    /** 全部类型串（报错时列给用户看）。 */
    public static String allIds() {
        StringBuilder sb = new StringBuilder();
        for (OperatorType type : values()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(type.id);
        }
        return sb.toString();
    }
}
