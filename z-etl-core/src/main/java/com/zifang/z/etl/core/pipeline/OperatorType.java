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

    /** 过滤（表达式为真才放行；未命中可走旁路输出）。 */
    FILTER("filter", Kind.TRANSFORM),

    /** 行级映射（表达式产出新列）。 */
    MAP("map", Kind.TRANSFORM),

    /** 自由脚本（z-script 沙箱；可产出一列或整行）。 */
    SCRIPT("script", Kind.TRANSFORM),

    /** 字段映射（源列 → 目标列 + 落库类型）。 */
    FIELD_MAPPING("field.mapping", Kind.TRANSFORM),

    /** 类型归一（类型 + 定点小数位 + 空值策略）。 */
    TYPE_NORMALIZE("type.normalize", Kind.TRANSFORM),

    /** 多路合并（把多条入边接成一路；也可以不写该节点，靠多入边天然合并）。 */
    UNION("union", Kind.TRANSFORM),

    /** 键控聚合（无窗口，滚动累计值，键变化即发一次）。 */
    AGGREGATE("aggregate", Kind.STATEFUL),

    /** 窗口聚合（事件时间窗口 + 水位触发 + 允许迟到）。 */
    WINDOW_AGGREGATE("window.aggregate", Kind.STATEFUL),

    /** 去重（按键保留首条 / 末条，带存活时间）。 */
    DEDUP("dedup", Kind.STATEFUL),

    /** 连接（左右两路按键关联）。 */
    JOIN("join", Kind.STATEFUL),

    /** JDBC 落地（批量 upsert / insert）。 */
    JDBC_SINK("jdbc.sink", Kind.SINK),

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
        if ("groovy.script".equals(normalized) || "script.groovy".equals(normalized)) {
            return SCRIPT;
        }
        if ("window.agg".equals(normalized) || "windowAggregate".equals(normalized)) {
            return WINDOW_AGGREGATE;
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
