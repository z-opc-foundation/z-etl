package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 聚合下推：源节点声明了 {@code pushdown.aggregate}（{@code GROUP BY} + 聚合函数）时，
 * 把下游**重复计算**的 {@code aggregate} 节点摘掉。
 *
 * <p>口径：
 * <ul>
 *   <li><b>下推由用户在源节点显式声明</b>（像 {@code where} 一样写死在源上）——
 *       引擎不猜「这个聚合能不能下推」；</li>
 *   <li>只有当 aggregate 节点的 {@code keyBy} / {@code aggregates} 与源声明的**完全一致**
 *       （分组列集合相同、聚合逐条 fn/column/alias 相同）时才摘；</li>
 *   <li>不一致 ⇒ <b>不摘</b>（源已按分组聚合过，引擎再聚合是对聚合结果再聚合，
 *       可能是用户有意为之），只在备注里提示；</li>
 *   <li>摘掉后语义从「流式滚动累计（每条输入发一次）」变成「SQL GROUP BY（每组一行）」
 *       —— 这是**用户显式声明下推**换来的口径变化，备注里写清楚。</li>
 * </ul>
 *
 * <p><b>下游列守卫</b>（错了就是静默丢数据）：引擎侧 aggregate 的输出是「整行 + 聚合别名」，
 * 而下推后的源只输出「分组列 + 聚合别名」⇒ 下游若引用了别的列，就会拿到空值。
 * 因此摘节点前必须确认<b>下游链路用到的列全在下推输出列集里</b>：
 * <ul>
 *   <li>精确列名（{@code field.mapping} 的 source、汇的 columns / idempotencyKey、
 *       {@code keyBy}、聚合列、类型归一列）：不在输出列集 ⇒ 不摘；</li>
 *   <li>表达式里的标识符（map / filter 的表达式）：能拿到源表列清单时按「真实列」判定，
 *       拿不到 ⇒ 不摘（不猜）；</li>
 *   <li>整行脚本 / join：引用不可知 ⇒ 不摘。</li>
 * </ul>
 *
 * @author zifang
 */
public class AggregatePushdownRewriter implements PlanRewriter {

    private TableSchemaResolver schemaResolver;

    @Override
    public String name() {
        return "AggregatePushdown";
    }

    @Override
    public void prepare(TableSchemaResolver resolver) {
        this.schemaResolver = resolver;
    }

    @Override
    public PipelineSpec rewrite(PipelineSpec spec, List<String> notes) {
        for (PipelineNode node : new ArrayList<PipelineNode>(spec.getNodes())) {
            if (node == null || node.getOperatorType() != OperatorType.AGGREGATE) {
                continue;
            }
            List<PipelineEdge> in = spec.edgesTo(node.getId());
            if (in.size() != 1) {
                continue;
            }
            PipelineNode source = spec.node(in.get(0).getFrom());
            if (source == null || source.getOperatorType() != OperatorType.JDBC_SOURCE) {
                continue;
            }
            Map<String, Object> pushdown = source.map("pushdown");
            if (!isAggregatePushdown(pushdown)) {
                continue;
            }
            if (!matches(node, pushdown)) {
                notes.add("[" + name() + "] aggregate 节点 " + node.getId()
                        + " 与源 " + source.getId() + " 声明的聚合下推不一致，保留引擎侧计算");
                continue;
            }
            String blocker = downstreamBlocker(spec, node, source, pushdown);
            if (blocker != null) {
                notes.add("[" + name() + "] aggregate 节点 " + node.getId() + " 不摘：下游引用了"
                        + "下推结果里没有的列 —— " + blocker);
                continue;
            }
            // 摘掉 aggregate 节点：源已经算好了，重接边
            removeNode(spec, node);
            notes.add("[" + name() + "] aggregate 节点 " + node.getId() + " 的聚合已由源 "
                    + source.getId() + " 的 GROUP BY 下推完成，摘掉引擎侧重复计算"
                    + "（结果口径变为每组一行）");
        }
        return spec;
    }

    /**
     * 下游列守卫：返回「不能摘的原因」；{@code null} 表示可以摘。
     *
     * @param spec     流水线声明
     * @param aggregate 引擎侧聚合节点
     * @param source   源节点
     * @param pushdown 源声明的下推
     * @return 阻断原因（可摘时返回 {@code null}）
     */
    private String downstreamBlocker(PipelineSpec spec, PipelineNode aggregate, PipelineNode source,
                                     Map<String, Object> pushdown) {
        Set<String> available = outputColumns(pushdown);
        List<PipelineNode> descendants = descendants(spec, aggregate.getId());
        if (descendants.isEmpty()) {
            return null;
        }
        List<String> universe = sourceColumns(source);
        for (PipelineNode node : descendants) {
            References references = referencesOf(node);
            if (references.opaque) {
                return "下游节点 " + node.getId() + " 引用不可知（整行脚本 / join）";
            }
            for (String column : references.exact) {
                if (!available.contains(column)) {
                    return "下游节点 " + node.getId() + " 引用列 " + column + "，下推结果里没有这列";
                }
            }
            for (String candidate : references.candidates) {
                if (available.contains(candidate)) {
                    continue;
                }
                if (universe == null) {
                    return "下游节点 " + node.getId() + " 的表达式引用了 " + candidate
                            + "，拿不到源表列清单，无法判定它是不是真实列";
                }
                if (universe.contains(candidate)) {
                    return "下游节点 " + node.getId() + " 的表达式引用了真实列 " + candidate
                            + "，下推结果里没有这列";
                }
            }
        }
        return null;
    }

    /** 下推后源节点的输出列集（分组列 + 聚合别名）。 */
    private Set<String> outputColumns(Map<String, Object> pushdown) {
        Set<String> columns = new LinkedHashSet<String>();
        Object groupBy = pushdown.get("groupBy");
        if (groupBy instanceof List) {
            for (Object item : (List<?>) groupBy) {
                String column = ColumnReferences.normalize(String.valueOf(item));
                if (column != null && !column.isEmpty()) {
                    columns.add(column);
                }
            }
        }
        for (Map<String, Object> item : aggregatesOf(pushdown)) {
            String fn = function(item);
            String column = ColumnReferences.normalize(nullToEmpty(item.get("column")));
            if (fn != null) {
                columns.add(alias(fn, column, item));
            }
        }
        return columns;
    }

    /** 源表列清单（拿不到返回 {@code null} = 不知道）。 */
    private List<String> sourceColumns(PipelineNode source) {
        if (schemaResolver != null) {
            List<String> columns = schemaResolver.columnsOf(source.string("dataSourceRef"), source.string("table"));
            if (columns != null && !columns.isEmpty()) {
                List<String> result = new ArrayList<String>();
                for (String column : columns) {
                    result.add(ColumnReferences.normalize(column));
                }
                return result;
            }
        }
        List<String> declared = source.strings("columns");
        if (declared.isEmpty()) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        for (String column : declared) {
            result.add(ColumnReferences.normalize(column));
        }
        return result;
    }

    /** 节点下游（沿边可达，不含自己）。 */
    private List<PipelineNode> descendants(PipelineSpec spec, String nodeId) {
        List<PipelineNode> result = new ArrayList<PipelineNode>();
        Set<String> visited = new LinkedHashSet<String>();
        List<String> pending = new ArrayList<String>();
        pending.add(nodeId);
        visited.add(nodeId);
        while (!pending.isEmpty()) {
            String current = pending.remove(0);
            for (PipelineEdge edge : spec.edgesFrom(current)) {
                if (!visited.add(edge.getTo())) {
                    continue;
                }
                PipelineNode node = spec.node(edge.getTo());
                if (node != null) {
                    result.add(node);
                }
                pending.add(edge.getTo());
            }
        }
        return result;
    }

    /** 节点对上游行的列引用（精确列名 / 表达式候选 / 是否不可知）。 */
    private References referencesOf(PipelineNode node) {
        References references = new References();
        OperatorType type = node.getOperatorType();
        if (type == OperatorType.FIELD_MAPPING) {
            for (Map<String, Object> item : node.mapList("mappings")) {
                String source = ColumnReferences.normalize(text(item.get("source")));
                if (source != null) {
                    references.exact.add(source);
                }
            }
        } else if (type.isSink()) {
            for (String column : node.strings("columns")) {
                references.exact.add(ColumnReferences.normalize(column));
            }
            for (String key : node.strings("idempotencyKey")) {
                references.exact.add(ColumnReferences.normalize(key));
            }
        } else if (type == OperatorType.MAP) {
            for (Map<String, Object> item : node.mapList("columns")) {
                references.candidates.addAll(ColumnReferences.candidates(text(item.get("expression"))));
            }
        } else if (type == OperatorType.FILTER) {
            references.candidates.addAll(ColumnReferences.candidates(node.string("expression")));
            references.candidates.addAll(ColumnReferences.candidates(node.string("predicate")));
        } else if (type == OperatorType.SCRIPT) {
            references.opaque = true;
        } else if (type == OperatorType.TYPE_NORMALIZE) {
            for (Map<String, Object> item : node.mapList("rules")) {
                String column = ColumnReferences.normalize(text(item.get("column")));
                if (column != null) {
                    references.exact.add(column);
                }
            }
        } else if (type == OperatorType.DEDUP
                || type == OperatorType.AGGREGATE
                || type == OperatorType.WINDOW_AGGREGATE) {
            for (String key : node.strings("keyBy")) {
                references.exact.add(ColumnReferences.normalize(key));
            }
            for (Map<String, Object> item : node.mapList("aggregates")) {
                String column = text(item.get("column"));
                if (column != null && !"*".equals(column.trim())) {
                    references.exact.add(ColumnReferences.normalize(column));
                }
            }
        } else if (type == OperatorType.JOIN || type == OperatorType.INTERVAL_JOIN) {
            references.opaque = true;
        }
        return references;
    }

    /** 源节点是否声明了聚合下推。 */
    private boolean isAggregatePushdown(Map<String, Object> pushdown) {
        if (pushdown == null || pushdown.isEmpty()) {
            return false;
        }
        Object groupBy = pushdown.get("groupBy");
        boolean hasGroup = groupBy instanceof List && !((List<?>) groupBy).isEmpty();
        Object aggregates = pushdown.get("aggregates");
        boolean hasAggregate = aggregates instanceof List && !((List<?>) aggregates).isEmpty();
        return hasGroup || hasAggregate;
    }

    /** aggregate 节点与源声明的聚合是否一致。 */
    private boolean matches(PipelineNode node, Map<String, Object> pushdown) {
        Set<String> sourceGroups = new LinkedHashSet<String>();
        Object groupBy = pushdown.get("groupBy");
        if (groupBy instanceof List) {
            for (Object item : (List<?>) groupBy) {
                sourceGroups.add(ColumnReferences.normalize(String.valueOf(item)));
            }
        }
        Set<String> nodeKeys = new LinkedHashSet<String>();
        for (String key : node.strings("keyBy")) {
            nodeKeys.add(ColumnReferences.normalize(key));
        }
        if (!sourceGroups.equals(nodeKeys)) {
            return false;
        }
        List<Map<String, Object>> sourceAggregates = aggregatesOf(pushdown);
        List<Map<String, Object>> nodeAggregates = node.mapList("aggregates");
        if (sourceAggregates.size() != nodeAggregates.size()) {
            return false;
        }
        for (int i = 0; i < sourceAggregates.size(); i++) {
            if (!sameAggregate(sourceAggregates.get(i), nodeAggregates.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** 源声明的聚合项。 */
    private List<Map<String, Object>> aggregatesOf(Map<String, Object> pushdown) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        Object raw = pushdown.get("aggregates");
        if (!(raw instanceof List)) {
            return result;
        }
        for (Object item : (List<?>) raw) {
            if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) item;
                result.add(map);
            }
        }
        return result;
    }

    /** 两条聚合声明是否等价（fn / column / alias 都一样；alias 缺省按「fn_列」算）。 */
    private boolean sameAggregate(Map<String, Object> left, Map<String, Object> right) {
        String leftFn = function(left);
        String rightFn = function(right);
        if (leftFn == null || rightFn == null || !leftFn.equalsIgnoreCase(rightFn)) {
            return false;
        }
        String leftColumn = ColumnReferences.normalize(String.valueOf(nullToEmpty(left.get("column"))));
        String rightColumn = ColumnReferences.normalize(String.valueOf(nullToEmpty(right.get("column"))));
        if (!leftColumn.equals(rightColumn)) {
            return false;
        }
        return alias(leftFn, leftColumn, left).equals(alias(rightFn, rightColumn, right));
    }

    /** 聚合函数名（fn / function 都认）。 */
    private String function(Map<String, Object> item) {
        Object fn = item.get("fn");
        if (fn == null) {
            fn = item.get("function");
        }
        return fn == null ? null : String.valueOf(fn).trim().toUpperCase();
    }

    /** 别名（缺省按「fn_列」小写，与两端实现同口径）。 */
    private String alias(String fn, String column, Map<String, Object> item) {
        Object alias = item.get("alias");
        if (alias != null && !String.valueOf(alias).trim().isEmpty()) {
            return ColumnReferences.normalize(String.valueOf(alias));
        }
        String columnPart = column == null || column.isEmpty() ? "all" : column;
        return ColumnReferences.normalize(fn.toLowerCase() + "_" + columnPart);
    }

    /** 摘掉节点并重接它的上下游。 */
    private void removeNode(PipelineSpec spec, PipelineNode node) {
        List<PipelineEdge> in = spec.edgesTo(node.getId());
        List<PipelineEdge> out = spec.edgesFrom(node.getId());
        List<PipelineEdge> keep = new ArrayList<PipelineEdge>();
        for (PipelineEdge edge : spec.getEdges()) {
            if (edge.getFrom().equals(node.getId()) || edge.getTo().equals(node.getId())) {
                continue;
            }
            keep.add(edge);
        }
        for (PipelineEdge upstream : in) {
            for (PipelineEdge downstream : out) {
                keep.add(new PipelineEdge(upstream.getFrom(), downstream.getTo()));
            }
        }
        spec.getEdges().clear();
        spec.getEdges().addAll(keep);
        spec.getNodes().remove(node);
    }

    private String nullToEmpty(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private static String text(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    /** 列引用（精确列名 / 表达式候选 / 引用不可知）。 */
    private static final class References {

        private final Set<String> exact = new LinkedHashSet<String>();
        private final Set<String> candidates = new LinkedHashSet<String>();
        private boolean opaque;
    }
}
