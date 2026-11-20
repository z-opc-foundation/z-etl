package com.zifang.z.etl.core.pipeline;

import com.zifang.z.etl.core.job.AggregateSpec;
import com.zifang.util.db.support.Identifiers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 流水线声明校验器（跑之前先把「图不成立」这件事挑出来）。
 *
 * <p>校验口径（照抄 Flink 的 JobGraph 校验思路）：
 * <ol>
 *   <li>节点 id 唯一、类型已知；</li>
 *   <li>边引用的节点必须存在、不许自环；</li>
 *   <li>图必须无环（Kahn 拓扑排序）；</li>
 *   <li>至少一个源、至少一个汇；源不许有入边、汇不许有出边；</li>
 *   <li>join 节点必须正好两条入边且 {@code left}/{@code right} 指名到位。</li>
 * </ol>
 *
 * @author zifang
 */
public final class PipelineValidator {

    private PipelineValidator() {
    }

    /**
     * 校验流水线声明；不合法直接抛 {@link IllegalArgumentException}（消息自带定位信息）。
     *
     * @param spec 流水线声明
     */
    public static void validate(PipelineSpec spec) {
        if (spec == null) {
            throw new IllegalArgumentException("流水线声明为空");
        }
        if (spec.getName() == null || spec.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("流水线缺少 name");
        }
        if (spec.getNodes() == null || spec.getNodes().isEmpty()) {
            throw new IllegalArgumentException("流水线 " + spec.getName() + " 没有节点");
        }

        Map<String, PipelineNode> index = new LinkedHashMap<String, PipelineNode>();
        for (PipelineNode node : spec.getNodes()) {
            if (node == null) {
                throw new IllegalArgumentException("流水线 " + spec.getName() + " 存在空节点");
            }
            if (node.getId() == null || node.getId().trim().isEmpty()) {
                throw new IllegalArgumentException("流水线 " + spec.getName() + " 存在没有 id 的节点");
            }
            if (index.containsKey(node.getId())) {
                throw new IllegalArgumentException("节点 id 重复: " + node.getId());
            }
            if (node.getOperatorType() == null) {
                OperatorType resolved = OperatorType.fromId(node.getType());
                if (resolved == null) {
                    throw new IllegalArgumentException("节点 " + node.getId() + " 的类型无法识别: " + node.getType()
                            + "（可用类型: " + OperatorType.allIds() + "）");
                }
                node.setOperatorType(resolved);
            }
            index.put(node.getId(), node);
        }

        for (PipelineEdge edge : spec.getEdges()) {
            if (edge == null) {
                throw new IllegalArgumentException("流水线 " + spec.getName() + " 存在空边");
            }
            if (edge.getFrom() == null || edge.getTo() == null) {
                throw new IllegalArgumentException("边缺少 from/to: " + edge);
            }
            if (!index.containsKey(edge.getFrom())) {
                throw new IllegalArgumentException("边 " + edge + " 的上游节点不存在: " + edge.getFrom());
            }
            if (!index.containsKey(edge.getTo())) {
                throw new IllegalArgumentException("边 " + edge + " 的下游节点不存在: " + edge.getTo());
            }
            if (edge.getFrom().equals(edge.getTo())) {
                throw new IllegalArgumentException("边不允许自环: " + edge);
            }
        }

        int sources = 0;
        int sinks = 0;
        for (PipelineNode node : index.values()) {
            OperatorType type = node.getOperatorType();
            List<PipelineEdge> in = spec.edgesTo(node.getId());
            List<PipelineEdge> out = spec.edgesFrom(node.getId());
            if (type.isSource()) {
                sources++;
                if (!in.isEmpty()) {
                    throw new IllegalArgumentException("源节点 " + node.getId() + " 不允许有入边");
                }
            }
            if (type.isSink()) {
                sinks++;
                if (!out.isEmpty()) {
                    throw new IllegalArgumentException("汇节点 " + node.getId() + " 不允许有出边");
                }
            }
            if (type == OperatorType.JDBC_SOURCE) {
                List<String> pushdownErrors = checkSourcePushdown(node);
                if (!pushdownErrors.isEmpty()) {
                    throw new IllegalArgumentException("流水线 " + spec.getName() + " 的源节点 "
                            + node.getId() + " 下推声明不合法: " + String.join("; ", pushdownErrors));
                }
            }
            if (type == OperatorType.JOIN || type == OperatorType.INTERVAL_JOIN
                        || type == OperatorType.DIM_JOIN) {
                if (in.size() != 2) {
                    throw new IllegalArgumentException("join 节点 " + node.getId() + " 必须正好两条入边，实际 " + in.size());
                }
                String left = node.string("left");
                String right = node.string("right");
                if (left == null || right == null) {
                    throw new IllegalArgumentException("join 节点 " + node.getId() + " 必须声明 config.left 与 config.right");
                }
                if (!index.containsKey(left) || !index.containsKey(right)) {
                    throw new IllegalArgumentException("join 节点 " + node.getId() + " 的 left/right 指向了不存在的节点");
                }
            }
        }
        if (sources == 0) {
            throw new IllegalArgumentException("流水线 " + spec.getName() + " 没有源节点");
        }
        if (sinks == 0) {
            throw new IllegalArgumentException("流水线 " + spec.getName() + " 没有汇节点");
        }

        topologicalOrder(spec, index);
    }

    /**
     * 源节点的下推声明校验（过滤 / 分组 / 聚合）。
     *
     * <p>纪律：聚合表达式**不是**自由 SQL，而是「函数白名单 + 列名白名单」的组合
     * （与线性层 {@code AggregateSpec} 同口径）—— 聚合表达式无法参数绑定，拼文本就是注入面。
     */
    private static List<String> checkSourcePushdown(PipelineNode node) {
        List<String> errors = new ArrayList<String>();
        Map<String, Object> pushdown = node.map("pushdown");
        if (pushdown.isEmpty()) {
            return errors;
        }
        List<String> groupBy = new ArrayList<String>();
        Object groupByRaw = pushdown.get("groupBy");
        if (groupByRaw instanceof List) {
            for (Object item : (List<?>) groupByRaw) {
                String column = text(item);
                if (column != null) {
                    groupBy.add(column);
                }
            }
        }
        for (String column : groupBy) {
            if (!Identifiers.isSafe(column)) {
                errors.add("节点 " + node.getId() + " 的 pushdown.groupBy 含非法列标识符: " + column);
            }
        }
        List<Map<String, Object>> aggregates = new ArrayList<Map<String, Object>>();
        Object aggregatesRaw = pushdown.get("aggregates");
        if (aggregatesRaw instanceof List) {
            for (Object item : (List<?>) aggregatesRaw) {
                if (item instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) item;
                    aggregates.add(map);
                }
            }
        }
        for (Map<String, Object> item : aggregates) {
            String fn = text(item.get("fn"));
            if (fn == null) {
                fn = text(item.get("function"));
            }
            String column = text(item.get("column"));
            String alias = text(item.get("alias"));
            if (fn == null) {
                errors.add("节点 " + node.getId() + " 的 pushdown.aggregates 缺少 fn");
                continue;
            }
            boolean allowed = false;
            for (String safe : AggregateSpec.SAFE_FUNCTIONS) {
                if (safe.equalsIgnoreCase(fn.trim())) {
                    allowed = true;
                    break;
                }
            }
            if (!allowed) {
                errors.add("节点 " + node.getId() + " 的 pushdown.aggregates 聚合函数不支持: " + fn
                        + "（SQL 下推可用: " + String.join("/", AggregateSpec.SAFE_FUNCTIONS) + "）");
            }
            if (column == null || "*".equals(column.trim())) {
                if (!"*".equals(column == null ? "" : column.trim())) {
                    errors.add("节点 " + node.getId() + " 的 pushdown.aggregates 缺聚合列");
                } else if (!"COUNT".equalsIgnoreCase(fn.trim())) {
                    errors.add("节点 " + node.getId() + " 的 pushdown.aggregates 只有 COUNT 允许 *");
                }
            } else if (!Identifiers.isSafe(column)) {
                errors.add("节点 " + node.getId() + " 的 pushdown.aggregates 含非法列标识符: " + column);
            }
            if (alias != null && !Identifiers.isSafe(alias)) {
                errors.add("节点 " + node.getId() + " 的 pushdown.aggregates 含非法别名: " + alias);
            }
        }
        boolean aggregatePushdown = !groupBy.isEmpty() || !aggregates.isEmpty();
        if (aggregatePushdown && node.string("eventTimeColumn") != null) {
            errors.add("节点 " + node.getId() + " 不能同时声明聚合下推与 eventTimeColumn"
                    + "（聚合结果是分组口径，水位过滤对它没有意义）");
        }
        return errors;
    }

    /** 取文本（空串 → null）。 */
    private static String text(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    /**
     * 拓扑排序（Kahn）；有环直接报错，并给出环上的节点。
     *
     * @param spec  流水线声明
     * @param index 节点索引
     * @return 拓扑序（源在前、汇在后）
     */
    public static List<String> topologicalOrder(PipelineSpec spec, Map<String, PipelineNode> index) {
        Map<String, Integer> inDegree = new HashMap<String, Integer>();
        Map<String, List<String>> adjacency = new HashMap<String, List<String>>();
        for (String id : index.keySet()) {
            inDegree.put(id, Integer.valueOf(0));
            adjacency.put(id, new ArrayList<String>());
        }
        for (PipelineEdge edge : spec.getEdges()) {
            List<String> targets = adjacency.get(edge.getFrom());
            if (targets != null) {
                targets.add(edge.getTo());
            }
            Integer degree = inDegree.get(edge.getTo());
            if (degree != null) {
                inDegree.put(edge.getTo(), Integer.valueOf(degree.intValue() + 1));
            }
        }
        List<String> order = new ArrayList<String>();
        List<String> ready = new ArrayList<String>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue().intValue() == 0) {
                ready.add(entry.getKey());
            }
        }
        while (!ready.isEmpty()) {
            String current = ready.remove(0);
            order.add(current);
            List<String> targets = adjacency.get(current);
            if (targets == null) {
                continue;
            }
            for (String target : targets) {
                Integer degree = inDegree.get(target);
                if (degree == null) {
                    continue;
                }
                int updated = degree.intValue() - 1;
                inDegree.put(target, Integer.valueOf(updated));
                if (updated == 0) {
                    ready.add(target);
                }
            }
        }
        if (order.size() != index.size()) {
            Set<String> cyclic = new HashSet<String>();
            for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
                if (entry.getValue().intValue() > 0) {
                    cyclic.add(entry.getKey());
                }
            }
            throw new IllegalArgumentException("流水线 " + spec.getName() + " 存在环，涉及节点: " + cyclic);
        }
        return order;
    }

    /** 拓扑序（便捷入口；内部自建索引）。 */
    public static List<String> topologicalOrder(PipelineSpec spec) {
        Map<String, PipelineNode> index = new LinkedHashMap<String, PipelineNode>();
        for (PipelineNode node : spec.getNodes()) {
            if (node != null && node.getId() != null) {
                index.put(node.getId(), node);
            }
        }
        return topologicalOrder(spec, index);
    }
}
