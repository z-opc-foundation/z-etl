package com.zifang.z.etl.core.pipeline;

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
            if (type == OperatorType.JOIN) {
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
