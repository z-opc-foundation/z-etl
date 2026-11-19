package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 摘除死节点：到不了任何汇的节点（及其边）从图里拿掉。
 *
 * <p>保守口径：旁路输出（{@code rejectedTo}）指名的节点**不算死** ——
 * 它们不连主链也能收到被过滤的行。
 *
 * @author zifang
 */
public class EliminateUnusedRewriter implements PlanRewriter {

    @Override
    public String name() {
        return "EliminateUnused";
    }

    @Override
    public PipelineSpec rewrite(PipelineSpec spec, List<String> notes) {
        // 能到达汇的节点集合（从汇沿入边反向铺）
        Set<String> reaching = new HashSet<String>();
        List<String> queue = new ArrayList<String>();
        for (PipelineNode node : spec.getNodes()) {
            if (node.getOperatorType() != null && node.getOperatorType().isSink()) {
                reaching.add(node.getId());
                queue.add(node.getId());
            }
        }
        while (!queue.isEmpty()) {
            String current = queue.remove(0);
            for (PipelineEdge edge : spec.edgesTo(current)) {
                if (reaching.add(edge.getFrom())) {
                    queue.add(edge.getFrom());
                }
            }
        }
        // 旁路输出指名的节点也不算死
        Set<String> sideTargets = new HashSet<String>();
        for (PipelineNode node : spec.getNodes()) {
            String rejectedTo = node.string("rejectedTo");
            if (rejectedTo != null) {
                sideTargets.add(rejectedTo);
            }
        }

        List<PipelineNode> dead = new ArrayList<PipelineNode>();
        for (PipelineNode node : spec.getNodes()) {
            if (!reaching.contains(node.getId()) && !sideTargets.contains(node.getId())) {
                dead.add(node);
            }
        }
        if (dead.isEmpty()) {
            return spec;
        }
        Set<String> deadIds = new HashSet<String>();
        for (PipelineNode node : dead) {
            deadIds.add(node.getId());
        }
        List<PipelineEdge> keep = new ArrayList<PipelineEdge>();
        for (PipelineEdge edge : spec.getEdges()) {
            if (!deadIds.contains(edge.getFrom()) && !deadIds.contains(edge.getTo())) {
                keep.add(edge);
            }
        }
        spec.getEdges().clear();
        spec.getEdges().addAll(keep);
        spec.getNodes().removeAll(dead);
        for (PipelineNode node : dead) {
            notes.add("[" + name() + "] 摘除死节点 " + node.getId() + "（到不了任何汇）");
        }
        return spec;
    }
}
