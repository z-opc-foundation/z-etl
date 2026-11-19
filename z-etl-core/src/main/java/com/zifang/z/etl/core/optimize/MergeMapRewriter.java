package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 合并相邻映射：连续两个 {@code map} 节点合成一个。
 *
 * <p>为什么等价：表达式算子是「在累积行上逐条求值」—— 先跑 [e1,e2] 再跑 [e3]，
 * 与一次跑 [e1,e2,e3] 完全一致（e3 在两种情况里都能看到 e1/e2 的产出列）。
 * 合并后少一次行拷贝、少一个图节点。
 *
 * @author zifang
 */
public class MergeMapRewriter implements PlanRewriter {

    @Override
    public String name() {
        return "MergeMap";
    }

    @Override
    public PipelineSpec rewrite(PipelineSpec spec, List<String> notes) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (PipelineNode node : spec.getNodes()) {
                if (node.getOperatorType() != OperatorType.MAP) {
                    continue;
                }
                List<PipelineEdge> in = spec.edgesTo(node.getId());
                if (in.size() != 1) {
                    continue;
                }
                PipelineNode upstream = spec.node(in.get(0).getFrom());
                if (upstream == null || upstream.getOperatorType() != OperatorType.MAP) {
                    continue;
                }
                List<PipelineEdge> upstreamOut = spec.edgesFrom(upstream.getId());
                if (upstreamOut.size() != 1) {
                    continue;
                }
                List<Map<String, Object>> merged = new ArrayList<Map<String, Object>>(
                        upstream.mapList("columns"));
                merged.addAll(node.mapList("columns"));
                node.getConfig().put("columns", merged);

                // 重接边：上游的上游 → 本节点；摘掉上游节点
                List<PipelineEdge> upstreamIn = spec.edgesTo(upstream.getId());
                List<PipelineEdge> keep = new ArrayList<PipelineEdge>();
                for (PipelineEdge edge : spec.getEdges()) {
                    if (edge.getTo().equals(node.getId()) && edge.getFrom().equals(upstream.getId())) {
                        continue;
                    }
                    if (edge.getFrom().equals(upstream.getId()) || edge.getTo().equals(upstream.getId())) {
                        continue;
                    }
                    keep.add(edge);
                }
                for (PipelineEdge edge : upstreamIn) {
                    keep.add(new PipelineEdge(edge.getFrom(), node.getId()));
                }
                spec.getEdges().clear();
                spec.getEdges().addAll(keep);
                spec.getNodes().remove(upstream);
                notes.add("[" + name() + "] map 节点 " + upstream.getId() + " 并入 " + node.getId());
                changed = true;
                break;
            }
        }
        return spec;
    }
}
