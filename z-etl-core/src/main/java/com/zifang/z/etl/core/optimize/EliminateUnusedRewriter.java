package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 摘除死节点：到不了任何汇的节点（及其边）从图里拿掉。
 *
 * <p>保守口径：旁路输出指名的节点**不算死** —— 它们不连主链也能收到行（标签投递）。
 * 反向可达铺开走两种「边」：
 * <ul>
 *   <li>声明边（{@code from → to}）；</li>
 *   <li>旁路声明（目标 → **声明节点**）：{@code filter.rejectedTo} / {@code window.aggregate.lateTo} /
 *       {@code dedup.duplicateTo} / {@code route.rules[].to} + {@code fallback} / {@code profile.reportTo}
 *       —— route 这类「无主输出」节点全靠标签喂下游，声明边铺不到它们，必须按虚拟边保活。</li>
 * </ul>
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
        // 旁路目标 → 声明该目标的节点
        Map<String, List<String>> declarersOf = new LinkedHashMap<String, List<String>>();
        for (PipelineNode node : spec.getNodes()) {
            for (String target : sideTargetsOf(node)) {
                List<String> declarers = declarersOf.get(target);
                if (declarers == null) {
                    declarers = new ArrayList<String>();
                    declarersOf.put(target, declarers);
                }
                declarers.add(node.getId());
            }
        }

        // 可达集：从汇与旁路目标出发，沿「入边 + 旁路声明」反向铺
        Set<String> reaching = new HashSet<String>();
        List<String> queue = new ArrayList<String>();
        for (PipelineNode node : spec.getNodes()) {
            if (node.getOperatorType() != null && node.getOperatorType().isSink()) {
                reaching.add(node.getId());
                queue.add(node.getId());
            }
        }
        for (String target : declarersOf.keySet()) {
            if (reaching.add(target)) {
                queue.add(target);
            }
        }
        while (!queue.isEmpty()) {
            String current = queue.remove(0);
            for (PipelineEdge edge : spec.edgesTo(current)) {
                if (reaching.add(edge.getFrom())) {
                    queue.add(edge.getFrom());
                }
            }
            for (String declarer : declarersOf.containsKey(current)
                    ? declarersOf.get(current) : Collections.<String>emptyList()) {
                if (reaching.add(declarer)) {
                    queue.add(declarer);
                }
            }
        }

        List<PipelineNode> dead = new ArrayList<PipelineNode>();
        for (PipelineNode node : spec.getNodes()) {
            if (!reaching.contains(node.getId())) {
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

    /** 该节点声明了哪些旁路目标（rejectedTo / lateTo / duplicateTo / fallback / reportTo / rules[].to）。 */
    private static List<String> sideTargetsOf(PipelineNode node) {
        List<String> targets = new ArrayList<String>();
        for (String key : Arrays.asList("rejectedTo", "lateTo", "duplicateTo", "fallback", "reportTo")) {
            String target = node.string(key);
            if (target != null && !target.trim().isEmpty()) {
                targets.add(target.trim());
            }
        }
        for (java.util.Map<String, Object> rule : node.mapList("rules")) {
            Object to = rule.get("to");
            if (to != null && !String.valueOf(to).trim().isEmpty()) {
                targets.add(String.valueOf(to).trim());
            }
        }
        return targets;
    }
}
