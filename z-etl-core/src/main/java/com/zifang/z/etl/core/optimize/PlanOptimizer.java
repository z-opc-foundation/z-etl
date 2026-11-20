package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.pipeline.PipelineValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 计划优化器（DataFusion 的优化循环形状：固定顺序跑一轮改写规则）。
 *
 * <p>规则清单（默认四条，都保守）：
 * <ol>
 *   <li>{@link PushDownFilterRewriter}：SQL 谓词推回源库 WHERE；</li>
 *   <li>{@link ProjectionPushdownRewriter}：下游用得上的列回推到源库 SELECT 列表；</li>
 *   <li>{@link AggregatePushdownRewriter}：源声明了 GROUP BY 聚合下推 ⇒ 摘掉引擎侧重复的 aggregate 节点；</li>
 *   <li>{@link MergeMapRewriter}：相邻 map 合一；</li>
 *   <li>{@link EliminateUnusedRewriter}：摘除到不了汇的死节点。</li>
 * </ol>
 *
 * <p>投影下推的保守口径（错了就是静默丢数据）：只对 {@code jdbc.source} 生效、
 * 源必须已声明列清单、链上必须有 {@code field.mapping} 收敛输出列、水位列与平局列必保。
 * 任何一条不满足就不裁剪，并在 explain 的备注里说明原因。
 *
 * @author zifang
 */
public class PlanOptimizer {

    private final List<PlanRewriter> rewriters = new ArrayList<PlanRewriter>();

    public PlanOptimizer() {
        rewriters.add(new PushDownFilterRewriter());
        rewriters.add(new ProjectionPushdownRewriter());
        rewriters.add(new AggregatePushdownRewriter());
        rewriters.add(new MergeMapRewriter());
        rewriters.add(new EliminateUnusedRewriter());
    }

    /** 注册自定义规则（跑在默认规则之后）。 */
    public PlanOptimizer register(PlanRewriter rewriter) {
        rewriters.add(rewriter);
        return this;
    }

    /** 优化结果（改写后的声明 + 改写备注）。 */
    public static final class Optimized {

        private final PipelineSpec spec;
        private final List<String> notes;

        public Optimized(PipelineSpec spec, List<String> notes) {
            this.spec = spec;
            this.notes = notes;
        }

        public PipelineSpec spec() {
            return spec;
        }

        public List<String> notes() {
            return notes;
        }
    }

    /**
     * 优化一条声明。
     *
     * @param spec 原声明
     * @return 优化结果
     */
    public Optimized optimize(PipelineSpec spec) {
        return optimize(spec, null);
    }

    /**
     * 优化一条声明（带表结构解析）。
     *
     * @param spec     原声明
     * @param resolver 表结构解析（可为 {@code null} ⇒ 依赖表结构的规则按「不知道」保守处理）
     * @return 优化结果
     */
    public Optimized optimize(PipelineSpec spec, TableSchemaResolver resolver) {
        List<String> notes = new ArrayList<String>();
        PipelineSpec current = spec;
        for (PlanRewriter rewriter : rewriters) {
            rewriter.prepare(resolver);
            current = rewriter.rewrite(current, notes);
        }
        return new Optimized(current, notes);
    }

    /**
     * 优化并返回改写后的声明（不管备注）。
     *
     * @param spec 原声明
     * @return 优化后的声明
     */
    public PipelineSpec rewrite(PipelineSpec spec) {
        return optimize(spec).spec();
    }

    /**
     * 文本计划（Polars 的 {@code explain()} 风格）：拓扑序逐节点列出类型与关键配置。
     *
     * @param spec 流水线声明
     * @return 计划文本
     */
    public String explain(PipelineSpec spec) {
        // 优化会改写声明 ⇒ 先拷贝，别把调用方的对象改了
        Optimized optimized = optimize(PipelineParser.copyOf(spec));
        PipelineSpec plan = optimized.spec();
        StringBuilder sb = new StringBuilder();
        sb.append("pipeline: ").append(plan.getName()).append(" (mode=").append(plan.getMode()).append(")\n");
        List<String> topo = PipelineValidator.topologicalOrder(plan);
        for (String id : topo) {
            PipelineNode node = plan.node(id);
            sb.append("  ").append(id).append("  [").append(node.getType()).append(']');
            String detail = describe(node);
            if (!detail.isEmpty()) {
                sb.append(' ').append(detail);
            }
            sb.append('\n');
        }
        for (String note : optimized.notes()) {
            sb.append("  * ").append(note).append('\n');
        }
        return sb.toString();
    }

    /** 节点的关键配置摘要（explain 用）。 */
    private String describe(PipelineNode node) {
        StringBuilder sb = new StringBuilder();
        switch (node.getOperatorType()) {
            case JDBC_SOURCE:
                append(sb, "table", node.string("table"));
                append(sb, "where", node.string("where"));
                append(sb, "eventTime", node.string("eventTimeColumn"));
                List<String> columns = node.strings("columns");
                if (!columns.isEmpty()) {
                    sb.append(" columns=").append(String.join(",", columns));
                }
                break;
            case FILTER:
                append(sb, "expr", node.string("expression"));
                append(sb, "rejectedTo", node.string("rejectedTo"));
                break;
            case MAP:
                sb.append("columns=").append(node.mapList("columns").size());
                break;
            case SCRIPT:
                append(sb, "name", node.string("name"));
                append(sb, "engine", node.string("engine", "GROOVY"));
                append(sb, "mode", node.string("mode", "column"));
                break;
            case AGGREGATE:
            case WINDOW_AGGREGATE:
                append(sb, "keyBy", node.strings("keyBy").toString());
                sb.append(" aggs=").append(node.mapList("aggregates").size());
                break;
            case DEDUP:
                append(sb, "keyBy", node.strings("keyBy").toString());
                append(sb, "keep", node.string("keep", "first"));
                break;
            case JOIN:
                append(sb, "left", node.string("left"));
                append(sb, "right", node.string("right"));
                break;
            case JDBC_SINK:
                append(sb, "table", node.string("table"));
                append(sb, "writeMode", node.string("writeMode", "upsert"));
                break;
            default:
                break;
        }
        return sb.toString();
    }

    private void append(StringBuilder sb, String key, String value) {
        if (value != null) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(key).append('=').append(value);
        }
    }
}
