package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;

import java.util.List;

/**
 * 过滤下推：把 {@code filter} 节点的 SQL 谓词推回源库的 {@code WHERE}。
 *
 * <p>口径（照抄 DataFusion 的 {@code PushDownFilter}，但**保守得多**）：
 * <ul>
 *   <li>filter 节点要显式带 {@code config.sql}（SQL 谓词原文）—— EL/GROOVY 表达式
 *       不是 SQL，引擎**不猜**、不翻译，翻译错了就是数据事故；</li>
 *   <li>从 filter 往上溯到 {@code jdbc.source} 的链上，只许「行保持」算子
 *       （filter / map / script(落列) / type.normalize）—— {@code field.mapping} 会改名丢列，
 *       链上有它就不推；</li>
 *   <li>推成功后把 filter 节点的 {@code sql} 摘掉（避免重复下推），
 *       EL 表达式仍留在链上（重复过滤无害，语义等价）。</li>
 * </ul>
 *
 * @author zifang
 */
public class PushDownFilterRewriter implements PlanRewriter {

    @Override
    public String name() {
        return "PushDownFilter";
    }

    @Override
    public PipelineSpec rewrite(PipelineSpec spec, List<String> notes) {
        for (PipelineNode node : spec.getNodes()) {
            if (node.getOperatorType() != OperatorType.FILTER) {
                continue;
            }
            String sql = node.string("sql");
            if (sql == null) {
                continue;
            }
            PipelineNode source = findPushdownSource(spec, node.getId(), 0);
            if (source == null) {
                notes.add("[" + name() + "] filter 节点 " + node.getId()
                        + " 的 SQL 谓词未下推（上游链不满足行保持条件或无 jdbc.source）");
                continue;
            }
            String existing = source.string("where");
            String merged = existing == null || existing.trim().isEmpty()
                    ? "(" + sql.trim() + ")"
                    : "(" + existing.trim() + ") AND (" + sql.trim() + ")";
            source.getConfig().put("where", merged);
            node.getConfig().remove("sql");
            notes.add("[" + name() + "] filter 节点 " + node.getId() + " 的 SQL 谓词已下推到源 "
                    + source.getId() + " 的 WHERE");
        }
        return spec;
    }

    /** 从节点往上溯，找「行保持链」尽头的 jdbc.source；不满足条件返回 {@code null}。 */
    private PipelineNode findPushdownSource(PipelineSpec spec, String nodeId, int depth) {
        if (depth > 100) {
            return null;
        }
        List<PipelineEdge> in = spec.edgesTo(nodeId);
        if (in.size() != 1) {
            return null;
        }
        PipelineNode upstream = spec.node(in.get(0).getFrom());
        if (upstream == null) {
            return null;
        }
        if (upstream.getOperatorType() == OperatorType.JDBC_SOURCE) {
            return upstream;
        }
        if (!isRowPreserving(upstream)) {
            return null;
        }
        return findPushdownSource(spec, upstream.getId(), depth + 1);
    }

    /** 行保持算子：不删列、不改列名（谓词里的列名到源里还有效）。 */
    private boolean isRowPreserving(PipelineNode node) {
        OperatorType type = node.getOperatorType();
        if (type == OperatorType.FILTER || type == OperatorType.MAP || type == OperatorType.TYPE_NORMALIZE) {
            return true;
        }
        if (type == OperatorType.SCRIPT) {
            // mode=row 整行替换 ⇒ 列名可能全变 ⇒ 不保持
            return "column".equalsIgnoreCase(node.string("mode", "column"));
        }
        return false;
    }
}
