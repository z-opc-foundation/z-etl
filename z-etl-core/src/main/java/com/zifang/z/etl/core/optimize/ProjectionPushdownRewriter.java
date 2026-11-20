package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.pipeline.PipelineValidator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;

/**
 * 投影下推：把「下游真正用得上的列」回推到 {@code jdbc.source} 的 SELECT 列表。
 *
 * <p>口径（保守优先，错了就是静默丢数据）：
 * <ol>
 *   <li><b>只对 {@code jdbc.source} 生效</b> —— SQL 投影是确定的；文件 / HTTP / 消息源的
 *       {@code columns} 语义各不相同，不碰；</li>
 *   <li><b>源必须已声明列清单</b>：裁剪后的列是「声明列 ∩ 需要的列」⇒ SQL 一定有效；
 *       没声明列清单（全列）就不裁剪（不知道表长什么样，不敢猜）；</li>
 *   <li><b>必须有 {@code field.mapping} 兜底</b>：它是「整行替换」语义，输出列确定
 *       ⇒ 下游要什么才清晰；链上没有它 ⇒ 下游需求未知 ⇒ 不裁剪；</li>
 *   <li><b>水位列 / 平局列必保</b>：{@code eventTimeColumn} 与 {@code resume.tiebreakColumn}
 *       裁掉 ⇒ 水位失效 ⇒ 增量变全量（隐性数据事故）；</li>
 *   <li><b>join / 整行脚本 / 多路异构合并 ⇒ 不裁剪</b>（列名与列集合不可知）。</li>
 * </ol>
 *
 * <p>传播算法（DataFusion 的形状）：从汇出发**逆拓扑序**回推「每个节点被需要的列」，
 * 行保持算子把需求原样传给上游，{@code field.mapping} 把「目标列」翻译成「源列」，
 * 到源处与声明列求交即得最终投影。
 *
 * @author zifang
 */
public class ProjectionPushdownRewriter implements PlanRewriter {

    @Override
    public String name() {
        return "ProjectionPushdown";
    }

    @Override
    public PipelineSpec rewrite(PipelineSpec spec, List<String> notes) {
        Map<String, PipelineNode> index = new LinkedHashMap<String, PipelineNode>();
        for (PipelineNode node : spec.getNodes()) {
            if (node != null && node.getId() != null) {
                index.put(node.getId(), node);
            }
        }
        List<String> topo = PipelineValidator.topologicalOrder(spec, index);

        // 需求传播：need[node] = 下游需要该节点**输出**的列
        Map<String, ColumnNeed> need = new LinkedHashMap<String, ColumnNeed>();
        for (PipelineNode node : spec.getNodes()) {
            if (node != null && node.getOperatorType() != null && node.getOperatorType().isSink()) {
                need.put(node.getId(), ColumnNeed.all());
            }
        }

        // 逆拓扑序：下游先算
        for (int i = topo.size() - 1; i >= 0; i--) {
            String id = topo.get(i);
            PipelineNode node = index.get(id);
            ColumnNeed current = need.get(id);
            if (current == null) {
                continue;
            }
            ColumnNeed input = inputNeed(spec, node, current, need, index);
            for (PipelineEdge edge : spec.edgesTo(id)) {
                PipelineNode upstream = index.get(edge.getFrom());
                if (upstream == null) {
                    continue;
                }
                ColumnNeed merged = need.get(upstream.getId());
                need.put(upstream.getId(), merged == null ? input : merged.merge(input));
            }
        }

        // 收窄源节点的 SELECT 列表
        for (PipelineNode node : spec.getNodes()) {
            if (node == null || node.getOperatorType() != OperatorType.JDBC_SOURCE) {
                continue;
            }
            narrowSource(node, need.get(node.getId()), notes);
        }
        return spec;
    }

    /** 节点「需要从上游拿哪些列」。 */
    private ColumnNeed inputNeed(PipelineSpec spec, PipelineNode node, ColumnNeed current,
                                  Map<String, ColumnNeed> need, Map<String, PipelineNode> index) {
        OperatorType type = node.getOperatorType();
        if (type == OperatorType.FIELD_MAPPING) {
            return mappingInputNeed(node, current);
        }
        if (type == OperatorType.SCRIPT) {
            if (!"column".equalsIgnoreCase(node.string("mode", "column"))) {
                // 整行替换：脚本返回什么列不可知 ⇒ 全要
                return ColumnNeed.all();
            }
        }
        if (type == OperatorType.JOIN || type == OperatorType.INTERVAL_JOIN
                || type == OperatorType.DIM_JOIN || type == OperatorType.CEP_PATTERN) {
            // 左右两路列名与列集合交织 / CEP 整行进部分匹配状态 ⇒ 不裁
            return ColumnNeed.all();
        }
        if (type.isSink() || type.isSource()) {
            return current;
        }
        // 行保持算子：下游要的列原样传给上游，另加算子自身要的列
        ColumnNeed result = current;
        Set<String> own = operatorColumns(node);
        if (!own.isEmpty()) {
            result = result.merge(ColumnNeed.of(own));
        }
        // 旁路输出：被拒的行也来自上游 ⇒ 上游需要的列是两者的并集
        String rejectedTo = node.string("rejectedTo");
        if (rejectedTo != null) {
            ColumnNeed side = need.get(rejectedTo);
            if (side != null) {
                result = result.merge(side);
            }
        }
        return result;
    }

    /**
     * {@code field.mapping} 的输入需求：下游需要的**目标列**翻译成**源列**。
     *
     * <p>下游要「全部」时，取全部映射的源列（该节点的输出列就是全部 target，集合确定）。
     */
    private ColumnNeed mappingInputNeed(PipelineNode node, ColumnNeed current) {
        Set<String> sources = new LinkedHashSet<String>();
        for (Map<String, Object> item : node.mapList("mappings")) {
            String source = ColumnReferences.normalize(text(item.get("source")));
            String target = ColumnReferences.normalize(text(item.get("target")));
            if (source == null) {
                continue;
            }
            String targetName = target == null ? source : target;
            if (current.isAll() || current.columns().contains(targetName)) {
                sources.add(source);
            }
        }
        return ColumnNeed.of(sources);
    }

    /** url 模板里的 {@code {列名}} 占位符（与 async.map 同口径）。 */
    private static final Pattern TEMPLATE_COLUMN = Pattern.compile("\\{([A-Za-z0-9_]+)\\}");

    /** 算子自身需要的列（表达式 / 脚本引用、键列、聚合列、归一列）。 */
    private Set<String> operatorColumns(PipelineNode node) {
        Set<String> columns = new LinkedHashSet<String>();
        OperatorType type = node.getOperatorType();
        if (type == OperatorType.FILTER) {
            columns.addAll(ColumnReferences.candidates(node.string("expression")));
            columns.addAll(ColumnReferences.candidates(node.string("predicate")));
        } else if (type == OperatorType.MAP) {
            for (Map<String, Object> item : node.mapList("columns")) {
                columns.addAll(ColumnReferences.candidates(text(item.get("expression"))));
            }
        } else if (type == OperatorType.SCRIPT) {
            columns.addAll(ColumnReferences.candidates(node.string("script")));
        } else if (type == OperatorType.TYPE_NORMALIZE) {
            for (Map<String, Object> item : node.mapList("rules")) {
                columns.addAll(ColumnReferences.candidates(text(item.get("column"))));
            }
        } else if (type == OperatorType.ASSERT) {
            // 断言读的行列（表达式 + 各判定的列）必须活着传到它那儿，否则校验的是空值
            columns.addAll(ColumnReferences.candidates(node.string("expression")));
            for (String column : node.map("inRange").keySet()) {
                columns.add(ColumnReferences.normalize(column));
            }
            for (String column : node.map("allowedValues").keySet()) {
                columns.add(ColumnReferences.normalize(column));
            }
            for (String column : node.map("notNullRate").keySet()) {
                columns.add(ColumnReferences.normalize(column));
            }
            for (String column : node.strings("unique")) {
                columns.add(ColumnReferences.normalize(column));
            }
        } else if (type == OperatorType.ASYNC_MAP) {
            // url 模板里的 {列名} 与 body 列都得活着传到它那儿
            Matcher template = TEMPLATE_COLUMN.matcher(node.string("url") == null ? "" : node.string("url"));
            while (template.find()) {
                columns.add(template.group(1));
            }
            columns.addAll(node.strings("bodyColumns"));
        } else if (type == OperatorType.DEDUP
                || type == OperatorType.AGGREGATE
                || type == OperatorType.WINDOW_AGGREGATE) {
            for (String key : node.strings("keyBy")) {
                columns.add(ColumnReferences.normalize(key));
            }
            for (Map<String, Object> item : node.mapList("aggregates")) {
                columns.addAll(ColumnReferences.candidates(text(item.get("column"))));
            }
        }
        columns.remove(null);
        return columns;
    }

    /** 收窄源节点的列清单。 */
    private void narrowSource(PipelineNode node, ColumnNeed needed, List<String> notes) {
        Map<String, Object> pushdown = node.map("pushdown");
        Object groupBy = pushdown.get("groupBy");
        Object aggregates = pushdown.get("aggregates");
        boolean aggregatePushdown = (groupBy instanceof List && !((List<?>) groupBy).isEmpty())
                || (aggregates instanceof List && !((List<?>) aggregates).isEmpty());
        if (aggregatePushdown) {
            // 聚合下推的 SELECT 列表由 GROUP BY + 聚合表达式决定 ⇒ 列裁剪不适用
            notes.add("[" + name() + "] 源节点 " + node.getId()
                    + " 声明了聚合下推（SELECT 列表由 GROUP BY 决定），不裁剪投影");
            return;
        }
        List<String> declared = node.strings("columns");
        if (declared.isEmpty()) {
            notes.add("[" + name() + "] 源节点 " + node.getId()
                    + " 未声明列清单（全列取数），无法安全裁剪投影");
            return;
        }
        if (needed == null || needed.isAll()) {
            notes.add("[" + name() + "] 源节点 " + node.getId()
                    + " 的下游用得上全部列（链上没有 field.mapping 收敛输出列），不裁剪");
            return;
        }
        Set<String> keep = new LinkedHashSet<String>();
        for (String column : declared) {
            if (needed.columns().contains(ColumnReferences.normalize(column))) {
                keep.add(column);
            }
        }
        // 水位列 / 平局列必保（裁掉 ⇒ 水位失效 ⇒ 增量变全量）
        for (String mandatory : mandatoryColumns(node)) {
            for (String column : declared) {
                if (ColumnReferences.normalize(column).equals(ColumnReferences.normalize(mandatory))) {
                    keep.add(column);
                }
            }
        }
        if (keep.isEmpty()) {
            notes.add("[" + name() + "] 源节点 " + node.getId() + " 算不出需要的列（保守不裁剪）");
            return;
        }
        if (keep.size() == declared.size()) {
            return;
        }
        node.getConfig().put("columns", new ArrayList<String>(keep));
        notes.add("[" + name() + "] 源节点 " + node.getId() + " 的 SELECT 列表由 " + declared.size()
                + " 列收窄为 " + keep.size() + " 列：" + String.join(", ", keep));
    }

    /** 水位口径相关的列（必保）。 */
    private List<String> mandatoryColumns(PipelineNode node) {
        List<String> columns = new ArrayList<String>();
        String eventTime = node.string("eventTimeColumn");
        if (eventTime != null) {
            columns.add(eventTime);
        }
        Map<String, Object> resume = node.map("resume");
        String tiebreak = text(resume.get("tiebreakColumn"));
        if (tiebreak != null) {
            columns.add(tiebreak);
        }
        String resumeColumn = text(resume.get("column"));
        if (resumeColumn != null) {
            columns.add(resumeColumn);
        }
        return columns;
    }

    private static String text(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    /**
     * 列需求：{@code all=true} 表示「下游要全部列」（集合不可知）。
     */
    static final class ColumnNeed {

        private final boolean all;
        private final Set<String> columns;

        private ColumnNeed(boolean all, Set<String> columns) {
            this.all = all;
            this.columns = columns;
        }

        static ColumnNeed all() {
            return new ColumnNeed(true, new LinkedHashSet<String>());
        }

        static ColumnNeed of(Set<String> columns) {
            Set<String> copy = new LinkedHashSet<String>();
            if (columns != null) {
                for (String column : columns) {
                    if (column != null) {
                        copy.add(column);
                    }
                }
            }
            return new ColumnNeed(false, copy);
        }

        boolean isAll() {
            return all;
        }

        Set<String> columns() {
            return columns;
        }

        /** 并集：任一方是「全部」就是「全部」。 */
        ColumnNeed merge(ColumnNeed other) {
            if (other == null) {
                return this;
            }
            if (all || other.all) {
                return all();
            }
            Set<String> merged = new LinkedHashSet<String>(columns);
            merged.addAll(other.columns);
            return of(merged);
        }
    }
}
