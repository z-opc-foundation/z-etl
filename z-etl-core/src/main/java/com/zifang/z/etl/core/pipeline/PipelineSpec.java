package com.zifang.z.etl.core.pipeline;

import com.zifang.z.etl.core.job.ExpressionSpec;
import com.zifang.z.etl.core.job.FieldMapping;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.RetrySpec;
import com.zifang.z.etl.core.job.ScriptSpec;
import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.z.etl.core.job.TargetSpec;
import com.zifang.z.etl.core.job.TypeNormalizationSpec;
import com.zifang.z.etl.core.job.WatermarkSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 流水线声明（**Flink 式 DAG**）。
 *
 * <p>与旧版 {@link JobDescriptor} 的关系：旧版是「固定四段链」（source → transform → target），
 * 新版是「节点 + 边」的图；旧版描述可以**编译**成新版图（{@link #fromJobDescriptor}），
 * 于是旧作业不迁也能吃上新引擎（同一套算子、同一套水位、同一套对账）。
 *
 * <pre>
 * job:
 *   name: order-flow
 *   mode: auto
 *   nodes:
 *     - {id: src, type: jdbc.source, config: {...}}
 *     - {id: clean, type: script, config: {...}}
 *     - {id: agg, type: window.aggregate, config: {...}}
 *     - {id: dst, type: jdbc.sink, config: {...}}
 *   edges:
 *     - {from: src, to: clean}
 *     - {from: clean, to: agg}
 *     - {from: agg, to: dst}
 * </pre>
 *
 * @author zifang
 */
public class PipelineSpec {

    /** 流水线名（全局唯一，落库主键）。 */
    private String name;

    /** 流水线说明（人读）。 */
    private String description;

    /** 运行模式：{@code auto}（有状态则续跑）/ {@code init}（清状态重跑）/ {@code incremental}（必须续跑）。 */
    private String mode = "auto";

    /** 节点。 */
    private List<PipelineNode> nodes = new ArrayList<PipelineNode>();

    /** 边。 */
    private List<PipelineEdge> edges = new ArrayList<PipelineEdge>();

    /** 检查点配置（间隔 / 模式 / 状态后端）。 */
    private Map<String, Object> checkpoint = new LinkedHashMap<String, Object>();

    /** 失败重试策略（默认不重试）。 */
    private RetrySpec retry;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public List<PipelineNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<PipelineNode> nodes) {
        this.nodes = nodes == null ? new ArrayList<PipelineNode>() : nodes;
    }

    public List<PipelineEdge> getEdges() {
        return edges;
    }

    public void setEdges(List<PipelineEdge> edges) {
        this.edges = edges == null ? new ArrayList<PipelineEdge>() : edges;
    }

    public Map<String, Object> getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(Map<String, Object> checkpoint) {
        this.checkpoint = checkpoint == null ? new LinkedHashMap<String, Object>() : checkpoint;
    }

    public RetrySpec getRetry() {
        return retry;
    }

    public void setRetry(RetrySpec retry) {
        this.retry = retry;
    }

    /** 按 id 取节点；取不到返回 {@code null}。 */
    public PipelineNode node(String id) {
        if (id == null) {
            return null;
        }
        for (PipelineNode node : nodes) {
            if (node != null && id.equals(node.getId())) {
                return node;
            }
        }
        return null;
    }

    /** 某节点的出边。 */
    public List<PipelineEdge> edgesFrom(String nodeId) {
        List<PipelineEdge> result = new ArrayList<PipelineEdge>();
        for (PipelineEdge edge : edges) {
            if (edge != null && nodeId != null && nodeId.equals(edge.getFrom())) {
                result.add(edge);
            }
        }
        return result;
    }

    /** 某节点的入边。 */
    public List<PipelineEdge> edgesTo(String nodeId) {
        List<PipelineEdge> result = new ArrayList<PipelineEdge>();
        for (PipelineEdge edge : edges) {
            if (edge != null && nodeId != null && nodeId.equals(edge.getTo())) {
                result.add(edge);
            }
        }
        return result;
    }

    /** 源节点。 */
    public List<PipelineNode> sources() {
        return byKind(OperatorType.Kind.SOURCE);
    }

    /** 汇节点。 */
    public List<PipelineNode> sinks() {
        return byKind(OperatorType.Kind.SINK);
    }

    private List<PipelineNode> byKind(OperatorType.Kind kind) {
        List<PipelineNode> result = new ArrayList<PipelineNode>();
        for (PipelineNode node : nodes) {
            if (node != null && node.getOperatorType() != null && node.getOperatorType().kind() == kind) {
                result.add(node);
            }
        }
        return result;
    }

    /**
     * 旧版线性作业描述 → 流水线图（**兼容编译**）。
     *
     * <p>旧描述的四段链被摊平成一串节点：
     * {@code jdbc.source → field.mapping → map(表达式) → script → type.normalize → jdbc.sink}。
     * 某一段为空就不生成对应节点（空节点会变成哑算子，白跑一遍）。
     *
     * @param descriptor 旧版描述
     * @return 流水线声明
     */
    public static PipelineSpec fromJobDescriptor(JobDescriptor descriptor) {
        PipelineSpec spec = new PipelineSpec();
        spec.setName(descriptor.getName());
        spec.setDescription(descriptor.getDescription());
        spec.setMode(descriptor.getMode());

        List<String> chain = new ArrayList<String>();

        SourceSpec source = descriptor.getSource();
        if (source != null) {
            PipelineNode node = new PipelineNode("source", OperatorType.JDBC_SOURCE);
            Map<String, Object> config = node.getConfig();
            config.put("dataSourceRef", source.getDataSourceRef());
            config.put("table", source.getTable());
            if (source.getColumns() != null && !source.getColumns().isEmpty()) {
                config.put("columns", new ArrayList<String>(source.getColumns()));
            }
            if (source.getWhere() != null && !source.getWhere().trim().isEmpty()) {
                config.put("where", source.getWhere().trim());
            }
            if (source.getOrderBy() != null && !source.getOrderBy().trim().isEmpty()) {
                config.put("orderBy", source.getOrderBy().trim());
            }
            config.put("pageSize", Integer.valueOf(source.getPageSize()));

            WatermarkSpec watermark = descriptor.getWatermark();
            String eventTimeColumn = null;
            if (watermark != null && watermark.getColumn() != null && !watermark.getColumn().trim().isEmpty()) {
                eventTimeColumn = watermark.getColumn().trim();
                config.put("eventTimeColumn", eventTimeColumn);
                config.put("eventTimeType", watermark.getType() == null ? "TIMESTAMP" : watermark.getType());
                Map<String, Object> resume = new LinkedHashMap<String, Object>();
                resume.put("column", eventTimeColumn);
                resume.put("type", watermark.getType() == null ? "TIMESTAMP" : watermark.getType());
                if (watermark.getTiebreakColumn() != null && !watermark.getTiebreakColumn().trim().isEmpty()) {
                    resume.put("tiebreakColumn", watermark.getTiebreakColumn().trim());
                }
                resume.put("overlap", Long.valueOf(Math.max(0L, watermark.getOverlap())));
                config.put("resume", resume);
            } else if (source.getWatermarkColumn() != null && !source.getWatermarkColumn().trim().isEmpty()) {
                eventTimeColumn = source.getWatermarkColumn().trim();
                config.put("eventTimeColumn", eventTimeColumn);
                config.put("eventTimeType", "TIMESTAMP");
            }
            spec.getNodes().add(node);
            chain.add("source");
        }

        if (descriptor.getTransform() != null) {
            List<FieldMapping> mappings = descriptor.getTransform().getFieldMappings();
            if (mappings != null && !mappings.isEmpty()) {
                PipelineNode node = new PipelineNode("fieldMapping", OperatorType.FIELD_MAPPING);
                List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
                for (FieldMapping mapping : mappings) {
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("source", mapping.getSource());
                    item.put("target", mapping.effectiveTarget());
                    item.put("type", mapping.getType());
                    items.add(item);
                }
                node.getConfig().put("mappings", items);
                spec.getNodes().add(node);
                chain.add("fieldMapping");
            }

            List<ExpressionSpec> expressions = descriptor.getTransform().getExpressions();
            if (expressions != null && !expressions.isEmpty()) {
                PipelineNode node = new PipelineNode("expression", OperatorType.MAP);
                List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
                for (ExpressionSpec expression : expressions) {
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("name", expression.getName());
                    item.put("expression", expression.getExpression());
                    item.put("engine", expression.effectiveEngine());
                    item.put("type", expression.getType());
                    item.put("nullable", expression.getNullable());
                    items.add(item);
                }
                node.getConfig().put("columns", items);
                spec.getNodes().add(node);
                chain.add("expression");
            }

            List<ScriptSpec> scripts = descriptor.getTransform().getScripts();
            if (scripts != null && !scripts.isEmpty()) {
                PipelineNode node = new PipelineNode("script", OperatorType.SCRIPT);
                List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
                for (ScriptSpec script : scripts) {
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("name", script.getName());
                    item.put("engine", script.effectiveEngine());
                    item.put("script", script.getScript());
                    item.put("type", script.getType());
                    item.put("timeoutMs", Long.valueOf(script.effectiveTimeoutMs()));
                    item.put("maxOutputChars", Integer.valueOf(script.effectiveMaxOutputChars()));
                    items.add(item);
                }
                node.getConfig().put("scripts", items);
                spec.getNodes().add(node);
                chain.add("script");
            }

            List<TypeNormalizationSpec> rules = descriptor.getTransform().getTypeNormalizations();
            if (rules != null && !rules.isEmpty()) {
                PipelineNode node = new PipelineNode("typeNormalize", OperatorType.TYPE_NORMALIZE);
                List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
                for (TypeNormalizationSpec rule : rules) {
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("column", rule.getColumn());
                    item.put("type", rule.getType());
                    item.put("scale", rule.getScale());
                    item.put("nullable", rule.getNullable());
                    items.add(item);
                }
                node.getConfig().put("rules", items);
                spec.getNodes().add(node);
                chain.add("typeNormalize");
            }
        }

        TargetSpec target = descriptor.getTarget();
        if (target != null) {
            PipelineNode node = new PipelineNode("sink", OperatorType.JDBC_SINK);
            Map<String, Object> config = node.getConfig();
            config.put("dataSourceRef", target.getDataSourceRef());
            config.put("table", target.getTable());
            config.put("batchSize", Integer.valueOf(target.getBatchSize()));
            config.put("writeMode", target.getWriteMode());
            if (target.getColumns() != null && !target.getColumns().isEmpty()) {
                config.put("columns", new ArrayList<String>(target.getColumns()));
            }
            if (descriptor.getIdempotencyKey() != null && !descriptor.getIdempotencyKey().isEmpty()) {
                config.put("idempotencyKey", new ArrayList<String>(descriptor.getIdempotencyKey()));
            }
            spec.getNodes().add(node);
            chain.add("sink");
        }

        for (int i = 1; i < chain.size(); i++) {
            spec.getEdges().add(new PipelineEdge(chain.get(i - 1), chain.get(i)));
        }
        return spec;
    }
}
