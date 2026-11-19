package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.job.FieldMapping;
import com.zifang.z.etl.core.job.TransformSpec;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.FieldMappingTransformer;
import com.zifang.z.etl.core.transform.RowTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 字段映射算子：源列 → 目标列 + 落库类型。
 *
 * <p>复用旧引擎已验收的 {@link FieldMappingTransformer}（取列 → 归一 → 写列一次做完），
 * 新旧引擎口径一致。
 *
 * @author zifang
 */
public class FieldMappingOperator implements Operator {

    private final PipelineNode node;
    private RowTransformer delegate;

    public FieldMappingOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) {
        TransformSpec spec = new TransformSpec();
        List<FieldMapping> mappings = new ArrayList<FieldMapping>();
        for (Map<String, Object> item : node.mapList("mappings")) {
            FieldMapping mapping = new FieldMapping();
            mapping.setSource(MapOperator.text(item.get("source")));
            mapping.setTarget(MapOperator.text(item.get("target")));
            mapping.setType(MapOperator.text(item.get("type")));
            Object nullable = item.get("nullable");
            if (nullable != null) {
                mapping.setNullable(Boolean.valueOf(String.valueOf(nullable)));
            }
            if (mapping.getSource() == null) {
                throw new IllegalArgumentException("field.mapping 节点 " + node.getId() + " 的映射项缺少 source");
            }
            mappings.add(mapping);
        }
        if (mappings.isEmpty()) {
            throw new IllegalArgumentException("field.mapping 节点 " + node.getId() + " 没有配置 mappings");
        }
        spec.setFieldMappings(mappings);
        delegate = new FieldMappingTransformer(spec);
    }

    @Override
    public void processElement(Record record, Output output) {
        output.collect(record.withRow(delegate.transform(record.row())));
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
