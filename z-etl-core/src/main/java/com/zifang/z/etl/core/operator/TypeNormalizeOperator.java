package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.job.TypeNormalizationSpec;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.RowTransformer;
import com.zifang.z.etl.core.transform.TypeNormalizationTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 类型归一算子：类型 + 定点小数位 + 空值策略。
 *
 * <p>复用旧引擎已验收的 {@link TypeNormalizationTransformer}。
 * 金额场景必用 {@code BIGDECIMAL} + {@code scale}：表达式是浮点语义，
 * 落库前不收口就会出现 {@code 112.99999999999999}。
 *
 * @author zifang
 */
public class TypeNormalizeOperator implements Operator {

    private final PipelineNode node;
    private RowTransformer delegate;

    public TypeNormalizeOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) {
        List<TypeNormalizationSpec> specs = new ArrayList<TypeNormalizationSpec>();
        for (Map<String, Object> item : node.mapList("rules")) {
            TypeNormalizationSpec spec = new TypeNormalizationSpec();
            spec.setColumn(MapOperator.text(item.get("column")));
            spec.setType(MapOperator.text(item.get("type")));
            Object nullable = item.get("nullable");
            if (nullable != null) {
                spec.setNullable(Boolean.valueOf(String.valueOf(nullable)));
            }
            Object scale = item.get("scale");
            if (scale != null) {
                spec.setScale(Integer.valueOf((int) MapOperator.asLong(scale)));
            }
            if (spec.getColumn() == null) {
                throw new IllegalArgumentException("type.normalize 节点 " + node.getId() + " 的规则项缺少 column");
            }
            specs.add(spec);
        }
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("type.normalize 节点 " + node.getId() + " 没有配置 rules");
        }
        delegate = new TypeNormalizationTransformer(specs);
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
