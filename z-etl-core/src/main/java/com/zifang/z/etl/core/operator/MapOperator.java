package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.job.ExpressionSpec;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.transform.ExpressionTransformer;
import com.zifang.z.etl.core.transform.RowTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 映射算子：行级表达式产出新列。
 *
 * <p>复用旧引擎那套已验收的 {@link ExpressionTransformer}（EL / GROOVY 沙箱、类型归一、
 * 失败带步骤名）⇒ 新旧引擎的表达式口径完全一致，不出现「同一个表达式两种结果」。
 *
 * <pre>
 * - id: derive
 *   type: map
 *   config:
 *     columns:
 *       - {name: status_text, expression: "status == 1 ? 'PAID' : 'CLOSED'", type: STRING}
 * </pre>
 *
 * @author zifang
 */
public class MapOperator implements Operator {

    private final PipelineNode node;
    private final ScriptSandbox sandbox;
    private RowTransformer delegate;

    public MapOperator(PipelineNode node, ScriptSandbox sandbox) {
        this.node = node;
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext context) {
        List<ExpressionSpec> specs = new ArrayList<ExpressionSpec>();
        for (Map<String, Object> item : node.mapList("columns")) {
            ExpressionSpec spec = new ExpressionSpec();
            spec.setName(text(item.get("name")));
            spec.setExpression(text(item.get("expression")));
            spec.setEngine(text(item.get("engine")));
            spec.setType(text(item.get("type")));
            spec.setNullable(item.get("nullable") == null ? Boolean.TRUE : Boolean.valueOf(String.valueOf(item.get("nullable"))));
            Object timeout = item.get("timeoutMs");
            if (timeout != null) {
                spec.setTimeoutMs(Long.valueOf(asLong(timeout)));
            }
            if (spec.getName() == null || spec.getExpression() == null) {
                throw new IllegalArgumentException("map 节点 " + node.getId() + " 的表达式项缺少 name/expression");
            }
            specs.add(spec);
        }
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("map 节点 " + node.getId() + " 没有配置 columns");
        }
        delegate = new ExpressionTransformer(specs, sandbox);
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> transformed = delegate.transform(record.row());
        output.collect(record.withRow(transformed));
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

    static String text(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    static long asLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
