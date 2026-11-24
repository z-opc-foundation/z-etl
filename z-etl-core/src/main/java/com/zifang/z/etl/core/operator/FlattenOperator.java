package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 拍平算子：把行里的嵌套 Map 拍成点路径列（{@code a.b.c}），数组不动。
 *
 * <p>配置：{@code separator}（默认 {@code .}）。拍平对 JSON 源 / 脚本产出的嵌套结构最有用——
 * 落库前一层压平。
 *
 * <pre>
 * - id: flat
 *   type: flatten
 *   config:
 *     separator: "_"
 * </pre>
 *
 * @author zifang
 */
public class FlattenOperator implements Operator {

    private final PipelineNode node;
    private final String separator;

    public FlattenOperator(PipelineNode node) {
        this.node = node;
        this.separator = node.string("separator", ".");
    }

    @Override
    public void open(OperatorContext context) {
        // 无资源
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(Record record, Output output) {
        Map<String, Object> flat = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : record.row().entrySet()) {
            if (entry.getValue() instanceof Map) {
                flatten(entry.getKey(), (Map<String, Object>) entry.getValue(), separator, flat);
            } else {
                flat.put(entry.getKey(), entry.getValue());
            }
        }
        output.collect(record.withRow(flat));
    }

    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> nested, String separator,
                                Map<String, Object> flat) {
        for (Map.Entry<String, Object> entry : nested.entrySet()) {
            String path = prefix.isEmpty() ? entry.getKey() : prefix + separator + entry.getKey();
            if (entry.getValue() instanceof Map) {
                flatten(path, (Map<String, Object>) entry.getValue(), separator, flat);
            } else {
                flat.put(path, entry.getValue());
            }
        }
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
