package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 列改名算子：按 {@code mapping}（旧列名 → 新列名）整行改名，其余列不动。
 *
 * <pre>
 * - id: rename
 *   type: rename
 *   config:
 *     mapping: {user_id: id, uname: name}
 * </pre>
 *
 * @author zifang
 */
public class RenameOperator implements Operator {

    private final PipelineNode node;

    private Map<String, String> mapping;

    public RenameOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) {
        Map<String, Object> declared = node.map("mapping");
        if (declared.isEmpty()) {
            throw new IllegalArgumentException("rename 节点 " + node.getId() + " 缺少 mapping（旧列名 → 新列名）");
        }
        mapping = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : declared.entrySet()) {
            String target = MapOperator.text(entry.getValue());
            if (target == null || target.trim().isEmpty()) {
                throw new IllegalArgumentException("rename 节点 " + node.getId()
                        + " 的映射项 " + entry.getKey() + " 缺少新列名");
            }
            mapping.put(entry.getKey(), target.trim());
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> newRow = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : record.row().entrySet()) {
            String renamed = mapping.get(entry.getKey());
            newRow.put(renamed == null ? entry.getKey() : renamed, entry.getValue());
        }
        output.collect(record.withRow(newRow));
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
