package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 分列算子：把声明列按分隔符切开，落进声明的输出列名。
 *
 * <p>配置：{@code column}、{@code separator}、{@code names}（输出列名列表）、
 * {@code keepOriginal}（默认 false，原列被替换掉）。切出的段数不足时多余列给 null。
 *
 * <pre>
 * - id: split
 *   type: split
 *   config:
 *     column: full_name
 *     separator: " "
 *     names: [first_name, last_name]
 * </pre>
 *
 * @author zifang
 */
public class SplitOperator implements Operator {

    private final PipelineNode node;
    private final String column;
    private final boolean keepOriginal;

    private List<String> names;

    public SplitOperator(PipelineNode node) {
        this.node = node;
        this.column = node.string("column");
        this.keepOriginal = node.bool("keepOriginal", false);
        if (column == null) {
            throw new IllegalArgumentException("split 节点 " + node.getId() + " 缺少 column");
        }
        if (separator() == null || separator().isEmpty()) {
            throw new IllegalArgumentException("split 节点 " + node.getId() + " 缺少 separator");
        }
    }

    @Override
    public void open(OperatorContext context) {
        names = node.strings("names");
        if (names == null || names.isEmpty()) {
            throw new IllegalArgumentException("split 节点 " + node.getId() + " 缺少 names（输出列名列表）");
        }
    }

    /** 分隔符取原值（PipelineNode.string 会把纯空白当缺省吞掉，单空格分隔符就没了）。 */
    private String separator() {
        Object raw = node.getConfig() == null ? null : node.getConfig().get("separator");
        return raw == null ? null : String.valueOf(raw);
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        Object value = newRow.get(column);
        String[] parts = value == null ? new String[0] : String.valueOf(value).split(java.util.regex.Pattern.quote(separator()));
        for (int i = 0; i < names.size(); i++) {
            newRow.put(names.get(i), i < parts.length ? parts[i] : null);
        }
        if (!keepOriginal) {
            newRow.remove(column);
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
