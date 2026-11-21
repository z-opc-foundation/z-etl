package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 列转行算子（melt）：声明的列逐列竖排成「名字列 + 值列」，其余列原样保留。
 *
 * <pre>
 * - id: melt
 *   type: unpivot
 *   config:
 *     columns: [jan, feb, mar]  # 要竖排的列（必填，至少一列）
 *     nameColumn: month         # 可选：名字列（缺省 column）
 *     valueColumn: amount       # 可选：值列（缺省 value）
 *     keepNull: false           # 可选：值为 null 是否也出一行（缺省丢并计数）
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li>每条输入按声明列序各出一行：行 = 原行去掉熔炼列 + 名字列（列名）+ 值列（原值）；</li>
 *   <li>未声明的列原样保留（melt 语义）；</li>
 *   <li>值为 null：缺省丢掉并计数（{@code null-skipped}），{@code keepNull: true} 也出一行；</li>
 *   <li>无状态：行数可膨胀，但不碰键控状态；与 {@code explode}（纵向炸开）互补。</li>
 * </ul>
 *
 * @author zifang
 */
public class UnpivotOperator implements Operator {

    private final PipelineNode node;

    private List<String> columns;
    private String nameColumn;
    private String valueColumn;
    private boolean keepNull;

    private OperatorContext context;

    public UnpivotOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        columns = node.strings("columns");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("unpivot 节点 " + node.getId() + " 必须声明非空的 config.columns");
        }
        nameColumn = MapOperator.text(node.string("nameColumn", "column"));
        valueColumn = MapOperator.text(node.string("valueColumn", "value"));
        keepNull = Boolean.parseBoolean(String.valueOf(node.string("keepNull", "false")));
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> source = record.row();
        for (String column : columns) {
            Object value = source.get(column);
            if (value == null && !keepNull) {
                context.count("null-skipped");
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                if (!columns.contains(entry.getKey())) {
                    row.put(entry.getKey(), entry.getValue());
                }
            }
            row.put(nameColumn, column);
            row.put(valueColumn, value);
            output.collect(record.withRow(row));
        }
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态，不碰水位
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
