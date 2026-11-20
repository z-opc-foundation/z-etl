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
 * 炸行算子：一行拆多行（Flink SQL 的 {@code UNNEST} 口径）。
 *
 * <p>两种拆法：
 * <ul>
 *   <li><b>数组列炸开</b>：列的值是数组（{@code List}）⇒ 每个元素出一行；</li>
 *   <li><b>字符串切分</b>：声明 {@code delimiter} ⇒ 按分隔符切开（null/空串得到 0 段或 1 段）。 </li>
 * </ul>
 *
 * <pre>
 * - id: split
 *   type: explode
 *   config:
 *     column: tags          # 要炸的列（必填）
 *     target: tag           # 可选：炸出的元素放哪个列（缺省覆盖原列）
 *     indexColumn: tag_idx  # 可选：顺带写出元素下标（0 起）
 *     delimiter: ","        # 可选：声明后按字符串切分（数组列不受影响）
 *     keepEmpty: false      # 可选：空数组 / null / 空串是否保留原行（缺省丢并计数）
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li>数组元素与切分段逐个出一行，行 = 原行 + 目标列（元素值）+ 可选下标列；</li>
 *   <li>标量值（既不是数组也没有 delimiter）按单元素处理（原样出一行）；</li>
 *   <li>空数组 / null / 切出空：{@code keepEmpty: true} 保留原行（目标列置 null），
 *       否则丢掉并计数（{@code dropped-empty}）；</li>
 *   <li>无状态：行数可膨胀，但不碰键控状态。</li>
 * </ul>
 *
 * @author zifang
 */
public class ExplodeOperator implements Operator {

    private final PipelineNode node;

    private String column;
    private String target;
    private String indexColumn;
    private String delimiter;
    private boolean keepEmpty;

    private OperatorContext context;

    public ExplodeOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        column = MapOperator.text(node.string("column"));
        if (column == null) {
            throw new IllegalArgumentException("explode 节点 " + node.getId() + " 必须声明 config.column");
        }
        String declaredTarget = MapOperator.text(node.string("target"));
        target = declaredTarget == null ? column : declaredTarget;
        indexColumn = MapOperator.text(node.string("indexColumn"));
        delimiter = node.string("delimiter");
        keepEmpty = Boolean.parseBoolean(String.valueOf(node.string("keepEmpty", "false")));
    }

    @Override
    public void processElement(Record record, Output output) {
        Object value = record.row().get(column);
        List<Object> elements = elementsOf(value);
        if (elements.isEmpty()) {
            if (keepEmpty) {
                Map<String, Object> row = new LinkedHashMap<String, Object>(record.row());
                row.put(target, null);
                output.collect(record.withRow(row));
                context.count("kept-empty");
            } else {
                context.count("dropped-empty");
            }
            return;
        }
        for (int i = 0; i < elements.size(); i++) {
            Map<String, Object> row = new LinkedHashMap<String, Object>(record.row());
            row.put(target, elements.get(i));
            if (indexColumn != null) {
                row.put(indexColumn, i);
            }
            output.collect(record.withRow(row));
        }
        context.count("exploded", elements.size() - 1L);
    }

    /** 列值 → 元素列表：数组直接用；声明了 delimiter 按字符串切；标量当单元素。 */
    private List<Object> elementsOf(Object value) {
        List<Object> elements = new ArrayList<Object>();
        if (value instanceof List) {
            elements.addAll((List<?>) value);
            return elements;
        }
        if (delimiter != null && value != null) {
            String text = String.valueOf(value);
            if (!text.isEmpty()) {
                for (String piece : text.split(delimiter)) {
                    elements.add(piece);
                }
            }
            return elements;
        }
        if (value != null) {
            elements.add(value);
        }
        return elements;
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
