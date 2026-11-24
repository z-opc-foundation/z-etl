package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 时间格式化算子：时间戳 / ISO 字符串 / 自定义格式 → 目标格式文本。
 *
 * <p>配置：{@code column}、{@code fromPattern}（可选；声明后按它解析输入字符串，
 * 不声明则数字按 epoch 毫秒、字符串按 ISO-8601）、{@code toPattern}（默认
 * {@code yyyy-MM-dd HH:mm:ss}）、{@code name}（输出列名，默认原地覆盖）。
 * null 保持 null；解析失败炸行（时间解析错就该炸，静默 null 更危险）。
 *
 * <pre>
 * - id: fmt
 *   type: date.format
 *   config:
 *     column: created_at          # epoch 毫秒数字列
 *     toPattern: "yyyy/MM/dd"
 * </pre>
 *
 * @author zifang
 */
public class DateFormatOperator implements Operator {

    private final PipelineNode node;
    private final String column;
    private final String fromPattern;
    private final String toPattern;
    private final String name;

    private DateTimeFormatter fromFormatter;
    private DateTimeFormatter toFormatter;

    public DateFormatOperator(PipelineNode node) {
        this.node = node;
        this.column = node.string("column");
        this.fromPattern = node.string("fromPattern");
        this.toPattern = node.string("toPattern", "yyyy-MM-dd HH:mm:ss");
        this.name = node.string("name", column);
        if (column == null) {
            throw new IllegalArgumentException("date.format 节点 " + node.getId() + " 缺少 column");
        }
    }

    @Override
    public void open(OperatorContext context) {
        fromFormatter = fromPattern == null ? null : DateTimeFormatter.ofPattern(fromPattern);
        toFormatter = DateTimeFormatter.ofPattern(toPattern);
    }

    @Override
    public void processElement(Record record, Output output) {
        Object value = record.row().get(column);
        if (value == null) {
            output.collect(record);
            return;
        }
        LocalDateTime time;
        try {
            if (value instanceof java.util.Date) {
                // 上游源（inline / JDBC）常把时间串先转成 Timestamp 再递过来
                time = LocalDateTime.ofInstant(((java.util.Date) value).toInstant(), ZoneId.systemDefault());
            } else if (value instanceof Number) {
                time = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()),
                        ZoneId.systemDefault());
            } else if (fromFormatter != null) {
                time = LocalDateTime.parse(String.valueOf(value), fromFormatter);
            } else {
                time = LocalDateTime.parse(String.valueOf(value));
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("date.format 节点 " + node.getId() + " 列 " + column
                    + " 解析不了时间值 " + value + (fromPattern == null ? "（ISO/epoch 口径）"
                    : "（fromPattern=" + fromPattern + "）") + ": " + e.getMessage(), e);
        }
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        newRow.put(name, time.format(toFormatter));
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
