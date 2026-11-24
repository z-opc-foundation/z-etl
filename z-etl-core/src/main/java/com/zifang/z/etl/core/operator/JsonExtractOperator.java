package com.zifang.z.etl.core.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSON 提取算子：把声明列里的 JSON 文本当文档解析，按点路径取字段落列。
 *
 * <p>配置：{@code column}（含 JSON 文本的列）、{@code fields}（点路径 → 输出列名，
 * 如 {@code {user.name: user_name}}）、{@code onError}：{@code fail}（默认，解析失败炸行）｜
 * {@code 'null'}（写带引号的 'null'——裸 null 会被 yml 吃成缺值；解析失败输出列全 null）。
 *
 * <pre>
 * - id: parse
 *   type: json.extract
 *   config:
 *     column: payload
 *     fields: {user.name: user_name, order.total: total}
 * </pre>
 *
 * @author zifang
 */
public class JsonExtractOperator implements Operator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineNode node;
    private final String column;
    private final boolean nullOnError;

    /** 点路径 → 输出列名。 */
    private final Map<String, String> fields = new LinkedHashMap<String, String>();

    private OperatorContext context;

    public JsonExtractOperator(PipelineNode node) {
        this.node = node;
        this.column = node.string("column");
        this.nullOnError = "null".equalsIgnoreCase(node.string("onError", "fail"));
        if (column == null) {
            throw new IllegalArgumentException("json.extract 节点 " + node.getId() + " 缺少 column");
        }
    }

    @Override
    public void open(OperatorContext context) {
        this.context = context;
        Map<String, Object> declared = node.map("fields");
        if (declared.isEmpty()) {
            throw new IllegalArgumentException("json.extract 节点 " + node.getId() + " 缺少 fields（点路径 → 输出列名）");
        }
        for (Map.Entry<String, Object> entry : declared.entrySet()) {
            fields.put(entry.getKey(), MapOperator.text(entry.getValue()));
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        Object value = record.row().get(column);
        JsonNode root = null;
        if (value != null) {
            try {
                root = MAPPER.readTree(String.valueOf(value));
            } catch (Exception e) {
                context.count("parse-error");
                if (!nullOnError) {
                    throw new IllegalStateException("json.extract 节点 " + node.getId()
                            + " 列 " + column + " 不是合法 JSON: " + e.getMessage(), e);
                }
            }
        }
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            JsonNode hit = root == null ? null : navigate(root, entry.getKey());
            newRow.put(entry.getValue(), hit == null || hit.isNull() ? null : scalar(hit));
        }
        output.collect(record.withRow(newRow));
    }

    /** JsonNode → 行值标量（文本/数值/布尔原样，容器转文本）。 */
    private static Object scalar(JsonNode node) {
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isInt()) {
            return node.asInt();
        }
        if (node.isLong()) {
            return node.asLong();
        }
        if (node.isDouble() || node.isFloat() || node.isBigDecimal()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            return node.asText();
        }
        return node.toString();
    }

    /** 点路径导航；路上断了返回 null。 */
    private static JsonNode navigate(JsonNode root, String path) {
        JsonNode current = root;
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            if (current == null) {
                return null;
            }
            current = current.get(parts[i]);
        }
        return current;
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
