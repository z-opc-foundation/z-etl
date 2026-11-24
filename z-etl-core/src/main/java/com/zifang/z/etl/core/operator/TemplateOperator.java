package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 模板合成算子：用 ${列名} 占位符把整行拼成一个字符串落进声明列。
 *
 * <p>配置：{@code template}（含 {@code ${col}} 占位符）、{@code name}（输出列名）。
 * 占位列不存在或值为 null ⇒ 空串（不炸行，合成场景 null 很常见）。
 *
 * <pre>
 * - id: greet
 *   type: template
 *   config:
 *     template: "${last_name}${first_name}（${city}）"
 *     name: display
 * </pre>
 *
 * @author zifang
 */
public class TemplateOperator implements Operator {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    private final PipelineNode node;
    private final String template;
    private final String name;

    public TemplateOperator(PipelineNode node) {
        this.node = node;
        this.template = node.string("template");
        this.name = node.string("name");
        if (template == null) {
            throw new IllegalArgumentException("template 节点 " + node.getId() + " 缺少 template");
        }
        if (name == null) {
            throw new IllegalArgumentException("template 节点 " + node.getId() + " 缺少 name");
        }
    }

    @Override
    public void open(OperatorContext context) {
        // 无资源
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> row = record.row();
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder rendered = new StringBuilder();
        int cursor = 0;
        while (matcher.find()) {
            rendered.append(template, cursor, matcher.start());
            Object value = row.get(matcher.group(1).trim());
            rendered.append(value == null ? "" : String.valueOf(value));
            cursor = matcher.end();
        }
        rendered.append(template.substring(cursor));
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(row);
        newRow.put(name, rendered.toString());
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
