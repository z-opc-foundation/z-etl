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
 * 正则提取算子：对声明列跑正则，捕获组落列。
 *
 * <p>配置：{@code column}（输入列）、{@code pattern}（正则）、
 * {@code outputs}（组名或组序号 → 输出列名，如 {@code {1: area, 2: number}}）、
 * {@code onNoMatch}：{@code null}（默认，不匹配输出列全 null）｜{@code drop}（不匹配丢行）。
 *
 * <pre>
 * - id: phone
 *   type: regex.extract
 *   config:
 *     column: raw
 *     pattern: "(\\d{3,4})-(\\d{7,8})"
 *     outputs: {1: area, 2: number}
 * </pre>
 *
 * @author zifang
 */
public class RegexExtractOperator implements Operator {

    private final PipelineNode node;
    private final String column;
    private final String pattern;
    private final boolean dropOnNoMatch;

    private Pattern compiled;
    /** 组键（组名优先，没有组名按序号）→ 输出列名。 */
    private final Map<String, String> outputs = new LinkedHashMap<String, String>();

    private OperatorContext context;

    public RegexExtractOperator(PipelineNode node) {
        this.node = node;
        this.column = node.string("column");
        this.pattern = node.string("pattern");
        if (column == null) {
            throw new IllegalArgumentException("regex.extract 节点 " + node.getId() + " 缺少 column");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("regex.extract 节点 " + node.getId() + " 缺少 pattern");
        }
        this.dropOnNoMatch = "drop".equalsIgnoreCase(node.string("onNoMatch", "null"));
    }

    @Override
    public void open(OperatorContext context) {
        this.context = context;
        this.compiled = Pattern.compile(pattern);
        Map<String, Object> declared = node.map("outputs");
        if (declared.isEmpty()) {
            throw new IllegalArgumentException("regex.extract 节点 " + node.getId() + " 缺少 outputs（组 → 输出列名）");
        }
        for (Map.Entry<String, Object> entry : declared.entrySet()) {
            outputs.put(entry.getKey(), MapOperator.text(entry.getValue()));
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        Object value = record.row().get(column);
        Matcher matcher = compiled.matcher(value == null ? "" : String.valueOf(value));
        if (!matcher.find()) {
            context.count("no-match");
            if (dropOnNoMatch) {
                return;
            }
        }
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        for (Map.Entry<String, String> entry : outputs.entrySet()) {
            String groupValue = groupValue(matcher, entry.getKey());
            newRow.put(entry.getValue(), groupValue);
        }
        output.collect(record.withRow(newRow));
    }

    /** 组名优先取命名组，取不到按序号取。 */
    private static String groupValue(Matcher matcher, String key) {
        try {
            int index = Integer.parseInt(key);
            return matcher.group(index);
        } catch (NumberFormatException notAnIndex) {
            try {
                return matcher.group(key);
            } catch (IllegalArgumentException noSuchGroup) {
                return null;
            }
        } catch (IndexOutOfBoundsException badIndex) {
            return null;
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
