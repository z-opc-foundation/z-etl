package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.spi.AiExecutor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AI 映射算子：每行渲染提示词调大模型，输出文本落声明列（future-etl 的 AI 步骤口径）。
 *
 * <p>配置：
 * <pre>
 * - id: enrich
 *   type: ai.map
 *   config:
 *     prompt: "把这段投诉归类：${text}"   # 必填，${列} 占位符渲染
 *     outputField: category               # 可选，缺省 ai
 *     system: "你是工单分类助手"           # 可选，系统提示词
 *     agentCode: ticket-clf               # 可选，透传给执行器路由
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li>执行器经 {@link AiExecutor} SPI 注入；未装配时 {@code open} 即报错说明；</li>
 *   <li>占位符渲染与 {@code template} 算子同式（${列名}，null 渲染成空串）；</li>
 *   <li>失败走管道失败策略（fail-fast 中断 / continue 计 error 行），算子自身不吞异常；</li>
 *   <li>每行同步外呼 —— 高吞吐低延迟场景用 {@code async.map}（HTTP）或分片并行。</li>
 * </ul>
 *
 * @author zifang
 */
public class AiMapOperator implements Operator {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    private final PipelineNode node;
    private final AiExecutor executor;

    private String prompt;
    private String system;
    private String agentCode;
    private String outputField;

    private OperatorContext context;

    public AiMapOperator(PipelineNode node, AiExecutor executor) {
        this.node = node;
        this.executor = executor;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        prompt = node.string("prompt");
        if (prompt == null || prompt.trim().isEmpty()) {
            throw new IllegalArgumentException("ai.map 节点 " + node.getId() + " 必须声明 config.prompt");
        }
        system = node.string("system");
        agentCode = node.string("agentCode");
        outputField = MapOperator.text(node.string("outputField"));
        if (outputField == null) {
            outputField = "ai";
        }
        if (executor == null) {
            throw new IllegalStateException("ai.map 节点 " + node.getId()
                    + " 需要 AI 执行器：宿主未装配 AiExecutor SPI（core 不依赖任何模型 SDK，"
                    + "请在启动装配处注入）");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> row = record.row();
        String rendered = render(prompt, row);
        String answer;
        try {
            answer = executor.complete(agentCode, system, rendered, row);
        } catch (Exception e) {
            context.count("ai-errors");
            throw new IllegalStateException("ai.map 节点 " + node.getId() + " 补列失败: " + e.getMessage(), e);
        }
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(row);
        newRow.put(outputField, answer == null ? "" : answer);
        output.collect(record.withRow(newRow));
        context.count("ai-completed");
    }

    /** ${列名} 占位符渲染（与 template 算子同式；缺列渲染成空串）。 */
    private String render(String template, Map<String, Object> row) {
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder rendered = new StringBuilder();
        int cursor = 0;
        while (matcher.find()) {
            rendered.append(template, cursor, matcher.start());
            Object value = row.get(matcher.group(1).trim());
            rendered.append(value == null ? "" : String.valueOf(value));
            cursor = matcher.end();
        }
        return rendered.append(template.substring(cursor)).toString();
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
