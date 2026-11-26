package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.OutputTag;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.util.expr.el.ElEvaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 路由算子：**n 路条件分流**（Kafka Streams 的 {@code branch()} / Flink 侧输出 split 的口径）。
 *
 * <p>一条声明把「按条件把行发给不同下游」写清楚，不用再串一排 filter：
 * <ul>
 *   <li>{@code rules} 按**声明顺序**逐条判定，**命中即走**（一条行只进一个分支，
 *       与 nginx location / 路由表同口径）；行的去处 = 旁路输出标签（标签 id = 目标节点 id）；</li>
 *   <li>{@code fallback}：全不命中时的去处（可省 —— 缺省丢行并计数 {@code dropped}）；</li>
 *   <li>节点本身**没有主输出**：分流全走旁路标签，声明出边是配置错误（校验器直接报错）。</li>
 * </ul>
 *
 * <pre>
 * - id: route1
 *   type: route
 *   config:
 *     rules:
 *       - {to: vip,    expression: "amount > 10000"}
 *       - {to: normal, expression: "amount > 0"}
 *     fallback: dead
 * </pre>
 *
 * <p>表达式引擎与 filter 同口径：{@code EL}（默认）/ {@code GROOVY}（沙箱），
 * 可按条声明 {@code engine} / {@code timeoutMs}。
 *
 * @author zifang
 */
public class RouteOperator implements Operator {

    private final PipelineNode node;
    private final ScriptSandbox sandbox;

    private final List<Rule> rules = new ArrayList<Rule>();
    private OutputTag fallbackTag;
    private ElEvaluator elEvaluator;
    private OperatorContext context;

    public RouteOperator(PipelineNode node, ScriptSandbox sandbox) {
        this.node = node;
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        elEvaluator = new ElEvaluator();
        List<Map<String, Object>> declared = node.mapList("rules");
        if (declared.isEmpty()) {
            throw new IllegalArgumentException("route 节点 " + node.getId() + " 必须声明 rules（至少一条 to/expression）");
        }
        for (Map<String, Object> item : declared) {
            String to = text(item.get("to"));
            String expression = text(item.get("expression"));
            if (expression == null) {
                expression = text(item.get("predicate"));
            }
            if (to == null) {
                throw new IllegalArgumentException("route 节点 " + node.getId() + " 的 rules 里有一条缺少 to");
            }
            if (expression == null) {
                throw new IllegalArgumentException("route 节点 " + node.getId() + " 的 rules[" + to + "] 缺少 expression");
            }
            rules.add(new Rule(to, expression,
                    text(item.get("engine")) == null ? "EL" : text(item.get("engine")).toUpperCase(),
                    item.get("timeoutMs") == null ? 2000L : Long.parseLong(String.valueOf(item.get("timeoutMs")))));
        }
        String fallback = node.string("fallback");
        if (fallback != null) {
            fallbackTag = new OutputTag(fallback);
        }
        for (Rule rule : rules) {
            if ("GROOVY".equals(rule.engine) && sandbox == null) {
                throw new IllegalArgumentException("route 节点 " + node.getId() + " 的 rules[" + rule.to
                        + "] 指定了 GROOVY 引擎，但没有沙箱执行器");
            }
            if (!"EL".equals(rule.engine) && !"GROOVY".equals(rule.engine)) {
                throw new IllegalArgumentException("route 节点 " + node.getId() + " 的 rules[" + rule.to
                        + "] 引擎不支持: " + rule.engine + "（LUA/JS 无沙箱 ⇒ 硬拒绝）");
            }
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        for (Rule rule : rules) {
            if (FilterOperator.toBoolean(evaluate(rule, record.row()))) {
                output.collect(rule.tag(), record);
                context.count("routed");
                context.count("to:" + rule.to);
                return;
            }
        }
        if (fallbackTag != null) {
            output.collect(fallbackTag, record);
            context.count("routed");
            context.count("fallback");
        } else {
            context.count("dropped");
        }
    }

    /** 单条规则求值：EL 或沙箱 GROOVY；失败抛异常（不静默当 false）。 */
    private Object evaluate(Rule rule, Map<String, Object> row) {
        try {
            if ("EL".equals(rule.engine)) {
                return elEvaluator.eval(rule.expression, row);
            }
            return sandbox.evalGroovy(rule.expression, row, rule.timeoutMs);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("route 节点 " + node.getId() + " 的 rules[" + rule.to
                    + "] 求值失败: " + e.getMessage(), e);
        }
    }

    /** 取文本（空串 → null）。 */
    private static String text(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
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

    /** 一条分流规则（解析后形态）。 */
    private static final class Rule {

        private final String to;
        private final String expression;
        private final String engine;
        private final long timeoutMs;
        private OutputTag tag;

        Rule(String to, String expression, String engine, long timeoutMs) {
            this.to = to;
            this.expression = expression;
            this.engine = engine;
            this.timeoutMs = timeoutMs;
        }

        OutputTag tag() {
            if (tag == null) {
                tag = new OutputTag(to);
            }
            return tag;
        }
    }
}
