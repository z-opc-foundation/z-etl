package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.OutputTag;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.util.expr.el.ElEvaluator;

import java.util.Map;

/**
 * 过滤算子：表达式为真才放行。
 *
 * <p>两个执行器（与表达式算子同口径）：
 * <ul>
 *   <li>{@code EL}（默认）：z-util-expr 的自研 EL，无任意代码执行；</li>
 *   <li>{@code GROOVY}：z-script 沙箱（编译期 AST 黑名单 + 墙钟超时）。</li>
 * </ul>
 *
 * <p>「被过滤掉的行」不静默丢：声明 {@code rejectedTo: <节点id>} 时走**旁路输出**
 * （标签 id = 目标节点 id），让「脏数据」有去处 —— 这是流处理里最常被忽略、却最值钱的一根管子。
 *
 * @author zifang
 */
public class FilterOperator implements Operator {

    private final PipelineNode node;
    private final ScriptSandbox sandbox;

    private String expression;
    private String engine;
    private long timeoutMs;
    private boolean negated;
    private OutputTag rejectedTag;
    private ElEvaluator elEvaluator;

    public FilterOperator(PipelineNode node, ScriptSandbox sandbox) {
        this.node = node;
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext context) {
        expression = node.string("expression");
        if (expression == null) {
            expression = node.string("predicate");
        }
        if (expression == null) {
            throw new IllegalArgumentException("filter 节点 " + node.getId() + " 缺少 expression");
        }
        engine = node.string("engine", "EL").toUpperCase();
        timeoutMs = node.longValue("timeoutMs", 2000L);
        negated = node.bool("negated", false);
        String rejectedTo = node.string("rejectedTo");
        if (rejectedTo != null) {
            rejectedTag = new OutputTag(rejectedTo);
        }
        if ("EL".equals(engine)) {
            elEvaluator = new ElEvaluator();
        } else if ("GROOVY".equals(engine) && sandbox == null) {
            throw new IllegalArgumentException("filter 节点 " + node.getId() + " 指定了 GROOVY 引擎，但没有沙箱执行器");
        }
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        Object value = evaluate(record.row());
        boolean pass = toBoolean(value);
        if (negated) {
            pass = !pass;
        }
        if (pass) {
            output.collect(record);
        } else if (rejectedTag != null) {
            output.collect(rejectedTag, record);
        }
    }

    /** 求值：EL 或沙箱 GROOVY；失败抛异常（不静默当 false）。 */
    private Object evaluate(Map<String, Object> row) {
        try {
            if ("EL".equals(engine)) {
                return elEvaluator.eval(expression, row);
            }
            if ("GROOVY".equals(engine)) {
                return sandbox.evalGroovy(expression, row, timeoutMs);
            }
            throw new IllegalArgumentException("filter 节点 " + node.getId() + " 的引擎不支持: " + engine
                    + "（LUA/JS 无沙箱 ⇒ 硬拒绝）");
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("filter 节点 " + node.getId() + " 求值失败: " + e.getMessage(), e);
        }
    }

    /** 结果归布尔：Boolean / 数字（非零）/ 字符串（true/1/yes）。包内可见（route 算子复用）。 */
    static boolean toBoolean(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0d;
        }
        String text = String.valueOf(value).trim();
        return "true".equalsIgnoreCase(text) || "1".equals(text) || "yes".equalsIgnoreCase(text);
    }

    @Override
    public void processWatermark(long watermark) {
        // 过滤不涉状态
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 过滤不涉定时器
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
