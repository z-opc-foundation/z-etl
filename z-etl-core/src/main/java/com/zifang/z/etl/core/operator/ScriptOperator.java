package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.transform.TypeNormalizer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 脚本算子：**自由清洗**的主力算子（z-util / z-script 那套沙箱脚本）。
 *
 * <p>两种产出模式：
 * <ul>
 *   <li>{@code column}（默认）：脚本算一个值 → 落进 {@code name} 指定的列；</li>
 *   <li>{@code row}：脚本返回一张 Map → **整行替换**（做整行重塑、拼装新结构最顺手）。</li>
 * </ul>
 *
 * <p>安全闸三道（与旧引擎同口径）：沙箱（编译期 AST 黑名单）、墙钟超时、单步产出上限。
 * {@code LUA} 硬拒绝（z-script 的 Lua 无沙箱无超时，实测能逃逸）。
 *
 * <pre>
 * - id: clean
 *   type: script
 *   config:
 *     name: pay_channel
 *     engine: GROOVY
 *     mode: column
 *     script: "def raw = payment_method\n(raw == null ? '' : raw.toString().trim().toUpperCase())\n"
 * </pre>
 *
 * @author zifang
 */
public class ScriptOperator implements Operator {

    /** 产出模式：落一列。 */
    public static final String MODE_COLUMN = "column";
    /** 产出模式：整行替换。 */
    public static final String MODE_ROW = "row";

    private final PipelineNode node;
    private final ScriptSandbox sandbox;

    private String name;
    private String engine;
    private String mode;
    private String script;
    private String type;
    private boolean nullable = true;
    private long timeoutMs;
    private int maxOutputChars;

    public ScriptOperator(PipelineNode node, ScriptSandbox sandbox) {
        this.node = node;
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext context) {
        script = node.string("script");
        if (script == null) {
            throw new IllegalArgumentException("script 节点 " + node.getId() + " 缺少 script");
        }
        engine = node.string("engine", "GROOVY").toUpperCase();
        mode = node.string("mode", MODE_COLUMN).toLowerCase();
        name = node.string("name");
        type = node.string("type");
        nullable = node.bool("nullable", true);
        timeoutMs = node.longValue("timeoutMs", 2000L);
        maxOutputChars = node.integer("maxOutputChars", 65536);
        if (MODE_ROW.equals(mode) && name != null) {
            throw new IllegalArgumentException("script 节点 " + node.getId() + " 的 mode=row 不需要 name（整行替换）");
        }
        if (MODE_COLUMN.equals(mode) && name == null) {
            throw new IllegalArgumentException("script 节点 " + node.getId() + " 的 mode=column 必须声明 name");
        }
        if (!"GROOVY".equals(engine) && !"EL".equals(engine)) {
            throw new IllegalArgumentException("script 节点 " + node.getId() + " 的引擎不支持: " + engine
                    + "（LUA 执行器无沙箱无超时、实测可逃逸 ⇒ 硬拒绝）");
        }
        if (sandbox == null) {
            throw new IllegalArgumentException("script 节点 " + node.getId() + " 需要沙箱执行器，但未配置");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        Object value = evaluate(record.row());
        Map<String, Object> row = record.row();
        if (MODE_ROW.equals(mode)) {
            if (value == null) {
                throw new IllegalStateException("script 节点 " + node.getId() + " 的 mode=row 脚本返回了 null");
            }
            if (!(value instanceof Map)) {
                throw new IllegalStateException("script 节点 " + node.getId() + " 的 mode=row 脚本必须返回 Map，实际返回 "
                        + value.getClass().getName());
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> newRow = new LinkedHashMap<String, Object>((Map<String, Object>) value);
            output.collect(record.withRow(newRow));
            return;
        }
        Object coerced = TypeNormalizer.coerce(value, type, Boolean.valueOf(nullable));
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(row);
        newRow.put(name, coerced);
        output.collect(record.withRow(newRow));
    }

    /** 执行脚本；超时 / 超限 / 失败一律抛（不静默吞）。 */
    private Object evaluate(Map<String, Object> row) {
        Object value;
        try {
            if ("GROOVY".equals(engine)) {
                value = sandbox.evalGroovy(script, row, timeoutMs);
            } else {
                value = sandbox.evalEl(script, row, timeoutMs);
            }
        } catch (com.zifang.z.script.engine.sandbox.ScriptTimeoutException e) {
            throw new IllegalStateException("script 节点 " + node.getId() + " 执行超时(" + timeoutMs
                    + "ms)，已强制中断", e);
        } catch (RuntimeException e) {
            throw new IllegalStateException("script 节点 " + node.getId() + " 执行失败: " + e.getMessage(), e);
        }
        int size = measure(value);
        if (size > maxOutputChars) {
            throw new IllegalStateException("script 节点 " + node.getId() + " 产出 " + size
                    + " 字符，超过单步上限 " + maxOutputChars + " 字符（内存越限兜底）");
        }
        return value;
    }

    /** 产出大小（按字符串长度；非字符串按其文本长度算）。 */
    private static int measure(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof CharSequence) {
            return ((CharSequence) value).length();
        }
        return String.valueOf(value).length();
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

    /** 多脚本模式下的脚本清单（供批量声明用）。 */
    public static List<Map<String, Object>> scriptConfigs(PipelineNode node) {
        return node.mapList("scripts");
    }
}
