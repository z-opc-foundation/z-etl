package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.transform.ScriptSandbox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 脚本源（输入类型）：用 z-script 沙箱脚本**生成**一条流。
 *
 * <p>脚本返回值口径：
 * <ul>
 *   <li>{@code List<Map>} ⇒ 一行一个 Map；</li>
 *   <li>{@code Map} ⇒ 单行；</li>
 *   <li>{@code List<标量>} ⇒ 每行一列（列名取声明的 {@code name}，默认 {@code value}）。</li>
 * </ul>
 *
 * <p>安全闸与脚本算子同口径：沙箱 + 墙钟超时 + 单次产出上限；{@code LUA} 硬拒绝。
 *
 * <pre>
 * - id: gen
 *   type: script.source
 *   config:
 *     engine: GROOVY
 *     script: |
 *       (1..3).collect { [id: it, name: "第${it}行"] }
 * </pre>
 *
 * @author zifang
 */
public class ScriptSource extends AbstractPullSource {

    private final ScriptSandbox sandbox;
    private String engine = "GROOVY";
    private String script;
    private String columnName = "value";
    private long timeoutMs = 2000L;
    private int maxOutputChars = 65536;

    private List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    private int index;

    public ScriptSource(PipelineNode node, ScriptSandbox sandbox) {
        super(node);
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        script = node.string("script");
        if (script == null) {
            throw new IllegalArgumentException("script.source 节点 " + node.getId() + " 缺少 script");
        }
        engine = node.string("engine", "GROOVY").toUpperCase();
        columnName = node.string("name", "value");
        timeoutMs = node.longValue("timeoutMs", 2000L);
        maxOutputChars = node.integer("maxOutputChars", 65536);
        if (!"GROOVY".equals(engine) && !"EL".equals(engine)) {
            throw new IllegalArgumentException("script.source 节点 " + node.getId() + " 的引擎不支持: " + engine
                    + "（LUA 执行器无沙箱无超时、实测可逃逸 ⇒ 硬拒绝）");
        }
        if (sandbox == null) {
            throw new IllegalArgumentException("script.source 节点 " + node.getId() + " 需要沙箱执行器，但未配置");
        }
        Object value = evaluate();
        rows = toRows(value);
        index = 0;
    }

    /** 执行脚本（超时 / 超限 / 失败一律抛）。 */
    private Object evaluate() {
        Object value;
        try {
            Map<String, Object> bindings = new LinkedHashMap<String, Object>();
            if ("GROOVY".equals(engine)) {
                value = sandbox.evalGroovy(script, bindings, timeoutMs);
            } else {
                value = sandbox.evalEl(script, bindings, timeoutMs);
            }
        } catch (com.zifang.z.script.engine.sandbox.ScriptTimeoutException e) {
            throw new IllegalStateException("script.source 节点 " + node.getId() + " 执行超时("
                    + timeoutMs + "ms)，已强制中断", e);
        } catch (RuntimeException e) {
            throw new IllegalStateException("script.source 节点 " + node.getId() + " 执行失败: " + e.getMessage(), e);
        }
        int size = value == null ? 0 : String.valueOf(value).length();
        if (size > maxOutputChars) {
            throw new IllegalStateException("script.source 节点 " + node.getId() + " 产出 " + size
                    + " 字符，超过单次上限 " + maxOutputChars + " 字符（内存越限兜底）");
        }
        return value;
    }

    /** 脚本返回值 → 行列表。 */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> toRows(Object value) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        if (value == null) {
            return result;
        }
        if (value instanceof List) {
            for (Object item : (List<Object>) value) {
                result.add(toRow(item));
            }
            return result;
        }
        result.add(toRow(value));
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> toRow(Object item) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        if (item instanceof Map) {
            row.putAll((Map<String, Object>) item);
            return row;
        }
        row.put(columnName, item);
        return row;
    }

    @Override
    protected Map<String, Object> doNext() {
        if (index >= rows.size()) {
            return null;
        }
        return rows.get(index++);
    }
}
