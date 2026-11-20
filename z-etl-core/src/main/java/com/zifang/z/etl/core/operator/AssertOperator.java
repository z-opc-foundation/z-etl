package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.OutputTag;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.util.RowValues;
import com.zifang.util.expr.el.ElEvaluator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据质量断言算子：链上任一点插一道闸，**不合格的数据不许悄悄流下去**。
 *
 * <p>三类校验：
 * <ul>
 *   <li><b>逐行判定</b>（边流边判，违规当场处置）：{@code expression}（表达式为假即违规）、
 *       {@code inRange}（列落在区间外）、{@code allowedValues}（列不在允许值集合里）；</li>
 *   <li><b>唯一性</b>（逐行判、撞上重复即违规）：{@code unique}；</li>
 *   <li><b>结算期判定</b>（流结束才知道，{@code close()} 时结算）：{@code minRows} /
 *       {@code maxRows} / {@code exactRows}（行数）、{@code notNullRate}（列非空率）。</li>
 * </ul>
 *
 * <p>违规处置（{@code mode}）：
 * <ul>
 *   <li>{@code fail}（默认）：立刻抛错 ⇒ 整条流水线失败（数据已经写下去的部分不回滚，
 *       但水位不落库 ⇒ 下轮重跑）；</li>
 *   <li>{@code reject}：违规行走 {@code rejectedTo} 旁路（脏数据有去处），好数据照常流；</li>
 *   <li>{@code count}：只计数不处置（当作体检指标看）。</li>
 * </ul>
 *
 * <p>口径说明：结算期判定（行数 / 非空率）**只在流结束时**才有结论 ⇒ 失败时数据已经落库；
 * 这是「质量闸」与「事务」的差别，声明时就得想清楚。
 *
 * @author zifang
 */
public class AssertOperator implements Operator {

    /** 违规即失败。 */
    private static final String MODE_FAIL = "fail";
    /** 违规走旁路。 */
    private static final String MODE_REJECT = "reject";
    /** 违规只计数。 */
    private static final String MODE_COUNT = "count";

    private final PipelineNode node;
    private final ScriptSandbox sandbox;

    private String mode;
    private OutputTag rejectedTag;

    private String expression;
    private String engine;
    private long timeoutMs;
    private ElEvaluator elEvaluator;

    private Map<String, double[]> inRange = new LinkedHashMap<String, double[]>();
    private Map<String, Set<Object>> allowedValues = new LinkedHashMap<String, Set<Object>>();
    private Map<String, Double> notNullRate = new LinkedHashMap<String, Double>();
    private Map<String, Long> nullCounts = new LinkedHashMap<String, Long>();
    private List<String> uniqueColumns = new ArrayList<String>();
    private Map<String, Set<Object>> uniqueValues = new LinkedHashMap<String, Set<Object>>();

    private long minRows = -1L;
    private long maxRows = -1L;
    private long exactRows = -1L;
    private int maxTracked = 1000000;

    private OperatorContext context;
    private long rows;
    private long violations;
    private boolean settled;

    /**
     * @param node    节点声明
     * @param sandbox 脚本沙箱（GROOVY 表达式用）
     */
    public AssertOperator(PipelineNode node, ScriptSandbox sandbox) {
        this.node = node;
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext context) {
        this.context = context;
        mode = node.string("mode", MODE_FAIL).toLowerCase();
        if (!MODE_FAIL.equals(mode) && !MODE_REJECT.equals(mode) && !MODE_COUNT.equals(mode)) {
            throw new IllegalArgumentException("assert 节点 " + node.getId() + " 的 mode 只支持 "
                    + MODE_FAIL + "/" + MODE_REJECT + "/" + MODE_COUNT + "，实际: " + mode);
        }
        String rejectedTo = node.string("rejectedTo");
        if (MODE_REJECT.equals(mode) && rejectedTo == null) {
            throw new IllegalArgumentException("assert 节点 " + node.getId()
                    + " 的 mode=reject 必须声明 rejectedTo（违规行要有去处）");
        }
        if (rejectedTo != null) {
            rejectedTag = new OutputTag(rejectedTo);
        }

        expression = node.string("expression");
        engine = node.string("engine", "EL").toUpperCase();
        timeoutMs = node.longValue("timeoutMs", 2000L);
        if (expression != null) {
            if ("EL".equals(engine)) {
                elEvaluator = new ElEvaluator();
            } else if ("GROOVY".equals(engine) && sandbox == null) {
                throw new IllegalArgumentException("assert 节点 " + node.getId()
                        + " 指定了 GROOVY 引擎，但没有沙箱执行器");
            }
        }

        for (Map.Entry<String, Object> entry : node.map("inRange").entrySet()) {
            inRange.put(entry.getKey(), parseRange(node.getId(), entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Object> entry : node.map("allowedValues").entrySet()) {
            allowedValues.put(entry.getKey(), parseValues(entry.getValue()));
        }
        for (Map.Entry<String, Object> entry : node.map("notNullRate").entrySet()) {
            Object rate = entry.getValue();
            double value = rate instanceof Number ? ((Number) rate).doubleValue() : Double.NaN;
            if (Double.isNaN(value) || value < 0d || value > 1d) {
                throw new IllegalArgumentException("assert 节点 " + node.getId()
                        + " 的 notNullRate[" + entry.getKey() + "] 必须是 0~1 的数字，实际: " + rate);
            }
            notNullRate.put(entry.getKey(), Double.valueOf(value));
            nullCounts.put(entry.getKey(), Long.valueOf(0L));
        }
        uniqueColumns = node.strings("unique");
        for (String column : uniqueColumns) {
            uniqueValues.put(column, new LinkedHashSet<Object>());
        }
        minRows = node.longValue("minRows", -1L);
        maxRows = node.longValue("maxRows", -1L);
        exactRows = node.longValue("exactRows", -1L);
        maxTracked = node.integer("maxTracked", 1000000);

        if (expression == null && inRange.isEmpty() && allowedValues.isEmpty()
                && notNullRate.isEmpty() && uniqueColumns.isEmpty()
                && minRows < 0L && maxRows < 0L && exactRows < 0L) {
            throw new IllegalArgumentException("assert 节点 " + node.getId() + " 没有任何校验项");
        }
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        rows++;
        if (context != null) {
            context.count("rows");
        }
        String reason = checkRow(record.row());
        if (reason == null) {
            output.collect(record);
            return;
        }
        violations++;
        if (context != null) {
            context.count("violations");
        }
        if (MODE_FAIL.equals(mode)) {
            throw new IllegalStateException("断言节点 " + node.getId() + " 校验失败：" + reason
                    + "；行: " + brief(record.row()));
        }
        if (MODE_REJECT.equals(mode)) {
            output.collect(rejectedTag, record);
            return;
        }
        output.collect(record);
    }

    /** 逐行判定；返回违规原因（合规返回 {@code null}）。 */
    private String checkRow(Map<String, Object> row) {
        if (expression != null) {
            Object value = evaluate(row);
            if (!toBoolean(value)) {
                return "表达式不成立（" + expression + "）";
            }
        }
        for (Map.Entry<String, double[]> entry : inRange.entrySet()) {
            Object raw = RowValues.get(row, entry.getKey());
            if (raw == null) {
                continue;
            }
            Double number = toDouble(raw);
            if (number == null) {
                return "列 " + entry.getKey() + " 不是数字，无法判区间（值: " + raw + "）";
            }
            double[] range = entry.getValue();
            if (number.doubleValue() < range[0] || number.doubleValue() > range[1]) {
                return "列 " + entry.getKey() + " 超出区间 [" + range[0] + ", " + range[1]
                        + "]（值: " + raw + "）";
            }
        }
        for (Map.Entry<String, Set<Object>> entry : allowedValues.entrySet()) {
            Object raw = RowValues.get(row, entry.getKey());
            if (raw == null) {
                continue;
            }
            if (!contains(entry.getValue(), raw)) {
                return "列 " + entry.getKey() + " 不在允许值里（值: " + raw + "，允许: "
                        + entry.getValue() + "）";
            }
        }
        for (String column : notNullRate.keySet()) {
            if (RowValues.get(row, column) == null) {
                Long count = nullCounts.get(column);
                nullCounts.put(column, Long.valueOf(count == null ? 1L : count.longValue() + 1L));
            }
        }
        for (String column : uniqueColumns) {
            Object raw = RowValues.get(row, column);
            if (raw == null) {
                continue;
            }
            Set<Object> tracked = uniqueValues.get(column);
            if (tracked.size() >= maxTracked) {
                throw new IllegalStateException("断言节点 " + node.getId() + " 的唯一性校验追踪了 "
                        + tracked.size() + " 个不同值，超过上限 maxTracked=" + maxTracked
                        + "（调大它，或去掉 unique 校验）");
            }
            if (!tracked.add(raw)) {
                return "列 " + column + " 出现重复值（" + raw + "）";
            }
        }
        return null;
    }

    /** 表达式求值（EL / 沙箱 GROOVY）；失败抛异常（不静默当合规）。 */
    private Object evaluate(Map<String, Object> row) {
        try {
            if ("EL".equals(engine)) {
                return elEvaluator.eval(expression, row);
            }
            if ("GROOVY".equals(engine)) {
                return sandbox.evalGroovy(expression, row, timeoutMs);
            }
            throw new IllegalArgumentException("assert 节点 " + node.getId() + " 的引擎不支持: " + engine
                    + "（LUA/JS 无沙箱 ⇒ 硬拒绝）");
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("assert 节点 " + node.getId() + " 求值失败: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (settled) {
            return;
        }
        settled = true;
        List<String> failures = new ArrayList<String>();
        if (exactRows >= 0L && rows != exactRows) {
            failures.add("行数应为 " + exactRows + "，实际 " + rows);
        }
        if (minRows >= 0L && rows < minRows) {
            failures.add("行数应不少于 " + minRows + "，实际 " + rows);
        }
        if (maxRows >= 0L && rows > maxRows) {
            failures.add("行数应不多于 " + maxRows + "，实际 " + rows);
        }
        for (Map.Entry<String, Double> entry : notNullRate.entrySet()) {
            long nulls = nullCounts.get(entry.getKey()) == null ? 0L : nullCounts.get(entry.getKey()).longValue();
            double rate = rows <= 0L ? 0d : (double) (rows - nulls) / (double) rows;
            if (rate + 1e-9 < entry.getValue().doubleValue()) {
                failures.add("列 " + entry.getKey() + " 非空率应不低于 " + entry.getValue()
                        + "，实际 " + rate + "（空值 " + nulls + " / 总行 " + rows + "）");
            }
        }
        if (failures.isEmpty()) {
            return;
        }
        violations += failures.size();
        if (context != null) {
            context.count("violations", failures.size());
        }
        if (MODE_COUNT.equals(mode)) {
            return;
        }
        throw new IllegalStateException("断言节点 " + node.getId() + " 流结束结算不通过：" + String.join("；", failures));
    }

    @Override
    public void processWatermark(long watermark) {
        // 断言不涉水位（结算在流结束）
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 断言不涉定时器
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 行数（已处理）。 */
    public long rows() {
        return rows;
    }

    /** 违规次数。 */
    public long violations() {
        return violations;
    }

    /** 区间声明解析：{@code [min, max]}。 */
    private static double[] parseRange(String nodeId, String column, Object raw) {
        if (!(raw instanceof List) || ((List<?>) raw).size() != 2) {
            throw new IllegalArgumentException("assert 节点 " + nodeId + " 的 inRange[" + column
                    + "] 必须是 [最小值, 最大值] 两元素数组");
        }
        List<?> list = (List<?>) raw;
        Double min = toDouble(list.get(0));
        Double max = toDouble(list.get(1));
        if (min == null || max == null) {
            throw new IllegalArgumentException("assert 节点 " + nodeId + " 的 inRange[" + column
                    + "] 端点必须是数字");
        }
        if (min.doubleValue() > max.doubleValue()) {
            throw new IllegalArgumentException("assert 节点 " + nodeId + " 的 inRange[" + column
                    + "] 最小值大于最大值");
        }
        return new double[]{min.doubleValue(), max.doubleValue()};
    }

    /** 允许值集合解析。 */
    private static Set<Object> parseValues(Object raw) {
        Set<Object> values = new LinkedHashSet<Object>();
        if (raw instanceof List) {
            for (Object item : (List<?>) raw) {
                values.add(item);
            }
        } else if (raw != null) {
            values.add(raw);
        }
        return values;
    }

    /** 值是否在集合里（数字按数值比，其它按字符串比）。 */
    private static boolean contains(Set<Object> values, Object value) {
        if (values.contains(value)) {
            return true;
        }
        Double number = toDouble(value);
        if (number == null) {
            return false;
        }
        for (Object candidate : values) {
            Double other = toDouble(candidate);
            if (other != null && other.doubleValue() == number.doubleValue()) {
                return true;
            }
        }
        return false;
    }

    /** 转数字（BigDecimal 精度优先；字符串也认）。 */
    private static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return Double.valueOf(((Number) value).doubleValue());
        }
        try {
            return Double.valueOf(new BigDecimal(String.valueOf(value).trim()).doubleValue());
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** 结果归布尔：Boolean / 数字（非零）/ 字符串（true/1/yes）。 */
    private static boolean toBoolean(Object value) {
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

    /** 行摘要（报错时给点上下文，别把整行刷屏）。 */
    private static String brief(Map<String, Object> row) {
        if (row == null) {
            return "{}";
        }
        String text = String.valueOf(row);
        return text.length() <= 300 ? text : text.substring(0, 300) + "...";
    }
}
