package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.StateBackend;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.util.Durations;
import com.zifang.util.expr.el.ElEvaluator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 模式匹配算子：单流上按「步骤序列」做事件匹配（Flink CEP 口径的保守子集）。
 *
 * <p>声明是一串步骤（每步一个条件 + 可选次数量词），行进来时：
 * <b>能延续旧的部分匹配就延续，能开新匹配就开新匹配</b>（同一事件两件事都做 ——
 * 不做严格相邻 / 跳跃策略那套细分，保守口径：宁多配不少配）。
 * 最后一步的次数达到下限即产出一条匹配：行 = 最后一条事件的行 + 每步的
 * {@code 步骤名.列}（取该步第一条事件）+ {@code 步骤名#}（该步命中次数）。
 *
 * <pre>
 * - id: cep
 *   type: cep.pattern
 *   config:
 *     keyBy: [order_id]
 *     within: 5m              # 可选：整条模式「首尾事件时间差」的上限；水位过期清理
 *     maxPartials: 64         # 可选：每键部分匹配上限（防爆炸，超出丢最老并计数）
 *     steps:
 *       - {name: create, expression: "type == 'CREATE'"}
 *       - {name: pay, expression: "type == 'PAID'", times: "1,"}   # 至少 1 次（可继续吃同条件事件）
 *       - {name: ship, expression: "type == 'SHIP'", optional: true}  # 可选步骤：0 或 1 次
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li><b>次数量词</b>：{@code times} 缺省 {@code 1}；{@code "n"} 恰好 n 次；
 *       {@code "n,"} 至少 n 次（最后一步未封顶 ⇒ 产出后部分匹配继续活着吃同类事件）；
 *       {@code "n,m"} 闭区间；</li>
 *   <li><b>可选步骤</b>：{@code optional: true} 的步骤可以整步跳过（0 次）——
 *       事件不吃进该步时开一条「跳过它」的分支继续往下试，原匹配也留着
 *       （后续该步条件命中仍能走「吃到」路径，宁多配不少配）；
 *       跳过的步骤产出里只有 {@code 步骤名#}（= 0），没有 {@code 步骤名.列}；</li>
 *   <li><b>until 终止条件</b>：步骤可声明 {@code until} 表达式 —— 条件命中但 until
 *       也命中的事件<b>不吃进该步</b>（部分匹配留着等推进；比 Flink 的「until 销毁匹配」
 *       更保守：不销毁，只挡消费）；</li>
 *   <li><b>流尾封口</b>：声明了 optional 步骤的管道在流结束时（{@link #flush}）把
 *       「剩余步骤全是 optional」的部分匹配补产出（{@code flush-emitted} 计数）——
 *       等不到终态事件也别把能封口的匹配憋死；</li>
 *   <li><b>within 超时</b>：声明了 {@code within} 的行必须带事件时间（否则丢弃并计数）；
 *       部分「首事件」太老的匹配在水位推进时清掉（{@code 水位 − within} 之前起步的再也等不到）；</li>
 *   <li><b>防爆炸</b>：每键部分匹配数有上限（缺省 64），超出丢最老的并计数；</li>
 *   <li><b>状态可序列化</b>：部分匹配进键控状态 ⇒ 能进检查点。</li>
 * </ul>
 *
 * @author zifang
 */
public class CepPatternOperator implements Operator {

    /** 活跃键清单存放的哨兵键（不与真实键碰撞）。 */
    static final String KEY_SET_KEY = "\u0000cep-active-keys";

    /** 未声明 keyBy 时的全局键。 */
    private static final String GLOBAL_KEY = "\u0000cep-global";

    private final PipelineNode node;
    private final ScriptSandbox sandbox;

    private List<String> keyBy = new ArrayList<String>();
    private long withinMs;
    private int maxPartials = 64;
    private List<StepSpec> steps = new ArrayList<StepSpec>();

    private OperatorContext context;

    /**
     * @param node    节点声明
     * @param sandbox 脚本沙箱（步骤条件用 GROOVY 引擎时才需要）
     */
    public CepPatternOperator(PipelineNode node, ScriptSandbox sandbox) {
        this.node = node;
        this.sandbox = sandbox;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        keyBy = node.strings("keyBy");
        withinMs = Durations.parseMillis(node.string("within"), 0L);
        maxPartials = Math.max(1, node.integer("maxPartials", 64));
        List<Map<String, Object>> declared = node.mapList("steps");
        if (declared.isEmpty()) {
            throw new IllegalArgumentException("cep.pattern 节点 " + node.getId() + " 必须声明 steps");
        }
        for (Map<String, Object> item : declared) {
            StepSpec spec = new StepSpec();
            spec.name = item.get("name") == null ? null : String.valueOf(item.get("name")).trim();
            spec.expression = item.get("expression") == null ? null : String.valueOf(item.get("expression")).trim();
            if (spec.name == null || spec.name.isEmpty() || spec.expression == null || spec.expression.isEmpty()) {
                throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                        + " 的步骤缺少 name/expression: " + item);
            }
            parseTimes(spec, item.get("times") == null ? null : String.valueOf(item.get("times")));
            spec.optional = Boolean.parseBoolean(String.valueOf(item.get("optional")));
            if (spec.optional && (spec.minTimes != 1 || spec.maxTimes != 1)) {
                throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                        + " 的步骤 " + spec.name + " 是 optional，不能再声明 times（optional 就是 0 或 1 次）");
            }
            spec.until = item.get("until") == null ? null : String.valueOf(item.get("until")).trim();
            if (spec.until != null && spec.until.isEmpty()) {
                spec.until = null;
            }
            spec.engine = item.get("engine") == null ? "EL" : String.valueOf(item.get("engine")).toUpperCase();
            spec.timeoutMs = item.get("timeoutMs") == null ? 2000L
                    : Long.parseLong(String.valueOf(item.get("timeoutMs")).trim());
            if ("EL".equals(spec.engine)) {
                spec.el = new ElEvaluator();
                if (spec.until != null) {
                    spec.untilEl = new ElEvaluator();
                }
            } else if ("GROOVY".equals(spec.engine) && sandbox == null) {
                throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                        + " 的步骤 " + spec.name + " 指定了 GROOVY 引擎，但没有沙箱执行器");
            }
            steps.add(spec);
        }
        if (ctx.state() == null) {
            throw new IllegalStateException("cep.pattern 节点 " + node.getId()
                    + " 需要键控状态，但没有分配到状态存储");
        }
    }

    /** 次数量词：缺省 1；"n" / "n," / "n,m"。 */
    private void parseTimes(StepSpec spec, String times) {
        spec.minTimes = 1;
        spec.maxTimes = 1;
        if (times == null || times.trim().isEmpty()) {
            return;
        }
        String text = times.trim();
        int comma = text.indexOf(',');
        try {
            if (comma < 0) {
                spec.minTimes = spec.maxTimes = Integer.parseInt(text);
                return;
            }
            spec.minTimes = Integer.parseInt(text.substring(0, comma).trim());
            String upper = text.substring(comma + 1).trim();
            spec.maxTimes = upper.isEmpty() ? Integer.MAX_VALUE : Integer.parseInt(upper);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                    + " 的步骤 " + spec.name + " 的 times 不合法（缺省 1；n / n, / n,m）: " + times);
        }
        if (spec.minTimes < 1 || spec.maxTimes < spec.minTimes) {
            throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                    + " 的步骤 " + spec.name + " 的 times 要 1 ≤ 下限 ≤ 上限: " + times);
        }
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        String key = keyBy.isEmpty() ? GLOBAL_KEY : Keys.computeValues(record.row(), keyBy);
        if (key == null) {
            throw new IllegalStateException("cep.pattern 节点 " + node.getId() + " 的键列为空，算不出键");
        }
        long eventTime = record.hasEventTime() ? record.eventTime() : Record.NO_EVENT_TIME;
        if (withinMs > 0L && eventTime == Record.NO_EVENT_TIME) {
            context.count("no-event-time-dropped");
            return;
        }

        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        List<Partial> partials = decode(store.listState("partials").get());

        List<Partial> next = new ArrayList<Partial>();
        for (Partial partial : partials) {
            // 太老的部分匹配：等到的事件也填不满 within ⇒ 丢
            if (withinMs > 0L && eventTime - partial.startEventTime > withinMs) {
                context.count("expired");
                continue;
            }
            extend(partial, record, eventTime, next, output);
        }
        start(record, eventTime, next, output);
        while (next.size() > maxPartials) {
            next.remove(0);
            context.count("partial-overflow-dropped");
        }
        store.listState("partials").update(encode(next));
        rememberKey(store, key);
    }

    /** 用当前事件延续既有匹配：同级续吃（次数未封顶）+ 向下一步推进 + 可选步骤跳过。 */
    private void extend(Partial partial, Record record, long eventTime,
                        List<Partial> next, Output output) throws Exception {
        StepSpec current = steps.get(partial.stepIndex);
        StepMatch currentMatch = partial.steps.get(partial.stepIndex);
        boolean grownLast = false;
        // 同级续吃：次数还没封顶、条件又命中、until 又没拦
        if (currentMatch.rows.size() < current.maxTimes
                && matches(current, record.row()) && !untilFired(current, record.row(), "续吃")) {
            Partial grown = partial.copy();
            grown.steps.get(grown.stepIndex).rows.add(row(record));
            if (grown.stepIndex == steps.size() - 1
                    && grown.steps.get(grown.stepIndex).rows.size() >= current.minTimes) {
                emit(grown, record, output);
                grownLast = true;
            }
            // 未产出（次数还没到下限）或未封顶 ⇒ 续吃后的匹配继续活着
            if (!grownLast || current.maxTimes == Integer.MAX_VALUE) {
                next.add(grown);
            }
        }
        // 向下推进：下一步条件命中（推进即取代 —— 原匹配不再保留，避免同一步骤反复产出重复匹配）
        boolean advanced = false;
        int target = partial.stepIndex + 1;
        if (target < steps.size()) {
            StepSpec step = steps.get(target);
            if (matches(step, record.row())) {
                advanced = true;
                Partial nextPartial = partial.copy();
                nextPartial.stepIndex = target;
                StepMatch created = new StepMatch();
                created.name = step.name;
                created.rows.add(row(record));
                nextPartial.steps.add(created);
                if (target == steps.size() - 1 && step.minTimes <= 1) {
                    emit(nextPartial, record, output);
                    if (step.maxTimes == Integer.MAX_VALUE) {
                        next.add(nextPartial);
                    }
                } else {
                    next.add(nextPartial);
                }
                // optional 步骤吃到了也开一条「不吃（0 次）」的分支 —— 两条路径都合法
                if (step.optional && target + 1 < steps.size()) {
                    skipAndFeed(partial, target, record, eventTime, next, output);
                }
            } else if (step.optional && target + 1 < steps.size()) {
                // 下一步是 optional 又不吃这个事件 ⇒ 跳过它，同一事件继续往下试
                skipAndFeed(partial, target, record, eventTime, next, output);
            }
        }
        // 没推进才保留原匹配（还能被后续事件推进 / 续吃）
        if (!advanced && !grownLast) {
            next.add(partial);
        }
    }

    /** 开一条「跳过 optional 步骤」的分支（该步 0 行），同一事件喂进去继续试后面的步骤。 */
    private void skipAndFeed(Partial partial, int target, Record record, long eventTime,
                             List<Partial> next, Output output) throws Exception {
        Partial skipped = partial.copy();
        StepMatch empty = new StepMatch();
        empty.name = steps.get(target).name;
        skipped.steps.add(empty);
        skipped.stepIndex = target + 1;
        // 新的当前步也要有位子（还没吃到，先空着；不变式：steps.size() == stepIndex + 1）
        StepMatch pending = new StepMatch();
        pending.name = steps.get(target + 1).name;
        skipped.steps.add(pending);
        context.count("optional-skipped");
        extend(skipped, record, eventTime, next, output);
    }

    /** 用当前事件开新匹配（第一步条件命中）。 */
    private void start(Record record, long eventTime, List<Partial> next, Output output) throws Exception {
        StepSpec first = steps.get(0);
        if (!matches(first, record.row())) {
            return;
        }
        Partial partial = new Partial();
        partial.stepIndex = 0;
        partial.startEventTime = eventTime == Record.NO_EVENT_TIME ? System.currentTimeMillis() : eventTime;
        StepMatch created = new StepMatch();
        created.name = first.name;
        created.rows.add(row(record));
        partial.steps.add(created);
        if (steps.size() == 1 && first.minTimes <= 1) {
            emit(partial, record, output);
            if (first.maxTimes == Integer.MAX_VALUE) {
                next.add(partial);
            }
            return;
        }
        next.add(partial);
        context.count("started");
    }

    /** 匹配完成 ⇒ 产出：行 = 最后事件的行 + 每步「步骤名.列」（第一步事件）+「步骤名#」次数。 */
    private void emit(Partial partial, Record record, Output output) {
        output.collect(record.withRow(assembled(partial, record.row())));
        context.count("matched");
    }

    /** 组装匹配行：基底行 + 每步「步骤名.列」（该步第一条事件，跳过的步骤没有）+「步骤名#」次数。 */
    private Map<String, Object> assembled(Partial partial, Map<String, Object> baseRow) {
        Map<String, Object> out = new LinkedHashMap<String, Object>(baseRow);
        for (StepMatch step : partial.steps) {
            Map<String, Object> first = step.rows.isEmpty() ? null : step.rows.get(0);
            if (first != null) {
                for (Map.Entry<String, Object> entry : first.entrySet()) {
                    out.put(step.name + "." + entry.getKey(), entry.getValue());
                }
            }
            out.put(step.name + "#", step.rows.size());
        }
        return out;
    }

    /**
     * 流尾封口：剩余步骤全是 optional 的部分匹配，等不到终态事件也在流结束时补产出。
     *
     * <p>只对声明了 optional 步骤的管道生效（没有 optional 就没有「跳着等」的匹配可封）。
     */
    @Override
    public void flush(Output output) {
        boolean anyOptional = false;
        for (StepSpec step : steps) {
            if (step.optional) {
                anyOptional = true;
                break;
            }
        }
        if (!anyOptional) {
            return;
        }
        StateBackend.KeyedStateStore store = context.state();
        for (String key : new LinkedHashSet<String>(activeKeys(store))) {
            store.setCurrentKey(key);
            for (Partial partial : decode(store.listState("partials").get())) {
                if (canFlushEmit(partial)) {
                    Partial sealed = partial.copy();
                    for (int i = partial.stepIndex + 1; i < steps.size(); i++) {
                        StepMatch empty = new StepMatch();
                        empty.name = steps.get(i).name;
                        sealed.steps.add(empty);
                    }
                    List<Map<String, Object>> rows = partial.steps.get(partial.stepIndex).rows;
                    Map<String, Object> last = rows.isEmpty()
                            ? new LinkedHashMap<String, Object>() : rows.get(rows.size() - 1);
                    output.collect(Record.of(assembled(sealed, last)));
                    context.count("flush-emitted");
                }
            }
        }
    }

    /** 能否流尾封口：还没走到最后一步，且剩余步骤全是 optional。 */
    private boolean canFlushEmit(Partial partial) {
        if (partial.stepIndex >= steps.size() - 1) {
            return false;
        }
        for (int i = partial.stepIndex + 1; i < steps.size(); i++) {
            if (!steps.get(i).optional) {
                return false;
            }
        }
        return true;
    }

    /** 步骤条件求值（EL 缺省；GROOVY 走沙箱；LUA/JS 无沙箱 ⇒ 硬拒绝）。 */
    private boolean matches(StepSpec step, Map<String, Object> row) {
        Object value;
        try {
            if ("EL".equals(step.engine)) {
                value = step.el.eval(step.expression, row);
            } else if ("GROOVY".equals(step.engine)) {
                value = sandbox.evalGroovy(step.expression, row, step.timeoutMs);
            } else {
                throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                        + " 的引擎不支持: " + step.engine + "（LUA/JS 无沙箱 ⇒ 硬拒绝）");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                    + " 的步骤 " + step.name + " 求值失败: " + e.getMessage(), e);
        }
        return toBoolean(value);
    }

    /** until 终止条件：命中 ⇒ 该事件不吃进这一步（部分匹配留着等推进，不销毁）。 */
    private boolean untilFired(StepSpec step, Map<String, Object> row, String where) {
        if (step.until == null) {
            return false;
        }
        Object value;
        try {
            if ("EL".equals(step.engine)) {
                value = step.untilEl.eval(step.until, row);
            } else if ("GROOVY".equals(step.engine)) {
                value = sandbox.evalGroovy(step.until, row, step.timeoutMs);
            } else {
                return false;
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("cep.pattern 节点 " + node.getId()
                    + " 的步骤 " + step.name + " 的 until（" + where + "）求值失败: " + e.getMessage(), e);
        }
        boolean fired = toBoolean(value);
        if (fired) {
            context.count("until-stopped");
        }
        return fired;
    }

    /** 结果归布尔（与 filter 同口径）。 */
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

    /** 水位推进：起步太早（早于「水位 − within」）的部分匹配清掉。 */
    @Override
    public void processWatermark(long watermark) {
        if (withinMs <= 0L) {
            return;
        }
        long expireBefore = watermark - withinMs;
        if (expireBefore <= 0L) {
            return;
        }
        StateBackend.KeyedStateStore store = context.state();
        Set<String> activeKeys = activeKeys(store);
        long expired = 0L;
        for (String key : new LinkedHashSet<String>(activeKeys)) {
            store.setCurrentKey(key);
            List<Partial> partials = decode(store.listState("partials").get());
            List<Partial> kept = new ArrayList<Partial>();
            for (Partial partial : partials) {
                if (partial.startEventTime < expireBefore) {
                    expired++;
                    continue;
                }
                kept.add(partial);
            }
            if (kept.isEmpty()) {
                store.listState("partials").clear();
                activeKeys.remove(key);
            } else {
                store.listState("partials").update(encode(kept));
            }
        }
        if (expired > 0L) {
            context.count("expired", expired);
            store.setCurrentKey(KEY_SET_KEY);
            store.valueState("activeKeys").update(new LinkedHashSet<String>(activeKeys));
        }
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 模式匹配不涉定时器（超时靠水位）
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    private static Map<String, Object> row(Record record) {
        return new LinkedHashMap<String, Object>(record.row());
    }

    /** 记下活跃键（水位的全局清理靠它遍历）。 */
    private void rememberKey(StateBackend.KeyedStateStore store, String key) {
        Set<String> keys = activeKeys(store);
        keys.add(key);
        store.setCurrentKey(KEY_SET_KEY);
        store.valueState("activeKeys").update(new LinkedHashSet<String>(keys));
    }

    /** 活跃键清单（存放在哨兵键下）。 */
    @SuppressWarnings("unchecked")
    private Set<String> activeKeys(StateBackend.KeyedStateStore store) {
        store.setCurrentKey(KEY_SET_KEY);
        Object value = store.valueState("activeKeys").value();
        if (value instanceof Set) {
            return (Set<String>) value;
        }
        return new LinkedHashSet<String>();
    }

    @SuppressWarnings("unchecked")
    private List<Partial> decode(List<Object> raw) {
        List<Partial> result = new ArrayList<Partial>();
        for (Object item : raw) {
            if (item instanceof Partial) {
                result.add((Partial) item);
            }
        }
        return result;
    }

    private List<Object> encode(List<Partial> partials) {
        return new ArrayList<Object>(partials);
    }

    /** 一个步骤的声明（条件 + 次数量词 + optional + until + 求值引擎）。 */
    private static final class StepSpec {
        private String name;
        private String expression;
        private String engine;
        private long timeoutMs;
        private int minTimes;
        private int maxTimes;
        private boolean optional;
        private String until;
        private ElEvaluator el;
        private ElEvaluator untilEl;
    }

    /** 一个步骤的已匹配事件（可序列化 ⇒ 能进检查点）。 */
    private static final class StepMatch implements Serializable {

        private static final long serialVersionUID = 1L;

        private String name;
        private final List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    }

    /** 一条部分匹配：填到第几步 + 起步时间 + 各步已配到的事件（可序列化）。 */
    private static final class Partial implements Serializable {

        private static final long serialVersionUID = 1L;

        private int stepIndex;
        private long startEventTime;
        private final List<StepMatch> steps = new ArrayList<StepMatch>();

        private Partial copy() {
            Partial copy = new Partial();
            copy.stepIndex = this.stepIndex;
            copy.startEventTime = this.startEventTime;
            for (StepMatch step : this.steps) {
                StepMatch copied = new StepMatch();
                copied.name = step.name;
                copied.rows.addAll(step.rows);
                copy.steps.add(copied);
            }
            return copy;
        }
    }
}
