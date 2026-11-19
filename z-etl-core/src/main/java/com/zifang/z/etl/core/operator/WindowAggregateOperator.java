package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.StateBackend;
import com.zifang.z.etl.core.runtime.TimeWindow;
import com.zifang.z.etl.core.runtime.WindowAssigners;
import com.zifang.z.etl.core.util.Durations;
import com.zifang.z.etl.core.util.RowValues;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 窗口聚合算子（事件时间窗口 + 水位触发 + 允许迟到）。
 *
 * <p>口径照抄 Flink 的窗口生命周期：
 * {@code keyBy → window → [trigger] → [evictor] → [allowedLateness] → aggregate}。
 * <ul>
 *   <li><b>触发</b>：水位推过窗口结束点 ⇒ 结算并发出（Flink 默认的 {@code EventTimeTrigger}）。</li>
 *   <li><b>允许迟到</b>：水位推过 {@code end + allowedLateness} 之后再来的数据 ⇒ 丢弃并计数；
 *       落在 {@code allowedLateness} 之内的迟到数据 ⇒ 重新累加并**再发一次**（Flink 的重触发语义）。</li>
 *   <li><b>会话窗口</b>：候选窗口与同组已有窗口相交时按 key 合并（会话窗口的定义就是这个）。</li>
 * </ul>
 *
 * <p>没有事件时间的记录归入**全局窗口**（流结束才结算）⇒ 不丢数据。
 *
 * @author zifang
 */
public class WindowAggregateOperator implements Operator {

    private final PipelineNode node;

    private List<String> keyColumns = new ArrayList<String>();
    private List<AggregateDef> defs = new ArrayList<AggregateDef>();
    private WindowAssigners.WindowAssigner assigner;
    private boolean sessionMode;
    private long allowedLateness;
    private boolean emitWindowBounds = true;

    private OperatorContext context;
    private Output currentOutput;
    private long lastWatermark = Long.MIN_VALUE;

    public WindowAggregateOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        keyColumns = node.strings("keyBy");
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException("window.aggregate 节点 " + node.getId() + " 必须声明 keyBy");
        }
        defs = new ArrayList<AggregateDef>();
        for (Map<String, Object> item : node.mapList("aggregates")) {
            defs.add(AggregateDef.from(item));
        }
        if (defs.isEmpty()) {
            throw new IllegalArgumentException("window.aggregate 节点 " + node.getId() + " 必须声明 aggregates");
        }
        Map<String, Object> window = node.map("window");
        String type = MapOperator.text(window.get("type"));
        type = type == null ? "tumbling" : type.toLowerCase();
        long size = Durations.parseMillis(MapOperator.text(window.get("size")), 0L);
        long slide = Durations.parseMillis(MapOperator.text(window.get("slide")), 0L);
        long gap = Durations.parseMillis(MapOperator.text(window.get("gap")), 0L);
        long offset = Durations.parseMillis(MapOperator.text(window.get("offset")), 0L);
        if ("tumbling".equals(type)) {
            if (size <= 0L) {
                throw new IllegalArgumentException("window.aggregate 节点 " + node.getId() + " 的滚动窗口缺少 size");
            }
            assigner = WindowAssigners.tumbling(size, offset);
        } else if ("sliding".equals(type)) {
            if (size <= 0L || slide <= 0L) {
                throw new IllegalArgumentException("window.aggregate 节点 " + node.getId()
                        + " 的滑动窗口需要 size 与 slide");
            }
            assigner = WindowAssigners.sliding(size, slide, offset);
        } else if ("session".equals(type)) {
            if (gap <= 0L) {
                throw new IllegalArgumentException("window.aggregate 节点 " + node.getId() + " 的会话窗口缺少 gap");
            }
            assigner = WindowAssigners.session(gap);
            sessionMode = true;
        } else if ("global".equals(type)) {
            assigner = WindowAssigners.global();
        } else {
            throw new IllegalArgumentException("window.aggregate 节点 " + node.getId() + " 的窗口类型不支持: " + type
                    + "（可用: tumbling/sliding/session/global）");
        }
        allowedLateness = Durations.parseMillis(node.string("allowedLateness"), 0L);
        emitWindowBounds = node.bool("emitWindowBounds", true);
        if (ctx.state() == null) {
            throw new IllegalStateException("window.aggregate 节点 " + node.getId() + " 需要键控状态，但没有分配到状态存储");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        this.currentOutput = output;
        String groupKey = Keys.compute(record.row(), keyColumns);
        if (groupKey == null) {
            throw new IllegalStateException("window.aggregate 节点 " + node.getId() + " 的 keyBy 列为空，算不出分组键");
        }
        Map<String, WindowState> windows = loadWindows();
        Collection<TimeWindow> assigned;
        if (record.hasEventTime()) {
            assigned = assigner.assign(record.eventTime());
        } else {
            assigned = Collections.singletonList(new TimeWindow(Long.MIN_VALUE, Long.MAX_VALUE));
        }
        for (TimeWindow candidate : assigned) {
            WindowState target = sessionMode
                    ? mergeSession(windows, groupKey, candidate)
                    : window(windows, groupKey, candidate);
            if (target.fired && lastWatermark > target.end + allowedLateness) {
                context.count("late-dropped");
                saveWindows(windows);
                continue;
            }
            accumulate(target, record.row());
            target.keyValues = keyValues(record.row());
            if (target.fired) {
                // 允许迟到范围内的迟到数据：重新累加并再发一次（Flink 的重触发语义）
                context.count("late-refired");
                emit(target, output);
            }
            saveWindows(windows);
        }
    }

    @Override
    public void processWatermark(long watermark) {
        lastWatermark = watermark;
        Map<String, WindowState> windows = loadWindows();
        if (windows.isEmpty()) {
            return;
        }
        boolean changed = false;
        for (WindowState window : windows.values()) {
            if (!window.fired && window.end <= watermark) {
                window.fired = true;
                emit(window, currentOutput);
                changed = true;
            }
        }
        // 过期窗口：丢掉累加器（只留「已结算」标记用于判迟到），状态不至于无限涨
        for (WindowState window : windows.values()) {
            if (window.fired && window.end + allowedLateness < watermark && window.accumulators != null) {
                window.accumulators = null;
                changed = true;
            }
        }
        if (changed) {
            saveWindows(windows);
        }
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 窗口靠水位触发（processWatermark），不用定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 取（或建）某个键 + 窗口的状态。 */
    private WindowState window(Map<String, WindowState> windows, String groupKey, TimeWindow timeWindow) {
        String windowKey = windowKey(groupKey, timeWindow);
        WindowState state = windows.get(windowKey);
        if (state == null) {
            state = new WindowState(groupKey, timeWindow.start(), timeWindow.end());
            windows.put(windowKey, state);
        }
        return state;
    }

    /** 会话窗口：候选窗口与同组已有窗口相交 ⇒ 合并。 */
    private WindowState mergeSession(Map<String, WindowState> windows, String groupKey, TimeWindow candidate) {
        List<String> merged = new ArrayList<String>();
        WindowState result = new WindowState(groupKey, candidate.start(), candidate.end());
        for (Map.Entry<String, WindowState> entry : windows.entrySet()) {
            WindowState existing = entry.getValue();
            if (!groupKey.equals(existing.groupKey)) {
                continue;
            }
            if (existing.start < result.end && result.start < existing.end) {
                result.start = Math.min(result.start, existing.start);
                result.end = Math.max(result.end, existing.end);
                merged.add(entry.getKey());
                if (existing.accumulators != null) {
                    for (Map.Entry<String, Accumulator> acc : existing.accumulators.entrySet()) {
                        Accumulator target = result.accumulators.get(acc.getKey());
                        if (target == null) {
                            Accumulator source = acc.getValue();
                            target = new Accumulator(source.fn(), source.column(), source.alias());
                            result.accumulators.put(acc.getKey(), target);
                        }
                        target.merge(acc.getValue());
                    }
                }
                if (existing.fired) {
                    result.fired = true;
                }
                if (existing.keyValues != null) {
                    result.keyValues = existing.keyValues;
                }
            }
        }
        for (String key : merged) {
            windows.remove(key);
        }
        windows.put(windowKey(groupKey, new TimeWindow(result.start, result.end)), result);
        return result;
    }

    /** 累加一行到窗口状态。 */
    private void accumulate(WindowState state, Map<String, Object> row) {
        if (state.accumulators == null) {
            state.accumulators = new LinkedHashMap<String, Accumulator>();
        }
        for (AggregateDef def : defs) {
            Accumulator accumulator = state.accumulators.get(def.alias());
            if (accumulator == null) {
                accumulator = new Accumulator(def.fn(), def.column(), def.alias());
                state.accumulators.put(def.alias(), accumulator);
            }
            Object value = def.isCountStar() ? Integer.valueOf(1) : RowValues.get(row, def.column());
            accumulator.accumulate(value);
        }
    }

    /** 发一个窗口的聚合结果。 */
    private void emit(WindowState state, Output output) {
        if (output == null) {
            return;
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        if (state.keyValues != null) {
            row.putAll(state.keyValues);
        }
        if (state.accumulators != null) {
            for (Map.Entry<String, Accumulator> entry : state.accumulators.entrySet()) {
                row.put(entry.getKey(), entry.getValue().result());
            }
        }
        if (emitWindowBounds) {
            row.put("windowStart", Long.valueOf(state.start));
            row.put("windowEnd", Long.valueOf(state.end));
        }
        context.count("windows-fired");
        output.collect(new Record(row,
                state.end == Long.MAX_VALUE ? Record.NO_EVENT_TIME : state.end,
                System.currentTimeMillis(), state.groupKey));
    }

    /** 键列取值（结算时一起发出）。 */
    private Map<String, Object> keyValues(Map<String, Object> row) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        for (String column : keyColumns) {
            values.put(column, RowValues.get(row, column));
        }
        return values;
    }

    private static String windowKey(String groupKey, TimeWindow window) {
        return groupKey + "" + window.start() + "" + window.end();
    }

    @SuppressWarnings("unchecked")
    private Map<String, WindowState> loadWindows() {
        context.state().setCurrentKey("");
        StateBackend.ValueState state = context.state().valueState("windows");
        Map<String, WindowState> windows = (Map<String, WindowState>) state.value();
        return windows == null ? new LinkedHashMap<String, WindowState>() : windows;
    }

    private void saveWindows(Map<String, WindowState> windows) {
        context.state().setCurrentKey("");
        context.state().valueState("windows").update(windows);
    }

    /** 一个键 + 一个窗口的状态（可序列化）。 */
    static final class WindowState implements Serializable {

        private static final long serialVersionUID = 1L;

        final String groupKey;
        long start;
        long end;
        boolean fired;
        Map<String, Accumulator> accumulators = new LinkedHashMap<String, Accumulator>();
        Map<String, Object> keyValues;

        WindowState(String groupKey, long start, long end) {
            this.groupKey = groupKey;
            this.start = start;
            this.end = end;
        }
    }
}
