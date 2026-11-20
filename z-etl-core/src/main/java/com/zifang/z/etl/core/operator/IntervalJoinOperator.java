package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.StateBackend;
import com.zifang.z.etl.core.util.Durations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 区间连接算子：左右两路按「关联键 + 事件时间区间」关联（Flink 的 {@code IntervalJoin} 口径）。
 *
 * <p>与普通 {@code join}（构建侧 / 探测侧，右表只留最新一条）的区别：
 * <b>两边都缓存、按事件时间配对</b> —— 两条流上事件时间相差不超过 {@code within} 的行两两配对，
 * 每对只发一次（后到的那条触发）。典型场景：订单流 × 物流轨迹流按「下单后 5 分钟内」关联。
 *
 * <pre>
 * - id: ij
 *   type: interval.join
 *   config:
 *     left: orders            # 上游节点 id
 *     right: tracks           # 上游节点 id
 *     leftKey: [order_id]
 *     rightKey: [order_id]
 *     within: 5m              # 事件时间之差的上限（Durations 口径）
 *     joinType: inner         # inner / left（左侧没命中也发，右侧列补 null）
 *     rightSuffix: _right
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li><b>必须带事件时间</b>：没有事件时间的行没法配对 ⇒ 丢弃并计数（不静默装作关联上）；</li>
 *   <li><b>水位到期清理</b>：事件时间早于「水位 − within」的缓存行再也等不到配对 ⇒ 清掉
 *       （内存只随「窗口内流量」涨，不无界增长）；</li>
 *   <li><b>迟到重发不适用</b>：{@code allowedLateness} 那套是窗口算子的口径，
 *       区间连接的配对发生在行到达时，过期缓存的旧配对不再补发；</li>
 *   <li><b>状态可序列化</b>：缓存行进键控状态 ⇒ 能进检查点（断点续传口径与其它有状态算子一致）。</li>
 * </ul>
 *
 * @author zifang
 */
public class IntervalJoinOperator implements Operator {

    /** 活跃键清单存放的哨兵键（不与真实关联键碰撞）。 */
    static final String KEY_SET_KEY = "\u0000interval-join-active-keys";

    private final PipelineNode node;

    private String leftNode;
    private String rightNode;
    private List<String> leftKey = new ArrayList<String>();
    private List<String> rightKey = new ArrayList<String>();
    private long withinMs;
    private boolean leftOuter;
    private String rightSuffix = "_right";

    private OperatorContext context;
    private Set<String> rightColumns = new LinkedHashSet<String>();

    /**
     * @param node 节点声明
     */
    public IntervalJoinOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        leftNode = node.string("left");
        rightNode = node.string("right");
        leftKey = node.strings("leftKey");
        rightKey = node.strings("rightKey");
        if (leftKey.isEmpty() || rightKey.isEmpty()) {
            throw new IllegalArgumentException("interval.join 节点 " + node.getId()
                    + " 必须声明 leftKey 与 rightKey");
        }
        withinMs = Durations.parseMillis(node.string("within"), -1L);
        if (withinMs <= 0L) {
            throw new IllegalArgumentException("interval.join 节点 " + node.getId()
                    + " 必须声明 within（如 5m / 30s / 500ms）");
        }
        String type = node.string("joinType", "inner").toLowerCase();
        if (!"inner".equals(type) && !"left".equals(type)) {
            throw new IllegalArgumentException("interval.join 节点 " + node.getId()
                    + " 的 joinType 只支持 inner/left，实际: " + type);
        }
        leftOuter = "left".equals(type);
        rightSuffix = node.string("rightSuffix", "_right");
        if (ctx.state() == null) {
            throw new IllegalStateException("interval.join 节点 " + node.getId()
                    + " 需要键控状态，但没有分配到状态存储");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        processElement(record, output, null);
    }

    @Override
    public void processElement(Record record, Output output, String inputTag) {
        if (!record.hasEventTime()) {
            context.count("no-event-time-dropped");
            return;
        }
        boolean fromLeft = isLeft(inputTag, record);
        List<String> keyColumns = fromLeft ? leftKey : rightKey;
        String key = Keys.compute(record.row(), keyColumns);
        if (key == null) {
            throw new IllegalStateException("interval.join 节点 " + node.getId()
                    + " 的关联键列为空，算不出关联键");
        }
        long eventTime = record.eventTime();

        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        List<Object> pending = pendingList(store);
        List<Pending> entries = decode(pending);

        // 与对侧缓存里的行两两配对（后到的那条触发，每对只发一次）
        int matches = 0;
        List<Pending> kept = new ArrayList<Pending>();
        for (Pending entry : entries) {
            if (entry.left != fromLeft && Math.abs(entry.eventTime - eventTime) <= withinMs) {
                Map<String, Object> merged = fromLeft
                        ? merge(record.row(), entry.row)
                        : merge(entry.row, record.row());
                output.collect(record.withRow(merged));
                matches++;
                context.count("matched");
            }
            kept.add(entry);
        }

        if (matches == 0 && fromLeft && leftOuter) {
            output.collect(record.withRow(merge(record.row(), null)));
            context.count("unmatched-left");
        }
        if (matches == 0 && !fromLeft) {
            context.count("unmatched-right");
        }

        // 自己也进缓存，等对面的后来配
        kept.add(new Pending(fromLeft, eventTime, new LinkedHashMap<String, Object>(record.row())));
        if (!fromLeft) {
            rightColumns.addAll(record.row().keySet());
        }
        // 先写缓存（当前 key 还是关联键），再记活跃键（它会切走当前 key）
        store.listState("pending").update(encode(kept));
        rememberKey(store, key);
    }

    /** 水位推进：事件时间早于「水位 − within」的缓存行再也等不到配对 ⇒ 清掉。 */
    @Override
    public void processWatermark(long watermark) {
        long expireBefore = watermark - withinMs;
        if (expireBefore <= 0L) {
            return;
        }
        StateBackend.KeyedStateStore store = context.state();
        Set<String> activeKeys = activeKeys(store);
        long expired = 0L;
        for (String key : new LinkedHashSet<String>(activeKeys)) {
            store.setCurrentKey(key);
            List<Pending> entries = decode(pendingList(store));
            List<Pending> kept = new ArrayList<Pending>();
            for (Pending entry : entries) {
                if (entry.eventTime < expireBefore) {
                    expired++;
                    continue;
                }
                kept.add(entry);
            }
            if (kept.isEmpty()) {
                store.listState("pending").clear();
                activeKeys.remove(key);
            } else {
                store.listState("pending").update(encode(kept));
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
        // 区间连接不涉定时器（配对在行到达时发生，清理靠水位）
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 判断这条记录来自左侧还是右侧（输入标签优先；没有标签时按是否命中左键列判断）。 */
    private boolean isLeft(String inputTag, Record record) {
        if (inputTag != null) {
            if (leftNode != null && leftNode.equals(inputTag)) {
                return true;
            }
            if (rightNode != null && rightNode.equals(inputTag)) {
                return false;
            }
        }
        return record.row().containsKey(leftKey.get(0));
    }

    /** 拼行：对侧列与本侧冲突时加后缀。 */
    private Map<String, Object> merge(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> merged = new LinkedHashMap<String, Object>(left);
        if (right == null) {
            for (String column : rightColumns) {
                merged.put(column + rightSuffix, null);
            }
            return merged;
        }
        for (Map.Entry<String, Object> entry : right.entrySet()) {
            String column = entry.getKey();
            if (merged.containsKey(column)) {
                merged.put(column + rightSuffix, entry.getValue());
            } else {
                merged.put(column, entry.getValue());
            }
        }
        return merged;
    }

    /** 当前 key 的缓存行（没有返回空表）。 */
    private List<Object> pendingList(StateBackend.KeyedStateStore store) {
        return store.listState("pending").get();
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

    /** 缓存行反序列化（兼容旧值形态）。 */
    @SuppressWarnings("unchecked")
    private List<Pending> decode(List<Object> raw) {
        List<Pending> result = new ArrayList<Pending>();
        for (Object item : raw) {
            if (item instanceof Pending) {
                result.add((Pending) item);
            }
        }
        return result;
    }

    /** 缓存行序列化。 */
    private List<Object> encode(List<Pending> entries) {
        return new ArrayList<Object>(entries);
    }

    /** 一条缓存行（可序列化 ⇒ 能进检查点）。 */
    private static final class Pending implements Serializable {

        private static final long serialVersionUID = 1L;

        private final boolean left;
        private final long eventTime;
        private final Map<String, Object> row;

        Pending(boolean left, long eventTime, Map<String, Object> row) {
            this.left = left;
            this.eventTime = eventTime;
            this.row = row;
        }
    }
}
