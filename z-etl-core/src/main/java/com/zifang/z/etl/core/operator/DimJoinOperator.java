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
 * 维表关联算子（{@code dim.join}，Flink broadcast-state 关维表的口径）。
 *
 * <p>与普通 {@code join} 的三点区别（维表场景的专属语义）：
 * <ul>
 *   <li><b>热更新</b>：右流（维表流）同一键再来一条就**替换**旧条目——维表变更重发即生效；</li>
 *   <li><b>右流只进状态不下传</b>：维表行是广播进来的，不是要输出的数据；</li>
 *   <li><b>TTL 过期</b>：声明 {@code ttl} 时，事件时间早于「水位 − ttl」的维表条目清掉
 *       （维表不再续发 ⇒ 过了保鲜期的关联宁可断掉，也不拿陈年旧值硬拼）。</li>
 * </ul>
 *
 * <pre>
 * - id: enrich
 *   type: dim.join
 *   config:
 *     left: orders            # 主流（探测侧）
 *     right: dimCities        # 维表流（广播侧）
 *     leftKey: [city_id]
 *     rightKey: [id]
 *     select: [city_name, province]   # 维表列投影（不声明 = 全列）
 *     ttl: 1h                 # 维表条目保鲜期（Durations 口径；不声明 = 永不过期）
 *     joinType: inner         # inner / left
 * </pre>
 *
 * @author zifang
 */
public class DimJoinOperator implements Operator {

    /** 活跃键清单存放的哨兵键（不与真实关联键碰撞）。 */
    static final String KEY_SET_KEY = "\u0000dim-join-active-keys";

    private final PipelineNode node;

    private String leftNode;
    private String rightNode;
    private List<String> leftKey = new ArrayList<String>();
    private List<String> rightKey = new ArrayList<String>();
    private List<String> select = new ArrayList<String>();
    private long ttlMs;
    private boolean leftOuter;
    private String rightSuffix = "_right";

    private OperatorContext context;
    private Set<String> rightColumns = new LinkedHashSet<String>();

    /**
     * @param node 节点声明
     */
    public DimJoinOperator(PipelineNode node) {
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
            throw new IllegalArgumentException("dim.join 节点 " + node.getId()
                    + " 必须声明 leftKey 与 rightKey");
        }
        select = node.strings("select");
        ttlMs = Durations.parseMillis(node.string("ttl"), 0L);
        String type = node.string("joinType", "inner").toLowerCase();
        if (!"inner".equals(type) && !"left".equals(type)) {
            throw new IllegalArgumentException("dim.join 节点 " + node.getId()
                    + " 的 joinType 只支持 inner/left，实际: " + type);
        }
        leftOuter = "left".equals(type);
        rightSuffix = node.string("rightSuffix", "_right");
        if (ctx.state() == null) {
            throw new IllegalStateException("dim.join 节点 " + node.getId()
                    + " 需要键控状态，但没有分配到状态存储");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        processElement(record, output, null);
    }

    @Override
    public void processElement(Record record, Output output, String inputTag) {
        boolean fromLeft = isLeft(inputTag, record);
        if (fromLeft) {
            probe(record, output);
        } else {
            update(record);
        }
    }

    /** 主流（探测侧）：查维表状态，命中就拼行。 */
    private void probe(Record record, Output output) {
        String key = Keys.compute(record.row(), leftKey);
        if (key == null) {
            throw new IllegalStateException("dim.join 节点 " + node.getId()
                    + " 的 leftKey 列为空，算不出关联键");
        }
        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        Object value = store.mapState("dim").get(key);
        if (value instanceof Dim) {
            Dim dim = (Dim) value;
            output.collect(record.withRow(merge(record.row(), dim.row)));
            context.count("matched");
        } else if (leftOuter) {
            output.collect(record.withRow(merge(record.row(), null)));
            context.count("unmatched-left");
        } else {
            context.count("unmatched-dropped");
        }
    }

    /** 维表流（广播侧）：同键替换（热更新），带投影；声明 ttl 时必须带事件时间。 */
    private void update(Record record) {
        String key = Keys.compute(record.row(), rightKey);
        if (key == null) {
            throw new IllegalStateException("dim.join 节点 " + node.getId()
                    + " 的 rightKey 列为空，算不出关联键");
        }
        long versionTime = 0L;
        if (ttlMs > 0L) {
            if (!record.hasEventTime()) {
                context.count("no-event-time-dropped");
                return;
            }
            versionTime = record.eventTime();
        }
        Map<String, Object> projected = project(record.row());
        StateBackend.KeyedStateStore store = context.state();
        // 先写维表条目（当前 key 还是关联键），再记活跃键（它会切走当前 key）
        store.setCurrentKey(key);
        store.mapState("dim").put(key, new Dim(versionTime, projected));
        rightColumns.addAll(projected.keySet());
        context.count("dim-updated");
        if (ttlMs > 0L) {
            rememberKey(store, key);
        }
    }

    /** 水位推进：事件时间早于「水位 − ttl」的维表条目过期清掉。 */
    @Override
    public void processWatermark(long watermark) {
        if (ttlMs <= 0L) {
            return;
        }
        long expireBefore = watermark - ttlMs;
        if (expireBefore <= 0L) {
            return;
        }
        StateBackend.KeyedStateStore store = context.state();
        Set<String> activeKeys = activeKeys(store);
        long expired = 0L;
        for (String key : new LinkedHashSet<String>(activeKeys)) {
            store.setCurrentKey(key);
            Object value = store.mapState("dim").get(key);
            if (value instanceof Dim && ((Dim) value).eventTime < expireBefore) {
                store.mapState("dim").remove(key);
                activeKeys.remove(key);
                expired++;
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
        // 维表关联不涉定时器（过期靠水位）
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 维表列投影（select 声明之外的列不进状态）。 */
    private Map<String, Object> project(Map<String, Object> row) {
        if (select.isEmpty()) {
            return new LinkedHashMap<String, Object>(row);
        }
        Map<String, Object> projected = new LinkedHashMap<String, Object>();
        for (String column : select) {
            projected.put(column, row.get(column));
        }
        return projected;
    }

    /** 判断这条记录来自主流还是维表流（输入标签优先；没有标签时按是否命中左键列判断）。 */
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

    /** 拼行：维表列与主流冲突时加后缀；维表没命中时投影列补 null。 */
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

    /** 记下活跃键（带 ttl 时水位的全局清理靠它遍历）。 */
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

    /** 一条维表条目（可序列化 ⇒ 能进检查点）。 */
    private static final class Dim implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long eventTime;
        private final Map<String, Object> row;

        Dim(long eventTime, Map<String, Object> row) {
            this.eventTime = eventTime;
            this.row = row;
        }
    }
}
