package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.StateBackend;
import com.zifang.z.etl.core.util.Durations;

import java.util.Map;

/**
 * 去重算子：按键保留首条 / 末条。
 *
 * <p>口径照抄 Flink 的去重套路（{@code KeyedProcessFunction} + 状态）：
 * <ul>
 *   <li>{@code keep: first}（默认）：同一键只发**第一条**，后续同键的行被丢弃并计数；</li>
 *   <li>{@code keep: last}：每条都发（下游按幂等键 upsert ⇒ 最终落地的是末条）。</li>
 * </ul>
 *
 * <p>状态：{@code valueState("dedup")} 里存「键 → 行指纹」。指纹相同 ⇒ 完全重复；
 * 指纹不同 ⇒ 同键的新版本（{@code keep: first} 口径下也照样丢弃，只计数）。
 *
 * @author zifang
 */
public class DedupOperator implements Operator {

    private final PipelineNode node;

    private java.util.List<String> keyColumns = new java.util.ArrayList<String>();
    private boolean keepFirst = true;
    private long ttl;
    private OperatorContext context;

    public DedupOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        keyColumns = node.strings("keyBy");
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException("dedup 节点 " + node.getId() + " 必须声明 keyBy");
        }
        String keep = node.string("keep", "first").toLowerCase();
        keepFirst = !"last".equals(keep);
        ttl = Durations.parseMillis(node.string("ttl"), 0L);
        if (ctx.state() == null) {
            throw new IllegalStateException("dedup 节点 " + node.getId() + " 需要键控状态，但没有分配到状态存储");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        String key = Keys.compute(record.row(), keyColumns);
        if (key == null) {
            throw new IllegalStateException("dedup 节点 " + node.getId() + " 的 keyBy 列为空，算不出分组键");
        }
        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        StateBackend.ValueState state = store.valueState("dedup");
        String fingerprint = fingerprint(record.row());
        Object seen = state.value();
        boolean duplicate = seen != null && fingerprint.equals(String.valueOf(seen));
        if (seen == null) {
            state.update(fingerprint);
            if (ttl > 0L && record.hasEventTime() && context.timers() != null) {
                context.timers().registerEventTimeTimer(record.eventTime() + ttl, "dedup:" + key);
            }
        } else if (duplicate) {
            context.count("duplicates");
        } else {
            context.count("same-key-new-version");
        }
        if (keepFirst) {
            if (seen != null) {
                context.count("dropped");
                return;
            }
            output.collect(record);
        } else {
            output.collect(record);
        }
    }

    @Override
    public void processWatermark(long watermark) {
        // 去重不涉水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // TTL 到期：清掉这个键的去重标记（之后同键的行又能过去了）
        if (timerName == null || !timerName.startsWith("dedup:")) {
            return;
        }
        String key = timerName.substring("dedup:".length());
        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        store.valueState("dedup").clear();
        context.count("ttl-expired");
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 行指纹（列名 + 值的稳定摘要；只看值，不看列序）。 */
    private static String fingerprint(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        java.util.List<String> columns = new java.util.ArrayList<String>(row.keySet());
        java.util.Collections.sort(columns);
        for (String column : columns) {
            Object value = row.get(column);
            sb.append(column).append('=').append(value == null ? "null" : String.valueOf(value)).append(';');
        }
        return sb.toString();
    }
}
