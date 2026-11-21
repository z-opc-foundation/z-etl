package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.StateBackend;
import com.zifang.z.etl.core.util.RowValues;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 键控聚合算子（无窗口）：按键维护滚动累计值，每来一条就发一次当前累计值。
 *
 * <p>语义照抄 Kafka Streams 的 {@code groupBy().aggregate()}：
 * 「键变了就发一次新的累计值」—— 下游若按键 upsert，落地表里每个键始终是最新累计值。
 *
 * <p>状态：{@code valueState("aggregate")} 里存 {@code 别名 → 累加器} 的映射（可序列化 ⇒ 能进检查点）。
 *
 * <pre>
 * - id: agg
 *   type: aggregate
 *   config:
 *     keyBy: [status]
 *     aggregates:
 *       - {fn: COUNT, column: "*",   alias: order_cnt}
 *       - {fn: SUM,   column: amount, alias: total_amount}
 * </pre>
 *
 * @author zifang
 */
public class AggregateOperator implements Operator {

    private final PipelineNode node;

    private List<String> keyColumns = new ArrayList<String>();
    private List<AggregateDef> defs = new ArrayList<AggregateDef>();
    private OperatorContext context;

    public AggregateOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(OperatorContext ctx) {
        this.context = ctx;
        keyColumns = node.strings("keyBy");
        if (keyColumns.isEmpty()) {
            throw new IllegalArgumentException("aggregate 节点 " + node.getId() + " 必须声明 keyBy");
        }
        defs = new ArrayList<AggregateDef>();
        for (Map<String, Object> item : node.mapList("aggregates")) {
            defs.add(AggregateDef.from(item));
        }
        if (defs.isEmpty()) {
            throw new IllegalArgumentException("aggregate 节点 " + node.getId() + " 必须声明 aggregates");
        }
        if (ctx.state() == null) {
            throw new IllegalStateException("aggregate 节点 " + node.getId() + " 需要键控状态，但没有分配到状态存储");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(Record record, Output output) {
        String key = Keys.compute(record.row(), keyColumns);
        if (key == null) {
            throw new IllegalStateException("aggregate 节点 " + node.getId() + " 的 keyBy 列为空，算不出分组键");
        }
        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        StateBackend.ValueState state = store.valueState("aggregate");
        Map<String, Accumulator> accumulators = (Map<String, Accumulator>) state.value();
        if (accumulators == null) {
            accumulators = new LinkedHashMap<String, Accumulator>();
        }
        for (AggregateDef def : defs) {
            Accumulator accumulator = accumulators.get(def.alias());
            if (accumulator == null) {
                accumulator = new Accumulator(def.fn(), def.column(), def.alias());
                accumulators.put(def.alias(), accumulator);
            }
            Object value = def.isCountStar() ? Integer.valueOf(1) : RowValues.get(record.row(), def.column());
            accumulator.accumulate(value);
        }
        state.update(accumulators);

        Map<String, Object> result = new LinkedHashMap<String, Object>(record.row());
        for (AggregateDef def : defs) {
            Accumulator accumulator = accumulators.get(def.alias());
            result.put(def.alias(), accumulator == null ? null : accumulator.result());
            if (accumulator != null) {
                long dropped = accumulator.collectDroppedDelta();
                if (dropped > 0L) {
                    context.count("collect-truncated", dropped);
                }
            }
        }
        context.count("emitted");
        output.collect(record.withRow(result));
    }

    @Override
    public void processWatermark(long watermark) {
        // 无窗口：水位不影响滚动聚合
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
}
