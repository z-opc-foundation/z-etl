package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 行号算子：给每行编一个序号列（SQL {@code ROW_NUMBER()} 无排序版的流式口径）。
 *
 * <pre>
 * - id: numbered
 *   type: index
 *   config:
 *     name: row_num     # 序号列名（缺省 row_num）
 *     start: 1          # 起始值（缺省 1）
 *     keyBy: [user_id]  # 可选：按键各自编号（每键从 start 重新起算）；缺省全局一路编号
 * </pre>
 *
 * <p>口径：编号按**到达序**（引擎单线程定序确定 ⇒ 可复现）；行原样 + 序号列；
 * 无键控状态（编号器是进程内计数，不进检查点——重跑从头编号，与「到达序」口径自洽）。
 *
 * @author zifang
 */
public class IndexOperator implements Operator {

    private final PipelineNode node;

    private String name;
    private long start;
    private List<String> keyColumns;

    private long globalCounter;
    private final Map<String, Long> keyedCounters = new LinkedHashMap<String, Long>();
    private OperatorContext context;

    public IndexOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        name = node.string("name", "row_num");
        start = node.longValue("start", 1L);
        keyColumns = node.strings("keyBy");
        globalCounter = start;
        keyedCounters.clear();
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> row = new LinkedHashMap<String, Object>(record.row());
        if (keyColumns.isEmpty()) {
            row.put(name, globalCounter);
            globalCounter++;
        } else {
            String key = Keys.compute(record.row(), keyColumns);
            Long counter = keyedCounters.get(key);
            if (counter == null) {
                counter = Long.valueOf(start);
            } else {
                counter = Long.valueOf(counter.longValue() + 1L);
            }
            keyedCounters.put(key, counter);
            row.put(name, counter);
        }
        output.collect(record.withRow(row));
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态，不碰水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        keyedCounters.clear();
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
