package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 全行去重算子：整行（或列子集）第一次出现放行，重复的丢掉。
 *
 * <pre>
 * - id: uniq
 *   type: distinct
 *   config:
 *     columns: [user, tag]   # 可选：按列子集判重（缺省整行）
 *     cap: 100000            # 可选：已见指纹上限（防内存撑爆，缺省 100000）
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li><b>判重键</b>：缺省整行所有列拼成指纹；声明 {@code columns} 就只按这几列；</li>
 *   <li><b>上限兜底</b>：指纹数超过 {@code cap} 后新行不再跟踪、**照常放行**并计数
 *       （{@code cap-overflow}）—— 宁可多放不可漏放，与 CEP 的保守口径同线；
 *       要严格去重就把 cap 调大或在上游先收窄；</li>
 *   <li>内存 set，不进键控状态（与 {@code topn} 的批缓存同款取舍：检查点不背）；</li>
 *   <li>行原样放行或丢掉（只计数，不改行）。</li>
 * </ul>
 *
 * @author zifang
 */
public class DistinctOperator implements Operator {

    private static final int DEFAULT_CAP = 100000;

    private final PipelineNode node;

    private List<String> columns = new ArrayList<String>();
    private int cap = DEFAULT_CAP;

    private final Set<String> seen = new HashSet<String>();

    private OperatorContext context;

    public DistinctOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        columns = node.strings("columns");
        cap = node.integer("cap", DEFAULT_CAP);
        if (cap < 1) {
            throw new IllegalArgumentException("distinct 节点 " + node.getId() + " 的 cap 至少为 1");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        String fingerprint = fingerprint(record.row());
        if (seen.contains(fingerprint)) {
            context.count("duplicates");
            return;
        }
        if (seen.size() < cap) {
            seen.add(fingerprint);
        } else {
            // 上限兜底：不再跟踪，但行照常放行（宁多放不漏放）
            context.count("cap-overflow");
        }
        output.collect(record);
    }

    /** 判重指纹：指定列（或整行）按列名序拼接；null 记作哨兵。 */
    private String fingerprint(Map<String, Object> row) {
        if (columns.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                sb.append(entry.getKey()).append('\u0000')
                        .append(entry.getValue() == null ? "\u0001null" : String.valueOf(entry.getValue()))
                        .append('\u0002');
            }
            return sb.toString();
        }
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            Object value = row.get(column);
            sb.append(column).append('\u0000')
                    .append(value == null ? "\u0001null" : String.valueOf(value))
                    .append('\u0002');
        }
        return sb.toString();
    }

    @Override
    public void processWatermark(long watermark) {
        // 不碰水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        seen.clear();
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
