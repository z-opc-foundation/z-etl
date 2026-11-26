package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.OutputTag;
import com.zifang.z.etl.core.runtime.Record;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 画像算子：**行原样透传，流尾按列出画像**（Spark {@code describe} / pandas
 * {@code describe()} 的流式口径）。
 *
 * <p>插在链上任意一点不改变数据（行原样过），流结束时把每列的统计收成**画像行**
 * 走旁路输出给 {@code reportTo} 指名的节点（没声明就只落节点指标）：
 *
 * <pre>
 * - id: stats
 *   type: profile
 *   config:
 *     reportTo: profile-sink     # 画像行去处（旁路标签；省略 = 只进节点指标）
 *     distinctCap: 10000         # 去重计数的上限（防高基数列撑内存；超出计数 distinct-cap）
 * </pre>
 *
 * <p>画像行形状（每列一行）：{@code column / rows / nulls / distinct / min / max / mean}。
 * min/max 数值按数值比、其余按文本比；mean 只对数值列算（BigDecimal，4 位小数）；
 * distinct 超上限后不再计数（宁可少报不撑爆）。
 *
 * @author zifang
 */
public class ProfileOperator implements Operator {

    private final PipelineNode node;

    private OutputTag reportTag;
    private int distinctCap;
    private OperatorContext context;

    /** 列画像（首次出现序）。 */
    private final Map<String, ColStat> stats = new LinkedHashMap<String, ColStat>();

    public ProfileOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        String reportTo = node.string("reportTo");
        if (reportTo != null) {
            reportTag = new OutputTag(reportTo);
        }
        distinctCap = node.integer("distinctCap", 10000);
        if (distinctCap < 1) {
            throw new IllegalArgumentException("profile 节点 " + node.getId() + " 的 distinctCap 至少为 1");
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        context.count("rows");
        for (Map.Entry<String, Object> entry : record.row().entrySet()) {
            ColStat stat = stats.get(entry.getKey());
            if (stat == null) {
                stat = new ColStat();
                stats.put(entry.getKey(), stat);
            }
            stat.observe(entry.getValue());
        }
        output.collect(record);
    }

    @Override
    public void processWatermark(long watermark) {
        // 透传不碰水位
    }

    @Override
    public void flush(Output output) {
        if (reportTag == null) {
            context.count("profiled-columns", stats.size());
            return;
        }
        for (Map.Entry<String, ColStat> entry : stats.entrySet()) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            ColStat stat = entry.getValue();
            row.put("column", entry.getKey());
            row.put("rows", stat.rows);
            row.put("nulls", stat.nulls);
            row.put("distinct", stat.distinct.size());
            row.put("min", stat.min);
            row.put("max", stat.max);
            row.put("mean", stat.mean());
            output.collect(reportTag, Record.of(row));
        }
        context.count("profiled-columns", stats.size());
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        stats.clear();
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 单列统计。 */
    private final class ColStat {

        private long rows;
        private long nulls;
        private final Set<Object> distinct = new LinkedHashSet<Object>();
        private Object min;
        private Object max;
        private BigDecimal sum;
        private long numericCount;

        void observe(Object value) {
            rows++;
            if (value == null) {
                nulls++;
                return;
            }
            Object normalized = normalize(value);
            if (distinct.size() < distinctCap) {
                distinct.add(normalized);
            } else {
                context.count("distinct-cap");
            }
            if (min == null) {
                min = normalized;
                max = normalized;
            } else {
                if (compare(normalized, min) < 0) {
                    min = normalized;
                }
                if (compare(normalized, max) > 0) {
                    max = normalized;
                }
            }
            BigDecimal numeric = Accumulator.toBigDecimal(value);
            if (numeric != null) {
                sum = sum == null ? numeric : sum.add(numeric);
                numericCount++;
            }
        }

        /** 数值均值（4 位小数，HALF_UP）；无数值返回 null。 */
        Object mean() {
            if (sum == null || numericCount == 0L) {
                return null;
            }
            return sum.divide(BigDecimal.valueOf(numericCount), 4, RoundingMode.HALF_UP);
        }
    }

    /** 值归一（Accumulator 同口径）：数值转 BigDecimal，其余原样 / 文本。 */
    private static Object normalize(Object value) {
        if (value instanceof BigDecimal || value instanceof Boolean || value instanceof String
                || value instanceof java.util.Date || value instanceof byte[]) {
            return value;
        }
        BigDecimal numeric = Accumulator.toBigDecimal(value);
        if (numeric != null) {
            return numeric;
        }
        return String.valueOf(value);
    }

    /** 比较：数值按数值比，其余按字符串比。 */
    private static int compare(Object left, Object right) {
        BigDecimal leftNumber = Accumulator.toBigDecimal(left);
        BigDecimal rightNumber = Accumulator.toBigDecimal(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }
}
