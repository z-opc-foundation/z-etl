package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 集合运算算子：**EXCEPT（差集）/ INTERSECT（交集）**（SQL 集合运算的流式版，物化口径）。
 *
 * <p>与 {@code diff} 的分工：diff 要「变化方向」（产出带 {@code __op}），except/intersect
 * 只要「集合关系」——行原样发，不打标记：
 * <ul>
 *   <li>{@code except}：左流有、右流没有的行（发左行原样）；</li>
 *   <li>{@code intersect}：两侧都有的行（发左行原样）。</li>
 * </ul>
 *
 * <pre>
 * - id: only-left
 *   type: except
 *   config:
 *     left: a
 *     right: b
 *     keys: [order_id]     # 可选：按键比（缺省整行指纹比对）
 * </pre>
 *
 * <p>口径：⚠️ **物化**（两侧收齐才比，只适合有界数据）；产出 = 左流首次出现序；
 * 比对口径**值归一**（{@code 1.0} ≡ {@code "1"}，BigDecimal 语义；缺省整行时逐列比）；
 * 同键多行：整行指纹集合判等（右流行集合完全含盖才算「有」）。
 *
 * @author zifang
 */
public class SetOpOperator implements Operator {

    private final PipelineNode node;
    private final boolean intersectMode;

    private String leftNode;
    private String rightNode;
    private List<String> keys;
    private OperatorContext context;

    private final List<Record> leftBuffer = new ArrayList<Record>();
    private final List<Record> rightBuffer = new ArrayList<Record>();

    public SetOpOperator(PipelineNode node, boolean intersectMode) {
        this.node = node;
        this.intersectMode = intersectMode;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        leftNode = node.string("left");
        rightNode = node.string("right");
        keys = node.strings("keys");
    }

    @Override
    public void processElement(Record record, Output output) {
        processElement(record, output, null);
    }

    @Override
    public void processElement(Record record, Output output, String inputTag) {
        if (inputTag != null && inputTag.equals(rightNode) && !inputTag.equals(leftNode)) {
            rightBuffer.add(record);
            return;
        }
        leftBuffer.add(record);
    }

    @Override
    public void processWatermark(long watermark) {
        // 与水位无关；flush 统一比
    }

    @Override
    public void flush(Output output) {
        Set<String> rightFingerprints = new LinkedHashSet<String>();
        for (Record record : rightBuffer) {
            rightFingerprints.add(fingerprint(record.row()));
        }
        for (Record record : leftBuffer) {
            boolean inRight = rightFingerprints.contains(fingerprint(record.row()));
            boolean emit = intersectMode ? inRight : !inRight;
            if (emit) {
                output.collect(record);
                context.count("emitted");
            } else {
                context.count("filtered");
            }
        }
        leftBuffer.clear();
        rightBuffer.clear();
    }

    /** 行指纹：声明了 keys 按键值；缺省整行逐列（列名序 + 值归一文本）。 */
    private String fingerprint(Map<String, Object> row) {
        if (!keys.isEmpty()) {
            String key = Keys.compute(row, keys);
            if (key == null) {
                throw new IllegalStateException((intersectMode ? "intersect" : "except")
                        + " 节点 " + node.getId() + " 有行算不出对比键");
            }
            return "k:" + key;
        }
        StringBuilder sb = new StringBuilder("r:");
        for (Map.Entry<String, Object> entry : new LinkedHashMap<String, Object>(row).entrySet()) {
            sb.append(entry.getKey()).append('=').append(normalized(entry.getValue())).append('\u0001');
        }
        return sb.toString();
    }

    /** 值归一：数值统一 BigDecimal 规范形（stripTrailingZeros，"100" ≡ "100.0"），其余原文本。 */
    private static String normalized(Object value) {
        if (value == null) {
            return "\u0000";
        }
        BigDecimal numeric = Accumulator.toBigDecimal(value);
        return numeric != null ? numeric.stripTrailingZeros().toPlainString() : String.valueOf(value);
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        leftBuffer.clear();
        rightBuffer.clear();
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
