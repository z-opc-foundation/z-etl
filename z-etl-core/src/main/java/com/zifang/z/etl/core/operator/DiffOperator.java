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
import java.util.List;
import java.util.Map;

/**
 * 差异算子：**两流按键比对，产出变化流**（INSERT / UPDATE / DELETE）。
 *
 * <p>存量与批量 ETL 的「只搬变化」主力：当前流对基线流逐键比对，变化才发——
 * 不用 binlog 也能得到变更口径（Debezium 快照 diff / Flink changelog 的保守子集）。
 *
 * <pre>
 * - id: delta
 *   type: diff
 *   config:
 *     left: cur            # 当前流节点 id
 *     right: ref           # 基线流节点 id
 *     keys: [order_id]     # 对比键（两侧同名）
 *     compare: [amount, status]   # 参与比对的列；缺省 = 左行全部非键列
 *     opColumn: __op       # 操作标记列（INSERT / UPDATE / DELETE；缺省 __op）
 *     unchanged: drop      # 键在且值同：drop（缺省，不发）/ emit（带 __op=SAME 发出）
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li>⚠️ **物化算子**：两侧收齐才比对 —— 只适合有界数据；</li>
 *   <li>产出顺序 = 左流首次出现序（INSERT/UPDATE/SAME），随后右流首次出现序（DELETE）；</li>
 *   <li>值比对**数值归一**（{@code 1.0} 与 {@code "1"} 相等，BigDecimal 语义），其余按文本；</li>
 *   <li>UPDATE/INSERT 发**左行**（当前值）、DELETE 发**右行**（基线值），各带 op 列。</li>
 * </ul>
 *
 * @author zifang
 */
public class DiffOperator implements Operator {

    /** 操作标记：新增（键只在左流）。 */
    public static final String OP_INSERT = "INSERT";
    /** 操作标记：更新（键在两侧但值不同）。 */
    public static final String OP_UPDATE = "UPDATE";
    /** 操作标记：删除（键只在右流）。 */
    public static final String OP_DELETE = "DELETE";
    /** 操作标记：相同（unchanged=emit 时发出）。 */
    public static final String OP_SAME = "SAME";

    private final PipelineNode node;

    private String leftNode;
    private String rightNode;
    private List<String> keys;
    private List<String> compare;
    private String opColumn;
    private boolean emitUnchanged;
    private OperatorContext context;

    private final List<Record> leftBuffer = new ArrayList<Record>();
    private final List<Record> rightBuffer = new ArrayList<Record>();

    public DiffOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        leftNode = node.string("left");
        rightNode = node.string("right");
        keys = node.strings("keys");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("diff 节点 " + node.getId() + " 必须声明 keys（对比键，两侧同名）");
        }
        compare = node.strings("compare");
        opColumn = node.string("opColumn", "__op");
        emitUnchanged = "emit".equalsIgnoreCase(node.string("unchanged", "drop"));
    }

    @Override
    public void processElement(Record record, Output output) {
        processElement(record, output, null);
    }

    @Override
    public void processElement(Record record, Output output, String inputTag) {
        if (inputTag != null && inputTag.equals(rightNode) && !inputTag.equals(leftNode)) {
            rightBuffer.add(record);
            context.count("right-in");
            return;
        }
        leftBuffer.add(record);
        context.count("left-in");
    }

    @Override
    public void processWatermark(long watermark) {
        // 与水位无关；flush 统一比对
    }

    @Override
    public void flush(Output output) {
        Map<String, Record> leftBy = byKey(leftBuffer, "left");
        Map<String, Record> rightBy = byKey(rightBuffer, "right");
        List<String> compareColumns = compare.isEmpty() ? compareOf(leftBy, rightBy) : compare;
        for (Map.Entry<String, Record> entry : leftBy.entrySet()) {
            Record right = rightBy.get(entry.getKey());
            Map<String, Object> row = new LinkedHashMap<String, Object>(entry.getValue().row());
            if (right == null) {
                row.put(opColumn, OP_INSERT);
                output.collect(entry.getValue().withRow(row));
                context.count("inserted");
            } else if (differs(entry.getValue().row(), right.row(), compareColumns)) {
                row.put(opColumn, OP_UPDATE);
                output.collect(entry.getValue().withRow(row));
                context.count("updated");
            } else {
                context.count("unchanged");
                if (emitUnchanged) {
                    row.put(opColumn, OP_SAME);
                    output.collect(entry.getValue().withRow(row));
                }
            }
        }
        for (Map.Entry<String, Record> entry : rightBy.entrySet()) {
            if (!leftBy.containsKey(entry.getKey())) {
                Map<String, Object> row = new LinkedHashMap<String, Object>(entry.getValue().row());
                row.put(opColumn, OP_DELETE);
                output.collect(entry.getValue().withRow(row));
                context.count("deleted");
            }
        }
        leftBuffer.clear();
        rightBuffer.clear();
    }

    /** 比对列：显式声明优先；缺省 = 样例行（左流优先）的全部非键列。 */
    private List<String> compareOf(Map<String, Record> leftBy, Map<String, Record> rightBy) {
        Map<String, Object> sample = leftBy.isEmpty()
                ? (rightBy.isEmpty() ? new LinkedHashMap<String, Object>() : rightBy.values().iterator().next().row())
                : leftBy.values().iterator().next().row();
        List<String> columns = new ArrayList<String>();
        for (String column : sample.keySet()) {
            if (!keys.contains(column)) {
                columns.add(column);
            }
        }
        return columns;
    }

    /** 两侧行按键索引（保持首次出现序）；键列缺失直接报错（不静默丢）。 */
    private Map<String, Record> byKey(List<Record> buffer, String side) {
        Map<String, Record> byKey = new LinkedHashMap<String, Record>();
        for (Record record : buffer) {
            String key = Keys.compute(record.row(), keys);
            if (key == null) {
                throw new IllegalStateException("diff 节点 " + node.getId() + " 的 " + side + " 侧有行算不出对比键");
            }
            byKey.put(key, record);
        }
        return byKey;
    }

    /** 比对两行的声明列（同键才走到这）：数值归一比，其余文本比。 */
    private boolean differs(Map<String, Object> left, Map<String, Object> right, List<String> columns) {
        for (String column : columns) {
            Object leftValue = left.get(column);
            Object rightValue = right.get(column);
            if (!sameValue(leftValue, rightValue)) {
                return true;
            }
        }
        return false;
    }

    /** 值相等：null == null；数值走 BigDecimal（1.0 ≡ "1"）；其余按文本。 */
    private static boolean sameValue(Object left, Object right) {
        if (left == null && right == null) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        BigDecimal leftNumber = Accumulator.toBigDecimal(left);
        BigDecimal rightNumber = Accumulator.toBigDecimal(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber) == 0;
        }
        return String.valueOf(left).equals(String.valueOf(right));
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
