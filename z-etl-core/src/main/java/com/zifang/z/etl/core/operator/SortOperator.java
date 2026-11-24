package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 排序算子：按声明列序整流排序（列名前缀 {@code -} 表示降序）。
 *
 * <p>⚠️ 物化算子：要收齐全流才能出第一行 —— 只适合有界数据（文件 / 批式源），
 * 无限流上用它会一直憋着不出数。
 *
 * <pre>
 * - id: sort
 *   type: sort
 *   config:
 *     columns: [city, -amount]    # city 升序、amount 降序
 * </pre>
 *
 * @author zifang
 */
public class SortOperator implements Operator {

    private final PipelineNode node;
    private final List<String> columns;

    private List<Record> buffer;
    private int cursor;

    public SortOperator(PipelineNode node) {
        this.node = node;
        this.columns = node.strings("columns");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("sort 节点 " + node.getId() + " 缺少 columns");
        }
    }

    @Override
    public void open(OperatorContext context) {
        buffer = new ArrayList<Record>();
        cursor = 0;
    }

    @Override
    public void processElement(Record record, Output output) {
        buffer.add(record);
    }

    @Override
    public void processWatermark(long watermark) {
        // 排序与水位无关；等 finish 统一放行
    }

    @Override
    public void flush(Output output) {
        buffer.sort(new RowComparator(columns));
        for (Record record : buffer) {
            output.collect(record);
        }
        buffer.clear();
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

    /** 多列比较器：前缀 - 降序；数值按数值比，其余按文本比；null 排最后。 */
    private static final class RowComparator implements Comparator<Record> {

        private final List<String> columns;

        RowComparator(List<String> columns) {
            this.columns = columns;
        }

        @Override
        public int compare(Record left, Record right) {
            for (String declared : columns) {
                boolean descending = declared.startsWith("-");
                String column = descending ? declared.substring(1) : declared;
                int result = compareValues(left.row().get(column), right.row().get(column));
                if (result != 0) {
                    return descending ? -result : result;
                }
            }
            return 0;
        }

        private static int compareValues(Object left, Object right) {
            if (left == null && right == null) {
                return 0;
            }
            if (left == null) {
                return 1;
            }
            if (right == null) {
                return -1;
            }
            if (left instanceof Number && right instanceof Number) {
                return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
            }
            return String.valueOf(left).compareTo(String.valueOf(right));
        }
    }
}
