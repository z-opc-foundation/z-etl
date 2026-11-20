package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 截断算子：按键 / 全局 Top-N（批口径，Flink Rank 的保守子集）。
 *
 * <p>缓存到流尾（{@code flush}）才排序产出：按 {@code orderBy} 列排序，
 * 取前 {@code limit} 条；声明了 {@code keyBy} 就每键各取前 N 条，否则全局取前 N 条。
 *
 * <pre>
 * - id: top
 *   type: topn
 *   config:
 *     orderBy: score     # 排序列（必填）
 *     order: desc        # 可选：asc（缺省）/ desc
 *     limit: 10          # 前 N 条（必填，至少 1）
 *     keyBy: [user_id]   # 可选：每键各取前 N 条
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li><b>批口径</b>：整条流缓在内存里，流尾才排序产出 —— 适合文件 / 有限流，
 *       别接无限源（有限流口径下 checkpoint 也不背这些缓存）；</li>
 *   <li>数值列按数值比（Integer/Long/Double 混着来都行），其余按字符串比；
 *       null 视为最小（asc 排最前、desc 排最后）；</li>
 *   <li>同值按到达顺序保持稳定（稳定排序，不折腾并列名次）；</li>
 *   <li>产出行是原行原样（不加分名列，不加名次列）。</li>
 * </ul>
 *
 * @author zifang
 */
public class TopNOperator implements Operator {

    /** 未声明 keyBy 时的全局键。 */
    private static final String GLOBAL_KEY = "\u0000topn-global";

    private final PipelineNode node;

    private String orderBy;
    private boolean desc;
    private int limit;
    private List<String> keyBy = new ArrayList<String>();

    private final Map<String, List<Map<String, Object>>> buffers =
            new LinkedHashMap<String, List<Map<String, Object>>>();

    private OperatorContext context;

    public TopNOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        orderBy = MapOperator.text(node.string("orderBy"));
        if (orderBy == null) {
            throw new IllegalArgumentException("topn 节点 " + node.getId() + " 必须声明 config.orderBy");
        }
        String order = node.string("order", "asc").trim().toLowerCase();
        if (!"asc".equals(order) && !"desc".equals(order)) {
            throw new IllegalArgumentException("topn 节点 " + node.getId()
                    + " 的 order 只认 asc/desc: " + order);
        }
        desc = "desc".equals(order);
        limit = node.integer("limit", 0);
        if (limit < 1) {
            throw new IllegalArgumentException("topn 节点 " + node.getId() + " 的 limit 至少为 1");
        }
        keyBy = node.strings("keyBy");
    }

    @Override
    public void processElement(Record record, Output output) {
        String key = keyBy.isEmpty() ? GLOBAL_KEY : Keys.computeValues(record.row(), keyBy);
        if (key == null) {
            throw new IllegalStateException("topn 节点 " + node.getId() + " 的键列为空，算不出键");
        }
        List<Map<String, Object>> buffer = buffers.get(key);
        if (buffer == null) {
            buffer = new ArrayList<Map<String, Object>>();
            buffers.put(key, buffer);
        }
        buffer.add(new LinkedHashMap<String, Object>(record.row()));
        context.count("buffered");
    }

    /** 流尾排序产出：每键（或全局）按 orderBy 排序取前 limit 条。 */
    @Override
    public void flush(Output output) {
        Comparator<Map<String, Object>> comparator = new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> left, Map<String, Object> right) {
                int cmp = compareValues(left.get(orderBy), right.get(orderBy));
                return desc ? -cmp : cmp;
            }
        };
        for (Map.Entry<String, List<Map<String, Object>>> entry : buffers.entrySet()) {
            List<Map<String, Object>> buffer = entry.getValue();
            Collections.sort(buffer, comparator);
            int emitted = 0;
            for (Map<String, Object> row : buffer) {
                if (emitted >= limit) {
                    break;
                }
                output.collect(Record.of(row));
                emitted++;
            }
            context.count("emitted", emitted);
            context.count("cut", buffer.size() - emitted);
        }
        buffers.clear();
    }

    /** 数值按数值比，其余按字符串比；null 视为最小。 */
    private static int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Number && right instanceof Number) {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    @Override
    public void processWatermark(long watermark) {
        // 缓存到流尾，不碰水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        // 缓存随算子一起释放
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
