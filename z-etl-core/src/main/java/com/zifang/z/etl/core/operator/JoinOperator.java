package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.StateBackend;
import com.zifang.z.etl.core.util.RowValues;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 连接算子：左右两路按键关联（流式版本）。
 *
 * <p>口径照抄 Flink 的 {@code CoGroupedStreams}：一路当**构建侧**（进状态），一路当**探测侧**（查状态）。
 * 谁是构建侧由配置指名（{@code config.left} / {@code config.right} 指的是上游节点 id）——
 * 执行器把「这条记录来自哪个上游节点」作为输入标签传进来（{@link Operator#processElement} 的三参版）。
 *
 * <p>连接类型：
 * <ul>
 *   <li>{@code inner}：探测侧没命中就不发；</li>
 *   <li>{@code left}：探测侧没命中也发（右侧列补 null）。</li>
 * </ul>
 *
 * <p>列名冲突时右侧列加后缀（默认 {@code _right}）。
 *
 * @author zifang
 */
public class JoinOperator implements Operator {

    private final PipelineNode node;

    private String leftNode;
    private String rightNode;
    private List<String> leftKey = new java.util.ArrayList<String>();
    private List<String> rightKey = new java.util.ArrayList<String>();
    private boolean leftOuter;
    private String rightSuffix = "_right";

    private OperatorContext context;
    private java.util.Set<String> rightColumns = new java.util.LinkedHashSet<String>();

    public JoinOperator(PipelineNode node) {
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
            throw new IllegalArgumentException("join 节点 " + node.getId() + " 必须声明 leftKey 与 rightKey");
        }
        String type = node.string("joinType", "inner").toLowerCase();
        leftOuter = "left".equals(type);
        rightSuffix = node.string("rightSuffix", "_right");
        if (ctx.state() == null) {
            throw new IllegalStateException("join 节点 " + node.getId() + " 需要键控状态，但没有分配到状态存储");
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
            build(record);
        }
    }

    /** 左侧（探测侧）：查右侧状态，命中就拼行。 */
    private void probe(Record record, Output output) {
        String key = Keys.compute(record.row(), leftKey);
        if (key == null) {
            throw new IllegalStateException("join 节点 " + node.getId() + " 的 leftKey 列为空，算不出关联键");
        }
        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        Object right = store.mapState("right").get(key);
        if (right instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> rightRow = (Map<String, Object>) right;
            output.collect(record.withRow(merge(record.row(), rightRow)));
            context.count("matched");
        } else if (leftOuter) {
            output.collect(record.withRow(merge(record.row(), null)));
            context.count("unmatched-left");
        } else {
            context.count("unmatched-dropped");
        }
    }

    /** 右侧（构建侧）：进状态，等左侧来查。 */
    @SuppressWarnings("unchecked")
    private void build(Record record) {
        String key = Keys.compute(record.row(), rightKey);
        if (key == null) {
            throw new IllegalStateException("join 节点 " + node.getId() + " 的 rightKey 列为空，算不出关联键");
        }
        StateBackend.KeyedStateStore store = context.state();
        store.setCurrentKey(key);
        store.mapState("right").put(key, new LinkedHashMap<String, Object>(record.row()));
        rightColumns.addAll(record.row().keySet());
        context.count("built");
    }

    /** 拼行：右侧列与左侧冲突时加后缀。 */
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

    /** 判断这条记录来自左侧还是右侧（输入标签优先；没有标签时按是否命中左键列判断）。 */
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

    @Override
    public void processWatermark(long watermark) {
        // 连接不涉水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 连接不涉定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 探测侧的键列（取证用）。 */
    public List<String> leftKeyColumns() {
        return leftKey;
    }

    /** 右侧列取值（供上层核对）。 */
    public Object rightValue(Map<String, Object> row, String column) {
        return RowValues.get(row, column);
    }
}
