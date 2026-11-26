package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 透视算子：**行转列**（{@code unpivot} 的对偶；Spark {@code pivot} 口径）。
 *
 * <p>声明「按什么分组、哪一列的值变成列名、对哪一列做什么聚合」，宽表一把出：
 *
 * <pre>
 * - id: wide
 *   type: pivot
 *   config:
 *     keyBy: [product]         # 分组列（保留为行的前几列；省略 = 全局一行）
 *     pivot: month             # 透视列：值变成列名
 *     value: amount            # 值列（fn=COUNT 时可省）
 *     fn: SUM                  # SUM/COUNT/AVG/MIN/MAX/FIRST/LAST（缺省 SUM）
 *     columns: ["2026-01", "2026-02"]   # 可选：显式列序；缺省按首次出现序
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li>⚠️ **物化算子**：收齐全流才出一行 —— 只适合有界数据（文件 / 批式源）；</li>
 *   <li>输出行 = keyBy 列（声明序）+ 每个透视值一列（列名 = 值的文本形态）；
 *       某组没出现的透视值补 null；</li>
 *   <li>透视值匹配按**文本形态**比对（{@code 1} 与 {@code "1"} 同列）；数值统一走
 *       {@link Accumulator} 的 BigDecimal 语义；</li>
 *   <li>聚合函数白名单（不含 COLLECT_*：每个透视格收数组太容易撑爆）。</li>
 * </ul>
 *
 * @author zifang
 */
public class PivotOperator implements Operator {

    private static final List<String> SAFE_FUNCTIONS = Arrays.asList(
            Accumulator.FN_SUM, Accumulator.FN_COUNT, Accumulator.FN_AVG,
            Accumulator.FN_MIN, Accumulator.FN_MAX, Accumulator.FN_FIRST, Accumulator.FN_LAST);

    private final PipelineNode node;

    private List<String> keyBy;
    private String pivot;
    private String value;
    private String fn;
    private List<String> declaredColumns;
    private OperatorContext context;

    /** 收齐的原始行（物化口径）。 */
    private List<Record> buffer;

    public PivotOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        keyBy = node.strings("keyBy");
        pivot = MapOperator.text(node.string("pivot"));
        value = MapOperator.text(node.string("value"));
        fn = node.string("fn", "SUM").toUpperCase();
        declaredColumns = node.strings("columns");
        if (pivot == null) {
            throw new IllegalArgumentException("pivot 节点 " + node.getId() + " 必须声明 config.pivot（值转列名的列）");
        }
        if (value == null && !Accumulator.FN_COUNT.equals(fn)) {
            throw new IllegalArgumentException("pivot 节点 " + node.getId() + " 必须声明 config.value（fn=COUNT 除外）");
        }
        if (!SAFE_FUNCTIONS.contains(fn)) {
            throw new IllegalArgumentException("pivot 节点 " + node.getId() + " 的聚合函数不支持: " + fn
                    + "（可用: SUM/COUNT/AVG/MIN/MAX/FIRST/LAST）");
        }
        buffer = new ArrayList<Record>();
    }

    @Override
    public void processElement(Record record, Output output) {
        buffer.add(record);
    }

    @Override
    public void processWatermark(long watermark) {
        // 与水位无关；等 flush 统一透视
    }

    @Override
    public void flush(Output output) {
        // 列序：显式声明优先；缺省按首次出现序
        List<String> columns = new ArrayList<String>(declaredColumns);
        // 组 → (透视值文本 → 累加器)；LinkedHashMap 保持组与透视值的首次出现序
        Map<List<String>, Map<String, Accumulator>> groups =
                new LinkedHashMap<List<String>, Map<String, Accumulator>>();
        for (Record record : buffer) {
            Map<String, Object> row = record.row();
            List<String> key = keyOf(row);
            Map<String, Accumulator> cells = groups.get(key);
            if (cells == null) {
                cells = new LinkedHashMap<String, Accumulator>();
                groups.put(key, cells);
            }
            Object pivotValue = row.get(pivot);
            if (pivotValue == null) {
                context.count("null-pivot-skipped");
                continue;
            }
            String column = String.valueOf(pivotValue);
            if (!columns.contains(column)) {
                columns.add(column);
            }
            Accumulator cell = cells.get(column);
            if (cell == null) {
                cell = new Accumulator(fn, value == null ? "*" : value, column);
                cells.put(column, cell);
            }
            cell.accumulate(value == null ? null : row.get(value));
        }
        for (Map.Entry<List<String>, Map<String, Accumulator>> entry : groups.entrySet()) {
            Map<String, Object> out = new LinkedHashMap<String, Object>();
            List<String> key = entry.getKey();
            for (int i = 0; i < keyBy.size(); i++) {
                out.put(keyBy.get(i), key.get(i));
            }
            Map<String, Accumulator> cells = entry.getValue();
            for (String column : columns) {
                Accumulator cell = cells.get(column);
                out.put(column, cell == null ? null : cell.result());
            }
            output.collect(Record.of(out));
            context.count("pivoted-groups");
        }
        buffer.clear();
    }

    /** 分组键（按 keyBy 声明序取值；文本形态，null 保留）。 */
    private List<String> keyOf(Map<String, Object> row) {
        List<String> key = new ArrayList<String>();
        for (String column : keyBy) {
            Object value = row.get(column);
            key.add(value == null ? null : String.valueOf(value));
        }
        return key;
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        buffer = null;
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
