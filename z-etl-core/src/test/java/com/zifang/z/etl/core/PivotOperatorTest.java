package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 透视算子验收：行转列（unpivot 的对偶，Spark pivot 口径）。
 *
 * <p>口径：keyBy 分组 + 透视列值变列名 + 聚合（白名单）；物化口径收齐全流才出；
 * 某组没出现的透视值补 null；无 keyBy = 全局一行；fn=COUNT 可省 value。
 *
 * @author zifang
 */
class PivotOperatorTest {

    @Test
    @DisplayName("基本透视：product × month 的 SUM，分组保持首次出现序，缺格补 null")
    void basicPivot() throws Exception {
        PipelineRunResult result = run(""
                + "name: pivot-basic\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {product: A, month: 1, amount: 100}\n"
                + "        - {product: A, month: 2, amount: 200}\n"
                + "        - {product: B, month: 1, amount: 50}\n"
                + "        - {product: A, month: 1, amount: 30}\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      keyBy: [product]\n"
                + "      pivot: month\n"
                + "      value: amount\n"
                + "      fn: SUM\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "A、B 两组各一行");
        assertEquals("A", rows.get(0).get("product"));
        assertEquals(0, new java.math.BigDecimal("130").compareTo((java.math.BigDecimal) rows.get(0).get("1")),
                "A 月 1 = 100 + 30");
        assertEquals(0, new java.math.BigDecimal("200").compareTo((java.math.BigDecimal) rows.get(0).get("2")));
        assertEquals("B", rows.get(1).get("product"));
        assertEquals(0, new java.math.BigDecimal("50").compareTo((java.math.BigDecimal) rows.get(1).get("1")));
        assertNull(rows.get(1).get("2"), "B 没有月 2 ⇒ 补 null");
    }

    @Test
    @DisplayName("显式 columns 定列序；COUNT 免 value；全局透视（无 keyBy）出一行")
    void declaredColumnsCountAndGlobal() throws Exception {
        PipelineRunResult declared = run(""
                + "name: pivot-declared\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {k: x, m: b, v: 1}\n"
                + "        - {k: x, m: a, v: 2}\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      keyBy: [k]\n"
                + "      pivot: m\n"
                + "      value: v\n"
                + "      columns: [a, b]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n");
        Map<String, Object> row = rowsOf(declared).get(0);
        assertEquals(0, new java.math.BigDecimal("2").compareTo((java.math.BigDecimal) row.get("a")),
                "columns 声明序 a 在前");
        assertEquals(0, new java.math.BigDecimal("1").compareTo((java.math.BigDecimal) row.get("b")));

        PipelineRunResult counted = run(""
                + "name: pivot-count\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {k: x, m: a}\n"
                + "        - {k: x, m: a}\n"
                + "        - {k: x, m: b}\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      keyBy: [k]\n"
                + "      pivot: m\n"
                + "      fn: COUNT\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n");
        Map<String, Object> countRow = rowsOf(counted).get(0);
        assertEquals(Long.valueOf(2L), countRow.get("a"));
        assertEquals(Long.valueOf(1L), countRow.get("b"));

        PipelineRunResult global = run(""
                + "name: pivot-global\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {m: a, v: 5}\n"
                + "        - {m: b, v: 7}\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      pivot: m\n"
                + "      value: v\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n");
        List<Map<String, Object>> globalRows = rowsOf(global);
        assertEquals(1, globalRows.size(), "无 keyBy = 全局一行");
        assertEquals(0, new java.math.BigDecimal("5").compareTo((java.math.BigDecimal) globalRows.get(0).get("a")));
        assertEquals(0, new java.math.BigDecimal("7").compareTo((java.math.BigDecimal) globalRows.get(0).get("b")));
    }

    @Test
    @DisplayName("校验：缺 pivot 报错；fn=SUM 缺 value 报错；fn 不在白名单报错")
    void validation() {
        IllegalArgumentException missingPivot = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: pivot-broken1\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{m: a, v: 1}]\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      value: v\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n"));
        assertTrue(missingPivot.getMessage().contains("pivot"), missingPivot.getMessage());

        IllegalArgumentException missingValue = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: pivot-broken2\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{m: a}]\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      pivot: m\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n"));
        assertTrue(missingValue.getMessage().contains("value"), missingValue.getMessage());

        IllegalArgumentException badFn = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: pivot-broken3\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{m: a, v: 1}]\n"
                + "  - id: wide\n"
                + "    type: pivot\n"
                + "    config:\n"
                + "      pivot: m\n"
                + "      value: v\n"
                + "      fn: COLLECT_LIST\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wide}\n"
                + "  - {from: wide, to: out}\n"));
        assertTrue(badFn.getMessage().contains("聚合函数"), badFn.getMessage());
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }
}
