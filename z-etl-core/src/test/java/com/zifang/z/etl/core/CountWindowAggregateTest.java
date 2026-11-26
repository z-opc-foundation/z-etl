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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 计数窗口验收：{@code window: {type: count, size: N}}（Kafka Streams 滚动计数窗口口径）。
 *
 * <p>口径：按键计数，每满 N 条**立即结算发出**再开新窗；流结束 flush 补发尾批（不满 N 条
 * 也出，不丢数据）；不涉水位；windowStart/windowEnd = 记录条数区间。
 *
 * @author zifang
 */
class CountWindowAggregateTest {

    @Test
    @DisplayName("每满 N 条立即结算 + flush 补发尾批：按键独立计数，不丢数据")
    void countWindowFiresAndFlushesTail() throws Exception {
        PipelineRunResult result = run(""
                + "name: count-window\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {k: a, amount: 1}\n"
                + "        - {k: a, amount: 2}\n"
                + "        - {k: a, amount: 3}\n"
                + "        - {k: b, amount: 10}\n"
                + "  - id: rollup\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [k]\n"
                + "      window: {type: count, size: 2}\n"
                + "      aggregates:\n"
                + "        - {fn: SUM, column: amount, alias: total}\n"
                + "        - {fn: COUNT, column: \"*\", alias: cnt}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: rollup}\n"
                + "  - {from: rollup, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "a 满批 1 + a 尾批 1 + b 尾批 1");
        assertEquals("a", rows.get(0).get("k"));
        assertEquals(0, new java.math.BigDecimal("3").compareTo((java.math.BigDecimal) rows.get(0).get("total")),
                "第一批 (1+2)");
        assertEquals(Long.valueOf(2L), rows.get(0).get("cnt"));
        assertEquals(Long.valueOf(0L), rows.get(0).get("windowStart"), "记录区间 [0, 2)");
        assertEquals(Long.valueOf(2L), rows.get(0).get("windowEnd"));
        assertEquals("a", rows.get(1).get("k"));
        assertEquals(0, new java.math.BigDecimal("3").compareTo((java.math.BigDecimal) rows.get(1).get("total")),
                "尾批 (3) flush 补发");
        assertEquals(Long.valueOf(1L), rows.get(1).get("cnt"));
        assertEquals(Long.valueOf(2L), rows.get(1).get("windowStart"), "记录区间 [2, 4)");
        assertEquals("b", rows.get(2).get("k"));
        assertEquals(0, new java.math.BigDecimal("10").compareTo((java.math.BigDecimal) rows.get(2).get("total")));
        assertEquals(Long.valueOf(1L), result.getNodeMetrics().get("rollup").get("count-batches"),
                "只有 a 的第一批是满批立即结算");
    }

    @Test
    @DisplayName("校验：count 窗口缺 size 报错")
    void validation() {
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: count-window-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{k: a, amount: 1}]\n"
                + "  - id: rollup\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [k]\n"
                + "      window: {type: count}\n"
                + "      aggregates:\n"
                + "        - {fn: SUM, column: amount, alias: total}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: rollup}\n"
                + "  - {from: rollup, to: out}\n"));
        assertTrue(missing.getMessage().contains("size"), missing.getMessage());
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
