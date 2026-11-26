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
 * 画像算子验收：行原样透传 + 流尾按列出画像（Spark describe 的流式口径）。
 *
 * <p>口径：行不改一字过主输出；流结束把每列的 column/rows/nulls/distinct/min/max/mean
 * 收成画像行走 {@code reportTo} 旁路；没声明 reportTo 就只落节点指标；
 * distinct 超 {@code distinctCap} 不再计数（宁可少报不撑内存）。
 *
 * @author zifang
 */
class ProfileOperatorTest {

    @Test
    @DisplayName("透传不改行 + reportTo 画像行形状：rows/nulls/distinct/min/max/mean 全齐")
    void passthroughAndReport() throws Exception {
        PipelineRunResult result = run(""
                + "name: profile-basic\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {city: '上海', amount: 100}\n"
                + "        - {city: '北京', amount: 300}\n"
                + "        - {city: null, amount: 200}\n"
                + "        - {city: '上海', amount: 400}\n"
                + "  - id: stats\n"
                + "    type: profile\n"
                + "    config:\n"
                + "      reportTo: report\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "  - id: report\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: stats}\n"
                + "  - {from: stats, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result, "out");
        assertEquals(4, rows.size(), "行原样透传一行不少");
        assertEquals("上海", rows.get(0).get("city"));

        List<Map<String, Object>> report = rowsOf(result, "report");
        assertEquals(2, report.size(), "两列各一条画像行");
        Map<String, Object> cityStat = report.get(0);
        assertEquals("city", cityStat.get("column"));
        assertEquals(Long.valueOf(4L), cityStat.get("rows"));
        assertEquals(Long.valueOf(1L), cityStat.get("nulls"));
        assertEquals(Integer.valueOf(2), cityStat.get("distinct"));
        assertEquals("上海", cityStat.get("min"), "文本列 min 按码点序（上 U+4E0A < 北 U+5317）");
        assertEquals("北京", cityStat.get("max"));
        assertNull(cityStat.get("mean"), "非数值列没有 mean");

        Map<String, Object> amountStat = report.get(1);
        assertEquals("amount", amountStat.get("column"));
        assertEquals(Long.valueOf(0L), amountStat.get("nulls"));
        assertEquals(Integer.valueOf(4), amountStat.get("distinct"));
        assertEquals(0, new java.math.BigDecimal("100").compareTo((java.math.BigDecimal) amountStat.get("min")));
        assertEquals(0, new java.math.BigDecimal("400").compareTo((java.math.BigDecimal) amountStat.get("max")));
        assertEquals(0, new java.math.BigDecimal("250.0000")
                .compareTo((java.math.BigDecimal) amountStat.get("mean")), "均值 (100+300+200+400)/4 = 250");
        assertEquals(Long.valueOf(2L), result.getNodeMetrics().get("stats").get("profiled-columns"));
    }

    @Test
    @DisplayName("distinctCap 超限：不再计数 distinct 但行照常透传；无 reportTo 只落指标")
    void distinctCapAndMetricsOnly() throws Exception {
        PipelineRunResult capped = run(""
                + "name: profile-cap\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "        - {id: 2}\n"
                + "        - {id: 3}\n"
                + "  - id: stats\n"
                + "    type: profile\n"
                + "    config:\n"
                + "      reportTo: report\n"
                + "      distinctCap: 2\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "  - id: report\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: stats}\n"
                + "  - {from: stats, to: out}\n");
        assertEquals(3, rowsOf(capped, "out").size(), "超限的行照常透传");
        assertEquals(Integer.valueOf(2), rowsOf(capped, "report").get(0).get("distinct"),
                "distinct 封在 cap");
        assertEquals(Long.valueOf(1L), capped.getNodeMetrics().get("stats").get("distinct-cap"));

        PipelineRunResult metricsOnly = run(""
                + "name: profile-metrics\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "  - id: stats\n"
                + "    type: profile\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: stats}\n"
                + "  - {from: stats, to: out}\n");
        assertEquals(1, rowsOf(metricsOnly, "out").size(), "主输出不受影响");
        assertEquals(Long.valueOf(1L), metricsOnly.getNodeMetrics().get("stats").get("profiled-columns"));
    }

    @Test
    @DisplayName("校验：distinctCap 非法报错")
    void validation() {
        IllegalArgumentException badCap = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: profile-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: stats\n"
                + "    type: profile\n"
                + "    config:\n"
                + "      distinctCap: 0\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: stats}\n"
                + "  - {from: stats, to: out}\n"));
        assertTrue(badCap.getMessage().contains("distinctCap"), badCap.getMessage());
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result, String sinkId) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get(sinkId);
        return sink.rows();
    }
}
