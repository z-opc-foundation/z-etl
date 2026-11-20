package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 区间连接验收（{@code interval.join}，Flink 的 IntervalJoin 口径）。
 *
 * <p>口径：两边都缓存、按「关联键 + 事件时间之差 ≤ within」两两配对（每对只发一次）；
 * 水位到期清理缓存；没有事件时间的行没法配对 ⇒ 丢弃并计数。
 *
 * @author zifang
 */
class IntervalJoinOperatorTest {

    private static final long MINUTE = 60_000L;

    @Test
    @DisplayName("区间内配对：两边事件时间相差 ≤ within ⇒ 拼行发出")
    void matchesWithinInterval() throws Exception {
        String yaml = pipeline(
                "      rows:\n"
                        + "        - {order_id: 100, amount: 5, t: " + (1000) + "}\n"
                        + "        - {order_id: 100, amount: 6, t: " + (4000) + "}\n",
                "      rows:\n"
                        + "        - {order_id: 100, track_no: T1, t: " + (2000) + "}\n",
                "      within: 5m\n");
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(2, rows.size(), "left1×right 与 left2×right 各配一对: " + rows);
        assertEquals("T1", rows.get(0).get("track_no"));
        assertEquals(Integer.valueOf(100), rows.get(0).get("order_id"));
        assertEquals(Integer.valueOf(100), rows.get(0).get("order_id_right"), "右侧行的同名列加后缀");
        assertTrue(result.getRecordsWritten() >= 2L);
    }

    @Test
    @DisplayName("区间外不配对：事件时间相差超过 within ⇒ 不发")
    void noMatchOutsideInterval() throws Exception {
        String yaml = pipeline(
                "      rows:\n"
                        + "        - {order_id: 100, amount: 5, t: " + (1000) + "}\n",
                "      rows:\n"
                        + "        - {order_id: 100, track_no: T1, t: " + (5000) + "}\n",
                "      within: 1s\n");
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        assertEquals(0, collectRows(captured).size(), "相差 4s > within 1s ⇒ 不配对");
    }

    @Test
    @DisplayName("joinType=left：左侧没命中也发（右侧列补 null）")
    void leftOuterEmitsUnmatched() throws Exception {
        String yaml = pipeline(
                "      rows:\n"
                        + "        - {order_id: 100, amount: 5, t: " + (1000) + "}\n",
                "      rows:\n"
                        + "        - {order_id: 999, track_no: T1, t: " + (2000) + "}\n",
                "      within: 5m\n"
                        + "      joinType: left\n");
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(1, rows.size(), "左侧行不该被丢: " + rows);
        assertEquals(Integer.valueOf(100), rows.get(0).get("order_id"));
        assertEquals(Integer.valueOf(5), rows.get(0).get("amount"));
    }

    @Test
    @DisplayName("每对只发一次：2×2 全在窗口内 ⇒ 4 对（笛卡尔积）")
    void crossPairsEmittedOnce() throws Exception {
        String yaml = pipeline(
                "      rows:\n"
                        + "        - {order_id: 100, amount: 5, t: " + (1000) + "}\n"
                        + "        - {order_id: 100, amount: 6, t: " + (2000) + "}\n",
                "      rows:\n"
                        + "        - {order_id: 100, track_no: T1, t: " + (1500) + "}\n"
                        + "        - {order_id: 100, track_no: T2, t: " + (2500) + "}\n",
                "      within: 10m\n");
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(yaml, captured);
        assertEquals(4, collectRows(captured).size(), "2 左 × 2 右 = 4 对");
    }

    @Test
    @DisplayName("水位到期清理：过期缓存被清掉并计数")
    void expiresOldEntries() throws Exception {
        String yaml = pipeline(
                "      rows:\n"
                        + "        - {order_id: 100, amount: 5, t: " + (0) + "}\n",
                "      rows:\n"
                        + "        - {order_id: 100, track_no: T1, t: " + (4 * MINUTE) + "}\n"
                        + "        - {order_id: 100, track_no: T2, t: " + (12 * MINUTE) + "}\n"
                        + "        - {order_id: 100, track_no: T3, t: " + (13 * MINUTE) + "}\n",
                "      within: 5m\n");
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        assertEquals(1, collectRows(captured).size(), "只有 T1 配上（Δ4m ≤ 5m）");
        Map<String, Long> metrics = result.getNodeMetrics().get("ij");
        assertNotNull(metrics, "应能拿到节点指标: " + result.getNodeMetrics());
        assertTrue(metrics.containsKey("expired") && metrics.get("expired") >= 2L,
                "水位过后的过期缓存应被清掉并计数，实际: " + metrics);
    }

    @Test
    @DisplayName("没有事件时间的行没法配对 ⇒ 丢弃并计数")
    void rowsWithoutEventTimeDropped() throws Exception {
        String yaml = ""
                + "name: ij-no-event-time\n"
                + "nodes:\n"
                + "  - id: src1\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 100, amount: 5}\n"
                + "  - id: src2\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 100, track_no: T1, t: 1000}\n"
                + "        - {order_id: 100, track_no: T2, t: 2000}\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "  - id: ij\n"
                + "    type: interval.join\n"
                + "    config:\n"
                + "      left: src1\n"
                + "      right: src2\n"
                + "      leftKey: [order_id]\n"
                + "      rightKey: [order_id]\n"
                + "      within: 5m\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src1, to: ij}\n"
                + "  - {from: src2, to: ij}\n"
                + "  - {from: ij, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        assertEquals(0, collectRows(captured).size(), "左侧没有事件时间 ⇒ 没法配对");
        Map<String, Long> metrics = result.getNodeMetrics().get("ij");
        assertNotNull(metrics);
        assertEquals(Long.valueOf(1L), metrics.get("no-event-time-dropped"), "应计数，实际: " + metrics);
    }

    @Test
    @DisplayName("没声明 within ⇒ 跑之前就报错")
    void requiresWithin() {
        String yaml = pipeline(
                "      rows:\n        - {order_id: 100, amount: 5, t: 1000}\n",
                "      rows:\n        - {order_id: 100, track_no: T1, t: 2000}\n",
                "");
        try {
            run(yaml, new LinkedHashMap<String, Operator>());
            org.junit.jupiter.api.Assertions.fail("缺 within 应该报错");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("within"), "应点名 within，实际: " + expected.getMessage());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 两路内联源 + 区间连接 + 收集汇的流水线声明。 */
    private String pipeline(String leftRows, String rightRows, String joinConfig) {
        return ""
                + "name: ij-demo\n"
                + "nodes:\n"
                + "  - id: src1\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + leftRows.replace("      rows:", "      rows:")
                + "  - id: src2\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + rightRows
                + "  - id: ij\n"
                + "    type: interval.join\n"
                + "    config:\n"
                + "      left: src1\n"
                + "      right: src2\n"
                + "      leftKey: [order_id]\n"
                + "      rightKey: [order_id]\n"
                + joinConfig
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src1, to: ij}\n"
                + "  - {from: src2, to: ij}\n"
                + "  - {from: ij, to: sink}\n";
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured) throws Exception {
        PipelineSpec spec = PipelineParser.parse(yaml);
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend()).execute(spec, captured);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured) {
        Operator operator = captured.get("sink");
        assertNotNull(operator, "应捕获到汇算子");
        return ((SinkOperators.CollectSink) operator).rows();
    }
}
