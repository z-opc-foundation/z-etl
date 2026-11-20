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
 * 逐节点耗时验收：运行结果的节点指标里有 {@code elapsed-nanos}，
 * 慢在哪个节点一眼可见（源拉取 / 变换 / 汇分别计时）。
 *
 * @author zifang
 */
class NodeMetricsTest {

    @Test
    @DisplayName("每个节点都有 elapsed-nanos，且都在总时长之内")
    void everyNodeReportsElapsed() throws Exception {
        String yaml = ""
                + "name: metrics-demo\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, amount: 100}\n"
                + "        - {id: 2, amount: 200}\n"
                + "  - id: calc\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns:\n"
                + "        - {name: doubled, engine: EL, expression: \"amount * 2\"}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: calc}\n"
                + "  - {from: calc, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        long start = System.nanoTime();
        PipelineRunResult result = new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
        long wallNanos = System.nanoTime() - start;

        assertEquals(2L, result.getRecordsWritten());
        for (String nodeId : new String[]{"src", "calc", "sink"}) {
            Map<String, Long> metrics = result.getNodeMetrics().get(nodeId);
            assertNotNull(metrics, "节点 " + nodeId + " 应有指标: " + result.getNodeMetrics());
            assertTrue(metrics.containsKey("elapsed-nanos"),
                    "节点 " + nodeId + " 应有耗时指标，实际: " + metrics);
            assertTrue(metrics.get("elapsed-nanos") >= 0L, "耗时非负: " + metrics);
            assertTrue(metrics.get("elapsed-nanos") <= wallNanos,
                    "节点耗时应不超过墙钟: " + metrics);
        }
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(Integer.valueOf(200), rows.get(0).get("doubled"), "变换照常生效");
    }

    @Test
    @DisplayName("干重活的节点耗时明显高于轻活节点")
    void heavyNodeTakesLonger() throws Exception {
        String light = ""
                + "name: metrics-heavy\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "  - id: heavy\n"
                + "    type: script\n"
                + "    config:\n"
                + "      name: burn\n"
                + "      engine: GROOVY\n"
                + "      script: \"def x = 0L\\n(1..300000).each { x = x + it }\\nx\"\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: heavy}\n"
                + "  - {from: heavy, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(light), captured);
        Map<String, Long> heavy = result.getNodeMetrics().get("heavy");
        Map<String, Long> source = result.getNodeMetrics().get("src");
        assertNotNull(heavy);
        assertNotNull(source);
        assertTrue(heavy.get("elapsed-nanos") > source.get("elapsed-nanos"),
                "干重活的节点耗时应更高，实际 heavy=" + heavy + " src=" + source);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured) {
        Operator operator = captured.get("sink");
        assertNotNull(operator, "应捕获到汇算子");
        return ((SinkOperators.CollectSink) operator).rows();
    }
}
