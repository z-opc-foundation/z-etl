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
 * 路由算子验收：n 路条件分流（Kafka Streams {@code branch()} 口径）。
 *
 * <p>口径：{@code rules} 声明序逐条判定、**命中即走**（一条行只进一个分支）；
 * 全不命中走 {@code fallback}（没声明就丢行计数）；分流全走旁路标签，
 * 节点本身无主输出（声明出边 = 配置错误，校验器报错）。
 *
 * @author zifang
 */
class RouteOperatorTest {

    @Test
    @DisplayName("顺序判定命中即走：大额进 vip、其余进 normal，一条行只进一个分支")
    void firstMatchWins() throws Exception {
        PipelineRunResult result = run(""
                + "name: route-order\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, amount: 12000}\n"
                + "        - {id: 2, amount: 5000}\n"
                + "        - {id: 3, amount: 99999}\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config:\n"
                + "      rules:\n"
                + "        - {to: vip,    expression: \"amount > 10000\"}\n"
                + "        - {to: normal, expression: \"amount > 0\"}\n"
                + "  - id: vip\n"
                + "    type: collect.sink\n"
                + "  - id: normal\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n");
        List<Map<String, Object>> vip = rowsOf(result, "vip");
        List<Map<String, Object>> normal = rowsOf(result, "normal");
        assertEquals(2, vip.size(), "amount 12000 与 99999 进 vip");
        assertEquals(Integer.valueOf(1), vip.get(0).get("id"));
        assertEquals(Integer.valueOf(3), vip.get(1).get("id"));
        assertEquals(1, normal.size(), "amount 5000 进 normal");
        assertEquals(Integer.valueOf(2), normal.get(0).get("id"));
        Map<String, Long> metrics = metricsOf(result);
        assertEquals(Long.valueOf(3L), metrics.get("routed"));
        assertEquals(Long.valueOf(2L), metrics.get("to:vip"));
        assertEquals(Long.valueOf(1L), metrics.get("to:normal"));
    }

    @Test
    @DisplayName("fallback 兜底：全不命中的行走 fallback；没声明就丢行并计数 dropped")
    void fallbackAndDrop() throws Exception {
        PipelineRunResult withFallback = run(""
                + "name: route-fallback\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, amount: 100}\n"
                + "        - {id: 2, amount: -5}\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config:\n"
                + "      rules:\n"
                + "        - {to: good, expression: \"amount > 0\"}\n"
                + "      fallback: dead\n"
                + "  - id: good\n"
                + "    type: collect.sink\n"
                + "  - id: dead\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n");
        assertEquals(1, rowsOf(withFallback, "good").size());
        assertEquals(1, rowsOf(withFallback, "dead").size(), "负数行走 fallback");
        assertEquals(Long.valueOf(1L), metricsOf(withFallback).get("fallback"));

        PipelineRunResult withoutFallback = run(""
                + "name: route-drop\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, amount: -5}\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config:\n"
                + "      rules:\n"
                + "        - {to: good, expression: \"amount > 0\"}\n"
                + "  - id: good\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n");
        assertEquals(0, rowsOf(withoutFallback, "good").size());
        assertEquals(Long.valueOf(1L), metricsOf(withoutFallback).get("dropped"));
    }

    @Test
    @DisplayName("GROOVY 按条声明引擎：沙箱表达式判定可用")
    void groovyRule() throws Exception {
        PipelineRunResult result = run(""
                + "name: route-groovy\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, channel: ' app '}\n"
                + "        - {id: 2, channel: 'web'}\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config:\n"
                + "      rules:\n"
                + "        - {to: app, expression: \"channel.trim().toUpperCase() == 'APP'\", engine: GROOVY}\n"
                + "        - {to: web, expression: \"channel == 'web'\"}\n"
                + "  - id: app\n"
                + "    type: collect.sink\n"
                + "  - id: web\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n");
        assertEquals(1, rowsOf(result, "app").size());
        assertEquals(1, rowsOf(result, "web").size());
    }

    @Test
    @DisplayName("校验：缺 rules 报错；rules 缺 to/expression 报错；声明出边报错（分流无主输出）")
    void validation() {
        IllegalArgumentException noRules = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: route-broken1\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n"
                + "  - {from: split, to: out}\n"));
        assertTrue(noRules.getMessage().contains("rules"), noRules.getMessage());

        IllegalArgumentException missingTo = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: route-broken2\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config:\n"
                + "      rules:\n"
                + "        - {expression: \"a > 0\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n"));
        assertTrue(missingTo.getMessage().contains("to"), missingTo.getMessage());

        IllegalArgumentException outEdge = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: route-broken3\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: split\n"
                + "    type: route\n"
                + "    config:\n"
                + "      rules:\n"
                + "        - {to: out, expression: \"a > 0\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: split}\n"
                + "  - {from: split, to: out}\n"));
        assertTrue(outEdge.getMessage().contains("出边"), outEdge.getMessage());
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

    private Map<String, Long> metricsOf(PipelineRunResult result) {
        return result.getNodeMetrics().get("split");
    }
}
