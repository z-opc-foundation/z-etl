package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.AiExecutor;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 护栏与失败策略验收（future-etl 抄写波）：
 *
 * <ul>
 *   <li>explode 的 {@code maxFanOut} 单行炸行硬顶（缺省 1000，声明超界报错）；</li>
 *   <li>管道级 {@code failurePolicy}：fail-fast（缺省，一炸全断）/ continue（坏行计数放行）；</li>
 *   <li>{@code ai.map}：SPI 注入执行器补列；未注入时打开即报错说明。</li>
 * </ul>
 *
 * @author zifang
 */
class GuardAndFailurePolicyTest {

    @Test
    @DisplayName("explode maxFanOut：单行炸出超顶 ⇒ 报错点名上限；声明超界 1..10000 同样拦")
    void explodeFanOutGuard() {
        IllegalStateException tooBig = assertThrows(IllegalStateException.class, () -> run(""
                + "name: explode-guard\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, tags: ["
                + repeat("x", 1001)
                + "]}\n"
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: tags\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n"));
        assertTrue(tooBig.getMessage().contains("maxFanOut"), "应点名 maxFanOut: " + tooBig.getMessage());

        IllegalArgumentException badDecl = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: explode-guard-decl\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, tags: [a]}\n"
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: tags\n"
                + "      maxFanOut: 20000\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n"));
        assertTrue(badDecl.getMessage().contains("1..10000"), "声明超界应拦: " + badDecl.getMessage());
    }

    @Test
    @DisplayName("failurePolicy 缺省 fail-fast：一行炸 ⇒ 整条流断")
    void defaultFailFast() {
        assertThrows(Exception.class, () -> run(badRowPipeline(null)));
    }

    @Test
    @DisplayName("failurePolicy=continue：坏行计数放行（records-error），好行照常落地，结果带 note")
    void continuePolicy() throws Exception {
        PipelineRunResult result = run(badRowPipeline("continue"));
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "id=1/3 两行好数据照常落地");
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"));
        assertEquals(Integer.valueOf(3), rows.get(1).get("id"));
        Map<String, Long> metrics = result.getNodeMetrics().get("boom");
        assertEquals(Long.valueOf(1L), metrics.getOrDefault("records-error", 0L), "坏行在目标节点计数");
        assertTrue(result.getNotes().stream().anyMatch(n -> n.contains("continue")),
                "结果 note 应声明 continue 模式: " + result.getNotes());
        assertTrue(result.getNotes().stream().anyMatch(n -> n.contains("最近一次")),
                "结果 note 应带最近一次失败摘要: " + result.getNotes());
    }

    @Test
    @DisplayName("ai.map：SPI 执行器补列，prompt ${列} 渲染；未装配 ⇒ open 报错点名 AiExecutor")
    void aiMapOperator() throws Exception {
        List<String> prompts = new ArrayList<String>();
        AiExecutor fake = (agentCode, system, prompt, row) -> {
            prompts.add(agentCode + "|" + system + "|" + prompt);
            return "分类:" + row.get("text");
        };
        captured.clear();
        PipelineRunResult result = new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend(),
                new com.zifang.z.etl.core.transform.ScriptSandbox(), null, null, null, null, fake)
                .execute(PipelineParser.parse(""
                        + "name: ai-map-ok\n"
                        + "nodes:\n"
                        + "  - id: src\n"
                        + "    type: inline.source\n"
                        + "    config:\n"
                        + "      rows:\n"
                        + "        - {id: 1, text: 服务器宕机}\n"
                        + "  - id: ai\n"
                        + "    type: ai.map\n"
                        + "    config:\n"
                        + "      prompt: \"归类：${text}\"\n"
                        + "      system: 你是工单分类助手\n"
                        + "      agentCode: ticket-clf\n"
                        + "      outputField: category\n"
                        + "  - id: out\n"
                        + "    type: collect.sink\n"
                        + "edges:\n"
                        + "  - {from: src, to: ai}\n"
                        + "  - {from: ai, to: out}\n"), captured);
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size());
        assertEquals("分类:服务器宕机", rows.get(0).get("category"), "模型输出落声明列");
        assertEquals("ticket-clf|你是工单分类助手|归类：服务器宕机", prompts.get(0),
                "agentCode/system/渲染后 prompt 透传");
        assertEquals(Long.valueOf(1L), result.getNodeMetrics().get("ai").get("ai-completed"));

        IllegalStateException noExecutor = assertThrows(IllegalStateException.class, () -> run(""
                + "name: ai-map-no-exec\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, text: x}\n"
                + "  - id: ai\n"
                + "    type: ai.map\n"
                + "    config:\n"
                + "      prompt: \"归类：${text}\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ai}\n"
                + "  - {from: ai, to: out}\n"));
        assertTrue(noExecutor.getMessage().contains("AiExecutor"), "应点名 AiExecutor SPI: " + noExecutor.getMessage());
    }

    /** 一条 id=2 会炸的 script 管道（failurePolicy 可注入）。 */
    private String badRowPipeline(String policy) {
        return ""
                + "name: failure-policy-demo\n"
                + (policy == null ? "" : "failurePolicy: " + policy + "\n")
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, text: ok}\n"
                + "        - {id: 2, text: bad}\n"
                + "        - {id: 3, text: ok}\n"
                + "  - id: boom\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: flat\n"
                + "      engine: GROOVY\n"
                + "      script: \"def v = 1 / (id - 2)\\n[id: id, text: text]\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: boom}\n"
                + "  - {from: boom, to: out}\n";
    }

    private static String repeat(String value, int times) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < times; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(value).append(i);
        }
        return builder.toString();
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
