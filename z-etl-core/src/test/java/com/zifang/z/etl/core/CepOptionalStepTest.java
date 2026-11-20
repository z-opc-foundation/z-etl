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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模式匹配组合语义验收：可选步骤（{@code optional}）、until 终止条件、流尾封口（flush）。
 *
 * <p>口径：optional 步骤 0 或 1 次 —— 下一步是 optional 又不吃事件就开「跳过它」的分支，
 * 吃到了也开一条「不吃」的分支（两条路径都合法，宁多配不少配）；
 * until 命中的事件不吃进该步（部分匹配留着等推进，不销毁）；
 * 剩余步骤全是 optional 的部分匹配在流结束时补产出（flush 封口）。
 *
 * @author zifang
 */
class CepOptionalStepTest {

    @Test
    @DisplayName("optional 跳过：create → (pay 可选) → ship，中间没有 pay 也能配成，pay# = 0")
    void optionalSkipped() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-opt-skip\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: SHIP}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", optional: true}\n"
                + "        - {name: ship, expression: \"type == 'SHIP'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "跳过 pay 也能配成");
        assertEquals("CREATE", rows.get(0).get("create.type"));
        assertEquals(Integer.valueOf(0), rows.get(0).get("pay#"), "跳过的步骤只有次数（0）");
        assertFalse(rows.get(0).containsKey("pay.type"), "跳过的步骤没有「步骤名.列」");
        assertEquals("SHIP", rows.get(0).get("ship.type"));
        assertEquals(Long.valueOf(1L), metrics(result).get("matched"));
        assertEquals(Long.valueOf(1L), metrics(result).get("optional-skipped"));
    }

    @Test
    @DisplayName("optional 多路径：pay 事件到了，吃进与跳过两条路都活着 ⇒ ship 到时出两条匹配")
    void optionalBothPaths() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-opt-both\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY}\n"
                + "        - {order_id: 1, type: SHIP}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", optional: true}\n"
                + "        - {name: ship, expression: \"type == 'SHIP'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "吃到（pay# 1）与跳过（pay# 0）两条路径各出一条");
        assertEquals(Integer.valueOf(1), rows.get(0).get("pay#"), "吃到路径先出（部分匹配表顺序）");
        assertEquals(Integer.valueOf(0), rows.get(1).get("pay#"));
        assertEquals("SHIP", rows.get(0).get("ship.type"));
        assertEquals(Long.valueOf(2L), metrics(result).get("matched"));
    }

    @Test
    @DisplayName("until 终止：cancelled 的 PAY 条件命中但 until 拦下 ⇒ 不吃进，不再多出匹配")
    void untilStopsConsumption() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-until\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY, cancelled: false, amount: 10}\n"
                + "        - {order_id: 1, type: PAY, cancelled: true, amount: 99}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", times: \"1,\", until: \"cancelled\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "cancelled 的 PAY 被 until 拦下，不再续吃");
        assertEquals(Integer.valueOf(1), rows.get(0).get("pay#"));
        assertEquals(Integer.valueOf(10), rows.get(0).get("pay.amount"), "行是第一条 PAY 的");
        assertEquals(Long.valueOf(1L), metrics(result).get("until-stopped"));
    }

    @Test
    @DisplayName("流尾封口：create → (pay 可选)，等不到 pay，流结束补产出 pay# = 0")
    void flushSealsOptionalTail() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-flush\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", optional: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "流尾把「剩余步骤全 optional」的匹配补产出");
        assertEquals("CREATE", rows.get(0).get("create.type"));
        assertEquals(Integer.valueOf(0), rows.get(0).get("pay#"));
        assertEquals(Long.valueOf(0L), metrics(result).getOrDefault("matched", 0L));
        assertEquals(Long.valueOf(1L), metrics(result).get("flush-emitted"));
    }

    @Test
    @DisplayName("optional 与 times 同步声明 ⇒ 跑之前就报错")
    void optionalWithTimesRejected() {
        String yaml = ""
                + "name: cep-opt-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", optional: true, times: \"2\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(yaml));
        assertTrue(error.getMessage().contains("optional"), "应点名 optional: " + error.getMessage());
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

    private Map<String, Long> metrics(PipelineRunResult result) {
        return result.getNodeMetrics().get("cep");
    }
}
