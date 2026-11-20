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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模式匹配紧邻度验收：步骤声明 {@code strict: true}（Flink {@code .next()} 口径）。
 *
 * <p>口径：strict 声明在「下一步」上 —— 当前事件既不吃进当前步也不推进到该步 ⇒
 * 部分匹配死掉（中间不许插事件）；缺省宽松（插了事件照样等）；
 * 当前步续吃（次数未封顶）不算插事件。
 *
 * @author zifang
 */
class CepStrictContiguityTest {

    @Test
    @DisplayName("strict 紧邻命中：create → pay 直接跟着来，照常配成")
    void strictFollowMatched() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-strict-hit\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", strict: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size());
        assertEquals("PAY", rows.get(0).get("pay.type"));
    }

    @Test
    @DisplayName("strict 中间插事件：create → OTHER → pay ⇒ 匹配死掉，strict-broken 计数")
    void strictFollowBroken() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-strict-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: OTHER}\n"
                + "        - {order_id: 1, type: PAY}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", strict: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        assertEquals(0, rowsOf(result).size(), "中间插了 OTHER ⇒ 不该配成");
        assertTrue(metrics(result).containsKey("strict-broken")
                        && metrics(result).get("strict-broken") >= 1L,
                "strict 破约应计数，实际: " + metrics(result));
    }

    @Test
    @DisplayName("缺省宽松：同样的 create → OTHER → pay，不声明 strict 照样配成")
    void relaxedByDefault() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-relaxed\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: OTHER}\n"
                + "        - {order_id: 1, type: PAY}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        assertEquals(1, rowsOf(result).size(), "宽松口径插事件照样配成");
    }

    @Test
    @DisplayName("当前步续吃不算插事件：create → pay → pay → ship(strict) 照样配成")
    void loopGrowthKeepsStrict() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-strict-loop\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY, batch: 1}\n"
                + "        - {order_id: 1, type: PAY, batch: 2}\n"
                + "        - {order_id: 1, type: SHIP}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", times: \"1,\"}\n"
                + "        - {name: ship, expression: \"type == 'SHIP'\", strict: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        // ship(strict) 前面是第二次 pay 续吃，续吃不算插事件 ⇒ 照常配成
        assertEquals(1, rows.size());
        assertEquals(Integer.valueOf(2), rows.get(0).get("pay#"));
        assertEquals("SHIP", rows.get(0).get("ship.type"));
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
