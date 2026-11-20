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
 * 模式匹配否定步骤验收：{@code notNext: true}（Flink {@code notNext()} 口径）。
 *
 * <p>口径：否定步不消费事件；部分匹配还没越过它时，违禁事件一到匹配即死
 * （否定优先于续吃）；不能是第一步、不能与 times/optional 同点声明；
 * 拖在模式末尾的 notNext 到流尾 flush 才封口产出。
 *
 * @author zifang
 */
class CepNotNextStepTest {

    @Test
    @DisplayName("notNext 中间守着：create → (不是 CANCEL) → ship，没来 CANCEL 照常配成")
    void notNextHolds() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-not-hit\n"
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
                + "        - {name: cancel, expression: \"type == 'CANCEL'\", notNext: true}\n"
                + "        - {name: ship, expression: \"type == 'SHIP'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size());
        assertEquals("SHIP", rows.get(0).get("ship.type"));
        assertEquals(Integer.valueOf(0), rows.get(0).get("cancel#"), "否定步只有次数（0）");
        assertFalse(rows.get(0).containsKey("cancel.type"));
    }

    @Test
    @DisplayName("notNext 违禁：create → CANCEL → ship ⇒ 匹配死掉，not-broken 计数")
    void notNextViolated() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-not-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: CANCEL}\n"
                + "        - {order_id: 1, type: SHIP}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: cancel, expression: \"type == 'CANCEL'\", notNext: true}\n"
                + "        - {name: ship, expression: \"type == 'SHIP'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        assertEquals(0, rowsOf(result).size(), "违禁事件一到匹配即死");
        assertTrue(metrics(result).containsKey("not-broken")
                        && metrics(result).get("not-broken") >= 1L,
                "否定违约应计数，实际: " + metrics(result));
    }

    @Test
    @DisplayName("notNext 拖尾：create → pay → (不是 CANCEL)，流尾封口才产出")
    void trailingNotNextFlush() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-not-tail\n"
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
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "        - {name: cancel, expression: \"type == 'CANCEL'\", notNext: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "到流尾都没等到 CANCEL ⇒ 封口产出");
        assertEquals("PAY", rows.get(0).get("pay.type"));
        assertEquals(Integer.valueOf(0), rows.get(0).get("cancel#"));
        assertEquals(Long.valueOf(0L), metrics(result).getOrDefault("matched", 0L));
        assertEquals(Long.valueOf(1L), metrics(result).get("flush-emitted"));
    }

    @Test
    @DisplayName("notNext 拖尾违禁：create → pay → CANCEL ⇒ 匹配死掉，流尾也不产出")
    void trailingNotNextViolated() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-not-tail-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY}\n"
                + "        - {order_id: 1, type: CANCEL}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "        - {name: cancel, expression: \"type == 'CANCEL'\", notNext: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        assertEquals(0, rowsOf(result).size());
        assertTrue(metrics(result).containsKey("not-broken"));
        assertEquals(Long.valueOf(0L), metrics(result).getOrDefault("flush-emitted", 0L));
    }

    @Test
    @DisplayName("notNext 是第一步 ⇒ 跑之前就报错")
    void notNextFirstRejected() {
        String yaml = ""
                + "name: cep-not-first\n"
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
                + "        - {name: cancel, expression: \"type == 'CANCEL'\", notNext: true}\n"
                + "        - {name: ship, expression: \"type == 'SHIP'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(yaml));
        assertTrue(error.getMessage().contains("notNext"), "应点名 notNext: " + error.getMessage());
    }

    @Test
    @DisplayName("notNext 与 times 同点声明 ⇒ 跑之前就报错")
    void notNextWithTimesRejected() {
        String yaml = ""
                + "name: cep-not-times\n"
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
                + "        - {name: cancel, expression: \"type == 'CANCEL'\", notNext: true, times: \"2\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(yaml));
        assertTrue(error.getMessage().contains("notNext"), "应点名 notNext: " + error.getMessage());
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
