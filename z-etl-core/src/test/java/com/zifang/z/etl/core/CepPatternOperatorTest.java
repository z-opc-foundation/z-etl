package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模式匹配（{@code cep.pattern}）验收：单流步骤序列 + 次数量词 + within 时间窗。
 *
 * <p>口径：能延续旧匹配就延续、能开新匹配就开新匹配；最后一步次数到下限即产出；
 * within 声明后水位过期清理；每键部分匹配有上限防爆炸。
 *
 * @author zifang
 */
class CepPatternOperatorTest {

    @Test
    @DisplayName("基本序列：create → pay，产出行带每步「步骤名.列」与「步骤名#」")
    void basicSequence() throws Exception {
        String yaml = ""
                + "name: cep-basic\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY}\n"
                + "        - {order_id: 2, type: CREATE}\n"
                + "        - {order_id: 2, type: PAY}\n"
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
                + "  - {from: cep, to: out}\n";
        PipelineRunResult result = run(yaml);
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "两个订单各出一条匹配");
        assertEquals("CREATE", rows.get(0).get("create.type"));
        assertEquals("PAY", rows.get(0).get("pay.type"));
        assertEquals(Integer.valueOf(1), rows.get(0).get("create#"));
        assertEquals(Integer.valueOf(1), rows.get(0).get("pay#"));
        assertEquals(Integer.valueOf(2), rows.get(1).get("order_id"));
        Map<String, Long> metrics = metrics(result);
        assertEquals(Long.valueOf(2L), metrics.get("matched"));
    }

    @Test
    @DisplayName("次数量词 times: '1,' —— 最后一步未封顶，每多一条同条件事件多出一条匹配")
    void openEndedTimes() throws Exception {
        String yaml = ""
                + "name: cep-loop\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 1, type: PAY, amount: 10}\n"
                + "        - {order_id: 1, type: PAY, amount: 20}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\", times: \"1,\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        PipelineRunResult result = run(yaml);
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "两条 PAY 各出一条匹配");
        assertEquals(Integer.valueOf(1), rows.get(0).get("pay#"));
        assertEquals(Integer.valueOf(2), rows.get(1).get("pay#"));
        assertEquals(Integer.valueOf(20), rows.get(1).get("amount"), "行是最后一条 PAY 的");
    }

    @Test
    @DisplayName("within 超时：配对事件来得太晚不发（旧匹配清掉并计数）")
    void withinExpiry() throws Exception {
        String yaml = ""
                + "name: cep-within\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {t: LONG}\n"
                + "      eventTimeColumn: t\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE, t: 0}\n"
                + "        - {order_id: 1, type: PAY, t: 600000}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      within: 5m\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        PipelineRunResult result = run(yaml);
        assertEquals(0, rowsOf(result).size(), "PAY 晚于 within ⇒ 不该配对");
        Map<String, Long> metrics = metrics(result);
        assertTrue(metrics.containsKey("expired") && metrics.get("expired") >= 1L,
                "过期应清掉并计数，实际: " + metrics);
    }

    @Test
    @DisplayName("键隔离：不同 order_id 的 create/pay 不互配")
    void keysIsolated() throws Exception {
        String yaml = ""
                + "name: cep-keys\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, type: CREATE}\n"
                + "        - {order_id: 2, type: PAY}\n"
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
                + "  - {from: cep, to: out}\n";
        PipelineRunResult result = run(yaml);
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size());
        assertEquals(Integer.valueOf(1), rows.get(0).get("order_id"));
    }

    @Test
    @DisplayName("maxPartials 上限：部分匹配超出丢最老并计数，最新的还能配成")
    void partialCap() throws Exception {
        String yaml = ""
                + "name: cep-cap\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user: 甲, session: 1, type: CREATE}\n"
                + "        - {user: 甲, session: 2, type: CREATE}\n"
                + "        - {user: 甲, session: 2, type: PAY}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [user]\n"
                + "      maxPartials: 1\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        PipelineRunResult result = run(yaml);
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "会话 1 的部分匹配被挤掉，只剩会话 2 能配成");
        assertEquals(Integer.valueOf(2), rows.get(0).get("session"));
        Map<String, Long> metrics = metrics(result);
        assertEquals(Long.valueOf(1L), metrics.get("partial-overflow-dropped"));
    }

    @Test
    @DisplayName("缺 steps ⇒ 跑之前就报错")
    void requiresSteps() {
        String yaml = ""
                + "name: cep-broken\n"
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
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(yaml));
        assertTrue(error.getMessage().contains("steps"), "应点名 steps: " + error.getMessage());
    }

    private final Map<String, com.zifang.z.etl.core.runtime.Operator> captured =
            new LinkedHashMap<String, com.zifang.z.etl.core.runtime.Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new com.zifang.z.etl.core.spi.MapDataSourceProvider(),
                new HeapStateBackend()).execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }

    private Map<String, Long> metrics(PipelineRunResult result) {
        return result.getNodeMetrics().get("cep");
    }
}
