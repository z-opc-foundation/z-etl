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
 * CEP 循环量词验收：{@code times: "*"}（0..∞）/ {@code "+"}（1..∞）。
 *
 * <p>口径：{@code *} 的步骤吃与不吃两条路径都活（宁多配不少配）——
 * 中间 0 条也能配（跳过分支），多条靠续吃；拖在模式末尾的 {@code *} 到流尾 flush 封口；
 * {@code +} ≡ {@code "1,"}（既有量词的别名）。
 *
 * @author zifang
 */
class CepLoopQuantifierTest {

    @Test
    @DisplayName("items* 中间步：每个循环长度（0..k）都是合法匹配路径，item# 区分（Flink NFA 同口径）")
    void starStepZeroOrMany() throws Exception {
        PipelineRunResult result = run(""
                + "name: cep-star\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: o1, type: 'CREATE'}\n"
                + "        - {order_id: o2, type: 'CREATE'}\n"
                + "        - {order_id: o2, type: 'ITEM'}\n"
                + "        - {order_id: o2, type: 'ITEM'}\n"
                + "        - {order_id: o2, type: 'PAY'}\n"
                + "        - {order_id: o1, type: 'PAY'}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: items, expression: \"type == 'ITEM'\", times: '*'}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(4, rows.size(), "o2 的循环长度 2/1/0 三条路径 + o1 的零商品路径");
        assertEquals("o2", rows.get(0).get("order_id"));
        assertEquals(Integer.valueOf(2), rows.get(0).get("items#"), "最长循环 2");
        assertEquals("o2", rows.get(1).get("order_id"));
        assertEquals(Integer.valueOf(1), rows.get(1).get("items#"), "循环 1 的路径也活（宁多配不少配）");
        assertEquals("o2", rows.get(2).get("order_id"));
        assertEquals(Integer.valueOf(0), rows.get(2).get("items#"), "跳过路径（循环 0）");
        assertEquals("o1", rows.get(3).get("order_id"));
        assertEquals(Integer.valueOf(0), rows.get(3).get("items#"), "零商品也能配");
    }

    @Test
    @DisplayName("拖尾 * 步从未进入时到流尾 flush 封口（items# = 0）；+ 是 1, 的别名（至少一次）")
    void trailingStarFlushSealAndPlus() throws Exception {
        PipelineRunResult trailing = run(""
                + "name: cep-trailing-star\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: o1, type: 'CREATE'}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: items, expression: \"type == 'ITEM'\", times: '*'}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(trailing);
        assertEquals(1, rows.size(), "* 步从未吃到事件 ⇒ 流尾封口补产出");
        assertEquals(Integer.valueOf(0), rows.get(0).get("items#"));
        assertEquals(Long.valueOf(1L), trailing.getNodeMetrics().get("cep").get("flush-emitted"));

        PipelineRunResult plus = run(""
                + "name: cep-plus\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: o1, type: 'CREATE'}\n"
                + "        - {order_id: o1, type: 'RETRY'}\n"
                + "        - {order_id: o1, type: 'PAY'}\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
                + "        - {name: retry, expression: \"type == 'RETRY'\", times: '+'}\n"
                + "        - {name: pay, expression: \"type == 'PAY'\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n");
        List<Map<String, Object>> plusRows = rowsOf(plus);
        assertEquals(1, plusRows.size(), "+ 至少一次：RETRY 一次后 PAY 封口");
        assertEquals(Integer.valueOf(1), plusRows.get(0).get("retry#"));
    }

    @Test
    @DisplayName("校验：恰好 0 次（times: '0'）不合法；* 与 optional 同点声明不合法")
    void validation() {
        IllegalArgumentException exactZero = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: cep-broken1\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{order_id: o1, type: 'CREATE'}]\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: a, expression: \"type == 'CREATE'\", times: '0'}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n"));
        assertTrue(exactZero.getMessage().contains("times"), exactZero.getMessage());

        IllegalArgumentException starOptional = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: cep-broken2\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{order_id: o1, type: 'CREATE'}]\n"
                + "  - id: cep\n"
                + "    type: cep.pattern\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + "      steps:\n"
                + "        - {name: a, expression: \"type == 'CREATE'\", times: '*', optional: true}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: cep}\n"
                + "  - {from: cep, to: out}\n"));
        assertTrue(starOptional.getMessage().contains("optional"), starOptional.getMessage());
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
