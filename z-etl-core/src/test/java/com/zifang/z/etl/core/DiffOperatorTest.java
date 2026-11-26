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
 * 差异算子验收：两流按键比对出变化流（快照 diff 口径）。
 *
 * <p>口径：键只在左流 ⇒ INSERT（发左行）；键在两侧但值不同 ⇒ UPDATE（发左行）；
 * 键只在右流 ⇒ DELETE（发右行）；键在且值同 ⇒ 缺省不发只计数（unchanged: emit 才发）；
 * 值比对数值归一（{@code "100"} ≡ {@code 100.0}）；⚠️ 物化口径收齐两侧才比。
 *
 * @author zifang
 */
class DiffOperatorTest {

    @Test
    @DisplayName("INSERT / UPDATE / DELETE：只发变化，unchanged 计数不发")
    void insertUpdateDelete() throws Exception {
        PipelineRunResult result = run(""
                + "name: diff-basic\n"
                + "nodes:\n"
                + "  - id: cur\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: o1, amount: 100, status: 1}\n"
                + "        - {order_id: o2, amount: 250, status: 1}\n"
                + "        - {order_id: o3, amount: 300, status: 2}\n"
                + "  - id: ref\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: o2, amount: 200, status: 1}\n"
                + "        - {order_id: o3, amount: 300, status: 2}\n"
                + "        - {order_id: o4, amount: 400, status: 1}\n"
                + "  - id: delta\n"
                + "    type: diff\n"
                + "    config:\n"
                + "      left: cur\n"
                + "      right: ref\n"
                + "      keys: [order_id]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: cur, to: delta}\n"
                + "  - {from: ref, to: delta}\n"
                + "  - {from: delta, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "只发变化：INSERT + UPDATE + DELETE");
        assertEquals("o1", rows.get(0).get("order_id"));
        assertEquals("INSERT", rows.get(0).get("__op"));
        assertEquals("o2", rows.get(1).get("order_id"));
        assertEquals("UPDATE", rows.get(1).get("__op"));
        assertEquals(Integer.valueOf(250), rows.get(1).get("amount"), "UPDATE 发左行（当前值）");
        assertEquals("o4", rows.get(2).get("order_id"));
        assertEquals("DELETE", rows.get(2).get("__op"));
        assertEquals(Integer.valueOf(400), rows.get(2).get("amount"), "DELETE 发右行（基线值）");
        Map<String, Long> metrics = result.getNodeMetrics().get("delta");
        assertEquals(Long.valueOf(1L), metrics.get("inserted"));
        assertEquals(Long.valueOf(1L), metrics.get("updated"));
        assertEquals(Long.valueOf(1L), metrics.get("deleted"));
        assertEquals(Long.valueOf(1L), metrics.get("unchanged"));
    }

    @Test
    @DisplayName("数值归一比对：'100' 与 100.0 等值不产 UPDATE；unchanged: emit 带 SAME 发出")
    void numericNormalizeAndEmitUnchanged() throws Exception {
        PipelineRunResult emit = run(""
                + "name: diff-emit\n"
                + "nodes:\n"
                + "  - id: cur\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: a, amount: '100'}\n"
                + "  - id: ref\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: a, amount: 100.0}\n"
                + "  - id: delta\n"
                + "    type: diff\n"
                + "    config:\n"
                + "      left: cur\n"
                + "      right: ref\n"
                + "      keys: [id]\n"
                + "      unchanged: emit\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: cur, to: delta}\n"
                + "  - {from: ref, to: delta}\n"
                + "  - {from: delta, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(emit);
        assertEquals(1, rows.size());
        assertEquals("SAME", rows.get(0).get("__op"), "数值归一后等值 ⇒ SAME");
        assertEquals(Long.valueOf(1L), emit.getNodeMetrics().get("delta").get("unchanged"));
    }

    @Test
    @DisplayName("校验：单入边报错；缺 keys 报错")
    void validation() {
        IllegalArgumentException oneEdge = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: diff-broken1\n"
                + "nodes:\n"
                + "  - id: cur\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: a}]\n"
                + "  - id: delta\n"
                + "    type: diff\n"
                + "    config:\n"
                + "      left: cur\n"
                + "      right: ref\n"
                + "      keys: [id]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: cur, to: delta}\n"
                + "  - {from: delta, to: out}\n"));
        assertTrue(oneEdge.getMessage().contains("入边"), oneEdge.getMessage());

        IllegalArgumentException missingKeys = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: diff-broken2\n"
                + "nodes:\n"
                + "  - id: cur\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: a}]\n"
                + "  - id: ref\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: a}]\n"
                + "  - id: delta\n"
                + "    type: diff\n"
                + "    config:\n"
                + "      left: cur\n"
                + "      right: ref\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: cur, to: delta}\n"
                + "  - {from: ref, to: delta}\n"
                + "  - {from: delta, to: out}\n"));
        assertTrue(missingKeys.getMessage().contains("keys"), missingKeys.getMessage());
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
