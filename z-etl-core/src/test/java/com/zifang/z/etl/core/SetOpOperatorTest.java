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
 * 集合运算算子验收：except（差集）/ intersect（交集），SQL 集合运算的流式版。
 *
 * <p>口径：与 diff 的分工——集合关系不要变化方向（行原样发不打标记）；
 * 缺省整行指纹比对（值归一 {@code 1.0} ≡ {@code "1"}），声明 keys 按键比；
 * 产出 = 左流首现序；⚠️ 物化口径。
 *
 * @author zifang
 */
class SetOpOperatorTest {

    private static final String LEFT_ROWS = ""
            + "  - id: a\n"
            + "    type: inline.source\n"
            + "    config:\n"
            + "      rows:\n"
            + "        - {id: 1, tag: x}\n"
            + "        - {id: 2, tag: y}\n"
            + "        - {id: 3, tag: z}\n";
    private static final String RIGHT_ROWS = ""
            + "  - id: b\n"
            + "    type: inline.source\n"
            + "    config:\n"
            + "      rows:\n"
            + "        - {id: 2, tag: y}\n"
            + "        - {id: 9, tag: w}\n";

    @Test
    @DisplayName("except 按键：左有右无的行原样发出（1、3），值归一不误伤")
    void exceptByKey() throws Exception {
        PipelineRunResult result = run(""
                + "name: except-key\n"
                + "nodes:\n"
                + LEFT_ROWS
                + RIGHT_ROWS
                + "  - id: setop\n"
                + "    type: except\n"
                + "    config:\n"
                + "      left: a\n"
                + "      right: b\n"
                + "      keys: [id]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: a, to: setop}\n"
                + "  - {from: b, to: setop}\n"
                + "  - {from: setop, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size());
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"));
        assertEquals(Integer.valueOf(3), rows.get(1).get("id"));
        assertEquals("x", rows.get(0).get("tag"), "行原样发，不打标记");
        assertEquals(Long.valueOf(2L), result.getNodeMetrics().get("setop").get("emitted"));
        assertEquals(Long.valueOf(1L), result.getNodeMetrics().get("setop").get("filtered"));
    }

    @Test
    @DisplayName("intersect 按键：两侧都有的行（2）；整行缺省比对按值归一（'100' ≡ 100.0）")
    void intersectByKeyAndWholeRow() throws Exception {
        PipelineRunResult intersect = run(""
                + "name: intersect-key\n"
                + "nodes:\n"
                + LEFT_ROWS
                + RIGHT_ROWS
                + "  - id: setop\n"
                + "    type: intersect\n"
                + "    config:\n"
                + "      left: a\n"
                + "      right: b\n"
                + "      keys: [id]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: a, to: setop}\n"
                + "  - {from: b, to: setop}\n"
                + "  - {from: setop, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(intersect);
        assertEquals(1, rows.size());
        assertEquals(Integer.valueOf(2), rows.get(0).get("id"));

        PipelineRunResult wholeRow = run(""
                + "name: setop-wholerow\n"
                + "nodes:\n"
                + "  - id: a\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {v: '100'}\n"
                + "        - {v: 7}\n"
                + "  - id: b\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {v: 100.0}\n"
                + "  - id: setop\n"
                + "    type: except\n"
                + "    config:\n"
                + "      left: a\n"
                + "      right: b\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: a, to: setop}\n"
                + "  - {from: b, to: setop}\n"
                + "  - {from: setop, to: out}\n");
        List<Map<String, Object>> rest = rowsOf(wholeRow);
        assertEquals(1, rest.size(), "'100' 与 100.0 等值被滤掉，只剩 7");
        assertEquals(Integer.valueOf(7), rest.get(0).get("v"));
    }

    @Test
    @DisplayName("校验：单入边报错")
    void validation() {
        IllegalArgumentException oneEdge = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: setop-broken\n"
                + "nodes:\n"
                + "  - id: a\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 1}]\n"
                + "  - id: setop\n"
                + "    type: except\n"
                + "    config:\n"
                + "      left: a\n"
                + "      right: b\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: a, to: setop}\n"
                + "  - {from: setop, to: out}\n"));
        assertTrue(oneEdge.getMessage().contains("入边"), oneEdge.getMessage());
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
