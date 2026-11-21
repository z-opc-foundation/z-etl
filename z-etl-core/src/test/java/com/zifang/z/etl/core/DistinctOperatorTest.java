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
 * 全行去重验收：整行 / 列子集判重，重复丢并计数，上限兜底放行不漏放。
 *
 * <p>口径：缺省整行所有列拼指纹；{@code columns} 声明后按列子集；
 * 指纹超 {@code cap} 的行不再跟踪但**照常放行**（{@code cap-overflow}，
 * 宁多放不漏放）；行原样放行或丢掉，只计数不改行。
 *
 * @author zifang
 */
class DistinctOperatorTest {

    @Test
    @DisplayName("整行判重：完全相同的行只留第一条，重复计数")
    void wholeRowDistinct() throws Exception {
        PipelineRunResult result = run(""
                + "name: distinct-row\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {a: x, b: 1}\n"
                + "        - {a: x, b: 1}\n"
                + "        - {a: y, b: 2}\n"
                + "        - {a: x, b: 2}\n"
                + "  - id: uq\n"
                + "    type: distinct\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: uq}\n"
                + "  - {from: uq, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "第 2 行整行重复被丢");
        assertEquals("x", rows.get(0).get("a"));
        assertEquals("y", rows.get(1).get("a"));
        assertEquals("x", rows.get(2).get("a"));
        assertEquals(Integer.valueOf(2), rows.get(2).get("b"), "a 相同但 b 不同 ⇒ 不算重复");
        assertEquals(Long.valueOf(1L), metrics(result).get("duplicates"));
        assertEquals(Long.valueOf(3L), metrics(result).get("records-out"));
    }

    @Test
    @DisplayName("列子集判重：columns [a] ⇒ 同 a 只留第一条，其余列不管")
    void columnSubsetDistinct() throws Exception {
        PipelineRunResult result = run(""
                + "name: distinct-cols\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, a: x}\n"
                + "        - {id: 2, a: x}\n"
                + "        - {id: 3, a: y}\n"
                + "  - id: uq\n"
                + "    type: distinct\n"
                + "    config:\n"
                + "      columns: [a]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: uq}\n"
                + "  - {from: uq, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "同 a 只留第一条");
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"));
        assertEquals(Integer.valueOf(3), rows.get(1).get("id"));
        assertEquals(Long.valueOf(1L), metrics(result).get("duplicates"));
    }

    @Test
    @DisplayName("上限兜底：cap 2 第 3 条不同行不再跟踪但照常放行（cap-overflow 计数）")
    void capOverflowPassesThrough() throws Exception {
        PipelineRunResult result = run(""
                + "name: distinct-cap\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {a: 1}\n"
                + "        - {a: 2}\n"
                + "        - {a: 3}\n"
                + "  - id: uq\n"
                + "    type: distinct\n"
                + "    config:\n"
                + "      cap: 2\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: uq}\n"
                + "  - {from: uq, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "超上限的行照常放行（宁多放不漏放）");
        assertEquals(Long.valueOf(1L), metrics(result).get("cap-overflow"));
        assertEquals(Long.valueOf(0L), metrics(result).getOrDefault("duplicates", 0L));
    }

    @Test
    @DisplayName("cap 不合法 ⇒ 跑之前就报错；null 值参与判重不炸")
    void validationAndNulls() throws Exception {
        IllegalArgumentException badCap = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: distinct-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: uq\n"
                + "    type: distinct\n"
                + "    config:\n"
                + "      cap: 0\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: uq}\n"
                + "  - {from: uq, to: out}\n"));
        assertTrue(badCap.getMessage().contains("cap"), badCap.getMessage());

        PipelineRunResult nulls = run(""
                + "name: distinct-null\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {a: null}\n"
                + "        - {a: null}\n"
                + "  - id: uq\n"
                + "    type: distinct\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: uq}\n"
                + "  - {from: uq, to: out}\n");
        assertEquals(1, rowsOf(nulls).size(), "null 行也判重");
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
        return result.getNodeMetrics().get("uq");
    }
}
