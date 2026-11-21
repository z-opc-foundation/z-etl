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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 炸行算子验收：一行拆多行（Flink SQL 的 UNNEST 口径）。
 *
 * <p>口径：数组列逐元素出一行 / 字符串按 delimiter 切分；标量当单元素原样过；
 * 空数组 / null / 空串缺省丢并计数，{@code keepEmpty: true} 保留原行（目标列置 null）；
 * {@code target} 可把元素写到新列（原列不动）；{@code indexColumn} 顺带写下标。
 *
 * @author zifang
 */
class ExplodeOperatorTest {

    @Test
    @DisplayName("数组列炸开：tags [a,b] / [c] ⇒ 三行，带下标")
    void arrayExplode() throws Exception {
        PipelineRunResult result = run(""
                + "name: explode-array\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, tags: [a, b]}\n"
                + "        - {id: 2, tags: [c]}\n"
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: tags\n"
                + "      indexColumn: tag_idx\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "2 + 1 = 3 行");
        assertEquals("a", rows.get(0).get("tags"));
        assertEquals(Integer.valueOf(0), rows.get(0).get("tag_idx"));
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"));
        assertEquals("b", rows.get(1).get("tags"));
        assertEquals(Integer.valueOf(1), rows.get(1).get("tag_idx"));
        assertEquals("c", rows.get(2).get("tags"));
        assertEquals(Integer.valueOf(2), rows.get(2).get("id"));
        assertEquals(Long.valueOf(1L), metrics(result).getOrDefault("exploded", 0L));
        assertEquals(Long.valueOf(2L), metrics(result).get("records-in"), "进 2 行");
        assertEquals(Long.valueOf(3L), metrics(result).get("records-out"), "出 3 行（行数膨胀可见）");
    }

    @Test
    @DisplayName("字符串切分：delimiter \",\" 把 csv 列切成多行")
    void delimiterSplit() throws Exception {
        PipelineRunResult result = run(""
                + "name: explode-split\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, csv: \"a,b,c\"}\n"
                + "        - {id: 2, csv: \"x\"}\n"
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: csv\n"
                + "      delimiter: \",\"\n"
                + "      target: item\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(4, rows.size());
        assertEquals("a", rows.get(0).get("item"));
        assertEquals("c", rows.get(2).get("item"));
        assertEquals("a,b,c", rows.get(0).get("csv"), "target 是新列 ⇒ 原列不动");
        assertEquals("x", rows.get(3).get("item"));
    }

    @Test
    @DisplayName("空值缺省丢并计数；keepEmpty: true 保留原行（目标列置 null）")
    void emptyHandling() throws Exception {
        String head = ""
                + "name: explode-empty\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, tags: []}\n"
                + "        - {id: 2, tags: [z]}\n";
        String tail = ""
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n";
        PipelineRunResult dropped = run(head
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: tags\n"
                + tail);
        assertEquals(1, rowsOf(dropped).size(), "空数组缺省丢");
        assertEquals(Long.valueOf(1L), metrics(dropped).get("dropped-empty"));

        PipelineRunResult kept = run(head
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: tags\n"
                + "      keepEmpty: true\n"
                + tail);
        List<Map<String, Object>> rows = rowsOf(kept);
        assertEquals(2, rows.size(), "keepEmpty 保留原行");
        assertNull(rows.get(0).get("tags"), "保留行的目标列置 null");
        assertEquals(Integer.valueOf(2), rows.get(1).get("id"));
        assertEquals(Long.valueOf(1L), metrics(kept).get("kept-empty"));
    }

    @Test
    @DisplayName("标量当单元素原样过；缺 column ⇒ 跑之前就报错")
    void scalarAndValidation() throws Exception {
        PipelineRunResult result = run(""
                + "name: explode-scalar\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, n: 5}\n"
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config:\n"
                + "      column: n\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "标量按单元素处理");
        assertEquals(Integer.valueOf(5), rows.get(0).get("n"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: explode-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "  - id: ex\n"
                + "    type: explode\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: ex}\n"
                + "  - {from: ex, to: out}\n"));
        assertTrue(error.getMessage().contains("column"), "应点名 column: " + error.getMessage());
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
        return result.getNodeMetrics().get("ex");
    }
}
