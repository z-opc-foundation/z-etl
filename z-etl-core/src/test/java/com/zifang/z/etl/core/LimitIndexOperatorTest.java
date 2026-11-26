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
 * 截取与行号算子验收：limit（边流边截）/ index（序号列）。
 *
 * @author zifang
 */
class LimitIndexOperatorTest {

    @Test
    @DisplayName("limit：前 N 条放行、后面丢弃计数；offset 先跳过再截")
    void limitAndOffset() throws Exception {
        PipelineRunResult limit = run(""
                + "name: limit-basic\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "        - {id: 2}\n"
                + "        - {id: 3}\n"
                + "        - {id: 4}\n"
                + "        - {id: 5}\n"
                + "  - id: head\n"
                + "    type: limit\n"
                + "    config:\n"
                + "      limit: 3\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: head}\n"
                + "  - {from: head, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(limit);
        assertEquals(3, rows.size());
        assertEquals(Integer.valueOf(3), rows.get(2).get("id"));
        assertEquals(Long.valueOf(2L), limit.getNodeMetrics().get("head").get("truncated"));

        PipelineRunResult offset = run(""
                + "name: limit-offset\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "        - {id: 2}\n"
                + "        - {id: 3}\n"
                + "        - {id: 4}\n"
                + "  - id: head\n"
                + "    type: limit\n"
                + "    config:\n"
                + "      limit: 2\n"
                + "      offset: 1\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: head}\n"
                + "  - {from: head, to: out}\n");
        List<Map<String, Object>> skipped = rowsOf(offset);
        assertEquals(2, skipped.size());
        assertEquals(Integer.valueOf(2), skipped.get(0).get("id"));
        assertEquals(Integer.valueOf(3), skipped.get(1).get("id"));
        assertEquals(Long.valueOf(1L), offset.getNodeMetrics().get("head").get("offset-skipped"));
    }

    @Test
    @DisplayName("index：全局编号（start 起算）与按 keyBy 各自编号")
    void globalAndKeyedIndex() throws Exception {
        PipelineRunResult global = run(""
                + "name: index-global\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {k: a}\n"
                + "        - {k: b}\n"
                + "        - {k: a}\n"
                + "  - id: numbered\n"
                + "    type: index\n"
                + "    config:\n"
                + "      name: row_num\n"
                + "      start: 100\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: numbered}\n"
                + "  - {from: numbered, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(global);
        assertEquals(Long.valueOf(100L), rows.get(0).get("row_num"));
        assertEquals(Long.valueOf(101L), rows.get(1).get("row_num"));
        assertEquals(Long.valueOf(102L), rows.get(2).get("row_num"));

        PipelineRunResult keyed = run(""
                + "name: index-keyed\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {k: a}\n"
                + "        - {k: b}\n"
                + "        - {k: a}\n"
                + "  - id: numbered\n"
                + "    type: index\n"
                + "    config:\n"
                + "      keyBy: [k]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: numbered}\n"
                + "  - {from: numbered, to: out}\n");
        List<Map<String, Object>> keyedRows = rowsOf(keyed);
        assertEquals(Long.valueOf(1L), keyedRows.get(0).get("row_num"));
        assertEquals(Long.valueOf(1L), keyedRows.get(1).get("row_num"), "b 键从 1 重新起算");
        assertEquals(Long.valueOf(2L), keyedRows.get(2).get("row_num"), "a 键第二行编号 2");
    }

    @Test
    @DisplayName("校验：limit 缺失报错")
    void validation() {
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: limit-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 1}]\n"
                + "  - id: head\n"
                + "    type: limit\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: head}\n"
                + "  - {from: head, to: out}\n"));
        assertTrue(missing.getMessage().contains("limit"), missing.getMessage());
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
