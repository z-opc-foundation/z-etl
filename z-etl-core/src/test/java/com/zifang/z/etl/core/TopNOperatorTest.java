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
 * 截断算子验收：按键 / 全局 Top-N（批口径，流尾排序产出）。
 *
 * <p>口径：整条流缓存到流尾（flush）才排序产出；数值列按数值比，null 视为最小；
 * 同值按到达顺序稳定；产出行是原行原样。
 *
 * @author zifang
 */
class TopNOperatorTest {

    @Test
    @DisplayName("全局 Top-N：desc 取前 2，产出有序")
    void globalTopN() throws Exception {
        PipelineRunResult result = run(""
                + "name: topn-global\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, score: 70}\n"
                + "        - {id: 2, score: 95}\n"
                + "        - {id: 3, score: 88}\n"
                + "        - {id: 4, score: 60}\n"
                + "  - id: top\n"
                + "    type: topn\n"
                + "    config:\n"
                + "      orderBy: score\n"
                + "      order: desc\n"
                + "      limit: 2\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: top}\n"
                + "  - {from: top, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size());
        assertEquals(Integer.valueOf(95), rows.get(0).get("score"));
        assertEquals(Integer.valueOf(88), rows.get(1).get("score"));
        assertEquals(Long.valueOf(4L), metrics(result).get("buffered"));
        assertEquals(Long.valueOf(2L), metrics(result).get("emitted"));
        assertEquals(Long.valueOf(2L), metrics(result).get("cut"));
    }

    @Test
    @DisplayName("按键 Top-N：每键各取前 N；数值跨类型比 + null 视为最小")
    void perKeyTopN() throws Exception {
        PipelineRunResult result = run(""
                + "name: topn-keyed\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user: 甲, score: 70}\n"
                + "        - {user: 甲, score: 95}\n"
                + "        - {user: 甲, score: 88}\n"
                + "        - {user: 乙, score: 60}\n"
                + "        - {user: 乙, score: 100}\n"
                + "  - id: top\n"
                + "    type: topn\n"
                + "    config:\n"
                + "      orderBy: score\n"
                + "      order: desc\n"
                + "      limit: 2\n"
                + "      keyBy: [user]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: top}\n"
                + "  - {from: top, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(4, rows.size(), "甲 2 条 + 乙 2 条");
        assertEquals("甲", rows.get(0).get("user"));
        assertEquals(Integer.valueOf(95), rows.get(0).get("score"));
        assertEquals(Integer.valueOf(88), rows.get(1).get("score"));
        assertEquals("乙", rows.get(2).get("user"));
        assertEquals(Integer.valueOf(100), rows.get(2).get("score"));
        assertEquals(Integer.valueOf(60), rows.get(3).get("score"));
        assertEquals(Long.valueOf(1L), metrics(result).get("cut"));
    }

    @Test
    @DisplayName("asc + null 视为最小：null 排最前；数值列 Integer/Long 混着比")
    void ascWithNulls() throws Exception {
        PipelineRunResult result = run(""
                + "name: topn-asc\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {big: LONG}\n"
                + "      rows:\n"
                + "        - {id: 1, score: 5, big: 100}\n"
                + "        - {id: 2, score: 3, big: 20}\n"
                + "        - {id: 3, big: 50}\n"
                + "  - id: top\n"
                + "    type: topn\n"
                + "    config:\n"
                + "      orderBy: score\n"
                + "      limit: 3\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: top}\n"
                + "  - {from: top, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size());
        assertNull(rows.get(0).get("score"), "null 视为最小 ⇒ asc 排最前");
        assertEquals(Integer.valueOf(2), rows.get(1).get("id"), "score 3 次小");
        assertEquals(Integer.valueOf(1), rows.get(2).get("id"));
    }

    @Test
    @DisplayName("缺 orderBy / limit 不合法 ⇒ 跑之前就报错")
    void validation() {
        String head = ""
                + "name: topn-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, score: 5}\n";
        String tail = ""
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: top}\n"
                + "  - {from: top, to: out}\n";
        IllegalArgumentException noColumn = assertThrows(IllegalArgumentException.class, () -> run(head
                + "  - id: top\n"
                + "    type: topn\n"
                + "    config:\n"
                + "      limit: 1\n"
                + tail));
        assertTrue(noColumn.getMessage().contains("orderBy"));
        IllegalArgumentException noLimit = assertThrows(IllegalArgumentException.class, () -> run(head
                + "  - id: top\n"
                + "    type: topn\n"
                + "    config:\n"
                + "      orderBy: score\n"
                + tail));
        assertTrue(noLimit.getMessage().contains("limit"));
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
        return result.getNodeMetrics().get("top");
    }
}
