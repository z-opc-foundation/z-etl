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
 * 列转行验收：声明列逐列竖排成名字列 + 值列，其余列原样保留。
 *
 * <p>口径：每条输入按声明列序各出一行；null 缺省丢并计数（{@code null-skipped}），
 * {@code keepNull: true} 也出一行；无状态，与 explode（纵向炸开）互补。
 *
 * @author zifang
 */
class UnpivotOperatorTest {

    @Test
    @DisplayName("基础 melt：两个月份列竖成两行，id 列原样保留，缺省名字/值列名")
    void basicMelt() throws Exception {
        PipelineRunResult result = run(""
                + "name: unpivot-basic\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, jan: 100, feb: 200}\n"
                + "  - id: melt\n"
                + "    type: unpivot\n"
                + "    config:\n"
                + "      columns: [jan, feb]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: melt}\n"
                + "  - {from: melt, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "两列各出一行");
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"), "未声明的列原样保留");
        assertEquals("jan", rows.get(0).get("column"));
        assertEquals(Integer.valueOf(100), rows.get(0).get("value"));
        assertEquals("feb", rows.get(1).get("column"));
        assertEquals(Integer.valueOf(200), rows.get(1).get("value"));
        assertEquals(Long.valueOf(2L), metrics(result).get("records-out"));
    }

    @Test
    @DisplayName("自定义名字/值列名 + keepNull：null 也出一行")
    void customColumnsAndKeepNull() throws Exception {
        PipelineRunResult result = run(""
                + "name: unpivot-null\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, jan: 100, feb: null}\n"
                + "  - id: melt\n"
                + "    type: unpivot\n"
                + "    config:\n"
                + "      columns: [jan, feb]\n"
                + "      nameColumn: month\n"
                + "      valueColumn: amount\n"
                + "      keepNull: true\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: melt}\n"
                + "  - {from: melt, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "keepNull ⇒ null 值也出一行");
        assertEquals("feb", rows.get(1).get("month"));
        assertNull(rows.get(1).get("amount"));
    }

    @Test
    @DisplayName("缺省丢 null：null 列跳过并计数 null-skipped")
    void nullSkippedByDefault() throws Exception {
        PipelineRunResult result = run(""
                + "name: unpivot-skip\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, jan: 100, feb: null}\n"
                + "        - {id: 2, jan: null, feb: null}\n"
                + "  - id: melt\n"
                + "    type: unpivot\n"
                + "    config:\n"
                + "      columns: [jan, feb]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: melt}\n"
                + "  - {from: melt, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "只有 id 1 的 jan 一行活着");
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"));
        assertEquals(Long.valueOf(3L), metrics(result).get("null-skipped"));
    }

    @Test
    @DisplayName("columns 缺省/空 ⇒ 跑之前就报错")
    void validation() {
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: unpivot-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: melt\n"
                + "    type: unpivot\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: melt}\n"
                + "  - {from: melt, to: out}\n"));
        assertTrue(missing.getMessage().contains("columns"), missing.getMessage());
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
        return result.getNodeMetrics().get("melt");
    }
}
