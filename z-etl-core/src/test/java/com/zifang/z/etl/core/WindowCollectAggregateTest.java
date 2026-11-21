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

/**
 * 窗口聚合收集函数验收：{@code window.aggregate} 里用 {@code COLLECT_LIST} /
 * {@code COLLECT_SET} —— 事件时间窗口 + 水位触发的完整链路上收集照常工作。
 *
 * <p>口径：窗口触发时产出数组快照（窗口内收到的值）；SET 去重保持插入序；
 * 与 SUM 同声明互不干扰；产出行带窗口首尾列（win_start/win_end）。
 *
 * @author zifang
 */
class WindowCollectAggregateTest {

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rows(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }

    @Test
    @DisplayName("滚动窗口 + COLLECT_LIST：窗口触发产出当时快照，两个窗口各一份")
    void windowCollectList() throws Exception {
        PipelineRunResult result = run(""
                + "name: win-collect\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {t: LONG}\n"
                + "      eventTimeColumn: t\n"
                + "      rows:\n"
                + "        - {user_id: 1, t: 100, tag: a}\n"
                + "        - {user_id: 1, t: 150, tag: b}\n"
                + "        - {user_id: 1, t: 210, tag: c}\n"
                + "  - id: wagg\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [user_id]\n"
                + "      window: {type: tumbling, size: 100ms}\n"
                + "      aggregates:\n"
                + "        - {fn: COLLECT_LIST, column: tag, alias: tags}\n"
                + "        - {fn: COUNT, column: \"*\", alias: n}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wagg}\n"
                + "  - {from: wagg, to: out}\n");
        List<Map<String, Object>> rows = rows(result);
        assertEquals(2, rows.size(), "两个窗口各产出一次");
        assertEquals(list("a", "b"), rows.get(0).get("tags"), "窗口一收集 a、b");
        assertEquals(list("c"), rows.get(1).get("tags"), "窗口二只有 c");
        assertEquals(Long.valueOf(2L), rows.get(0).get("n"));
        assertEquals(Long.valueOf(1L), rows.get(1).get("n"));
    }

    @Test
    @DisplayName("滚动窗口 + COLLECT_SET：同窗口同值只收一次；与 SUM 同声明互不干扰")
    void windowCollectSet() throws Exception {
        PipelineRunResult result = run(""
                + "name: win-collect-set\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {t: LONG}\n"
                + "      eventTimeColumn: t\n"
                + "      rows:\n"
                + "        - {user_id: 1, t: 100, tag: a, amount: 10}\n"
                + "        - {user_id: 1, t: 120, tag: a, amount: 20}\n"
                + "        - {user_id: 1, t: 140, tag: b, amount: 30}\n"
                + "  - id: wagg\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [user_id]\n"
                + "      window: {type: tumbling, size: 100ms}\n"
                + "      aggregates:\n"
                + "        - {fn: COLLECT_SET, column: tag, alias: tags}\n"
                + "        - {fn: SUM, column: amount, alias: total}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wagg}\n"
                + "  - {from: wagg, to: out}\n");
        List<Map<String, Object>> rows = rows(result);
        assertEquals(1, rows.size());
        assertEquals(list("a", "b"), rows.get(0).get("tags"), "SET 去重、插入序");
        Object total = rows.get(0).get("total");
        assertEquals(0, new java.math.BigDecimal("60").compareTo((java.math.BigDecimal) total),
                "SUM 照常（60）");
    }

    private static List<String> list(String... items) {
        return java.util.Arrays.asList(items);
    }
}
