package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.Accumulator;
import com.zifang.z.etl.core.operator.AggregateDef;
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
 * COLLECT_LIST 聚合验收：值收成数组（滚动累计），跳过 null，封顶防状态撑爆。
 *
 * <p>口径：{@code aggregate} 是滚动累计 —— 每行产出一次当前收集值；
 * null 跳过（SQL collect_list 口径）；单键收集上限 65536，超出丢并计数（增量口径）；
 * 配下游 {@code explode} 可以「聚合再还原」。
 *
 * @author zifang
 */
class CollectListAggregateTest {

    @Test
    @DisplayName("滚动收集：每行产出当前数组；null 跳过；COUNT(*) 同步在长")
    void rollingCollectSkipsNull() throws Exception {
        PipelineRunResult result = run(""
                + "name: collect-rolling\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user: 甲, tag: a}\n"
                + "        - {user: 甲, tag: null}\n"
                + "        - {user: 甲, tag: b}\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [user]\n"
                + "      aggregates:\n"
                + "        - {fn: COLLECT_LIST, column: tag, alias: tags}\n"
                + "        - {fn: COUNT, column: \"*\", alias: n}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: agg}\n"
                + "  - {from: agg, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "滚动累计每行都产出");
        assertEquals(list("a"), rows.get(0).get("tags"));
        assertEquals(list("a"), rows.get(1).get("tags"), "null 不收进数组");
        assertEquals(list("a", "b"), rows.get(2).get("tags"));
        assertEquals(Long.valueOf(3L), rows.get(2).get("n"), "COUNT(*) 数所有行（含空值行）");
    }

    @Test
    @DisplayName("封顶：超过 65536 丢并计数（增量口径）")
    void cappedCollect() {
        Accumulator accumulator = new Accumulator("COLLECT_LIST", "tag", "tags");
        for (int i = 0; i < 70000; i++) {
            accumulator.accumulate(Integer.valueOf(i));
        }
        List<?> collected = (List<?>) accumulator.result();
        assertEquals(65536, collected.size(), "收集封顶在 MAX_COLLECT");
        assertEquals(4464L, accumulator.collectDroppedDelta(), "超出部分计数");
        assertEquals(0L, accumulator.collectDroppedDelta(), "读后清零（增量口径）");
    }

    @Test
    @DisplayName("COLLECT_SET 去重收集：同值只收一次，保持插入序；快照照旧")
    void collectSetDedup() throws Exception {
        PipelineRunResult result = run(""
                + "name: collect-set\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user: 甲, tag: a}\n"
                + "        - {user: 甲, tag: a}\n"
                + "        - {user: 甲, tag: b}\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [user]\n"
                + "      aggregates:\n"
                + "        - {fn: COLLECT_SET, column: tag, alias: tags}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: agg}\n"
                + "  - {from: agg, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size());
        assertEquals(list("a"), rows.get(0).get("tags"));
        assertEquals(list("a"), rows.get(1).get("tags"), "同值只收一次");
        assertEquals(list("a", "b"), rows.get(2).get("tags"), "插入序保持");
    }

    @Test
    @DisplayName("校验：COLLECT_LIST 收 * 报错；白名单报错文案带上 COLLECT_LIST")
    void validation() {
        IllegalArgumentException star = assertThrows(IllegalArgumentException.class,
                () -> AggregateDef.from(aggregate("COLLECT_LIST", "*")));
        assertTrue(star.getMessage().contains("COLLECT_LIST"), star.getMessage());
        IllegalArgumentException setStar = assertThrows(IllegalArgumentException.class,
                () -> AggregateDef.from(aggregate("COLLECT_SET", "*")));
        assertTrue(setStar.getMessage().contains("COLLECT_SET"), setStar.getMessage());
        AggregateDef def = AggregateDef.from(aggregate("COLLECT_LIST", "tag"));
        assertEquals("COLLECT_LIST", def.fn());
    }

    private static Map<String, Object> aggregate(String fn, String column) {
        Map<String, Object> item = new LinkedHashMap<String, Object>();
        item.put("fn", fn);
        item.put("column", column);
        item.put("alias", "out");
        return item;
    }

    private static List<String> list(String... items) {
        return java.util.Arrays.asList(items);
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
