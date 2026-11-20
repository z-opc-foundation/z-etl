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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 采样算子验收：按比例放行。
 *
 * <p>口径：声明 {@code seed} 随机采样可复现；{@code hashBy} 按键哈希采样
 * 同键永远同判定且天然可复现；行原样放行或丢掉，只计数不改行。
 *
 * @author zifang
 */
class SampleOperatorTest {

    private static final String ROWS = ""
            + "      rows:\n"
            + "        - {id: 1, user: 甲}\n"
            + "        - {id: 2, user: 乙}\n"
            + "        - {id: 3, user: 甲}\n"
            + "        - {id: 4, user: 丙}\n"
            + "        - {id: 5, user: 乙}\n"
            + "        - {id: 6, user: 丁}\n";

    @Test
    @DisplayName("seed 随机采样：同 seed 两次跑产出完全一致；sampled + skipped = 总数")
    void seededReproducible() throws Exception {
        String yaml = ""
                + "name: sample-seed\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + ROWS
                + "  - id: sa\n"
                + "    type: sample\n"
                + "    config:\n"
                + "      rate: 0.5\n"
                + "      seed: 42\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sa}\n"
                + "  - {from: sa, to: out}\n";
        List<Object> first = ids(run(yaml));
        List<Object> second = ids(run(yaml));
        assertEquals(first, second, "同 seed ⇒ 两次跑完全一致");
        Map<String, Long> metrics = metrics(run(yaml));
        long sampled = metrics.getOrDefault("sampled", 0L);
        long skipped = metrics.getOrDefault("skipped", 0L);
        assertEquals(6L, sampled + skipped, "每行要么留要么丢");
        assertTrue(sampled >= 1L && sampled <= 5L, "0.5 的比例不该全留/全丢，实际: " + sampled);
    }

    @Test
    @DisplayName("hashBy 按键哈希：同键同判定（甲的行要么全留要么全丢），且可复现")
    void hashByKeyConsistent() throws Exception {
        String yaml = ""
                + "name: sample-hash\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + ROWS
                + "  - id: sa\n"
                + "    type: sample\n"
                + "    config:\n"
                + "      rate: 0.5\n"
                + "      hashBy: [user]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sa}\n"
                + "  - {from: sa, to: out}\n";
        List<Object> first = ids(run(yaml));
        List<Object> second = ids(run(yaml));
        assertEquals(first, second, "哈希采样天然可复现");
        // 甲（id 1、3）与乙（id 2、5）各两行：同键同判定 ⇒ 两行要么都在要么都不在
        boolean hasOne = first.contains(Integer.valueOf(1));
        assertEquals(hasOne, first.contains(Integer.valueOf(3)), "甲的两行同判定");
        assertEquals(first.contains(Integer.valueOf(2)), first.contains(Integer.valueOf(5)), "乙的两行同判定");
        Map<String, Long> metrics = metrics(run(yaml));
        assertEquals(6L, metrics.getOrDefault("sampled", 0L) + metrics.getOrDefault("skipped", 0L));
    }

    @Test
    @DisplayName("rate 1.0 全留；rate 越界 / 缺 rate ⇒ 跑之前就报错")
    void rateBoundaries() throws Exception {
        String head = ""
                + "name: sample-full\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + ROWS;
        String tail = ""
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sa}\n"
                + "  - {from: sa, to: out}\n";
        PipelineRunResult all = run(head
                + "  - id: sa\n"
                + "    type: sample\n"
                + "    config:\n"
                + "      rate: 1.0\n"
                + "      seed: 1\n"
                + tail);
        assertEquals(6, ids(all).size(), "rate 1.0 全留");
        assertEquals(Long.valueOf(6L), metrics(all).get("sampled"));

        assertThrows(IllegalArgumentException.class, () -> run(head
                + "  - id: sa\n"
                + "    type: sample\n"
                + "    config:\n"
                + "      rate: 1.5\n"
                + tail));
        assertThrows(IllegalArgumentException.class, () -> run(head
                + "  - id: sa\n"
                + "    type: sample\n"
                + "    config: {}\n"
                + tail));
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Object> ids(PipelineRunResult result) {
        List<Object> ids = new ArrayList<Object>();
        for (Map<String, Object> row : rowsOf(result)) {
            ids.add(row.get("id"));
        }
        return ids;
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }

    private Map<String, Long> metrics(PipelineRunResult result) {
        return result.getNodeMetrics().get("sa");
    }
}
