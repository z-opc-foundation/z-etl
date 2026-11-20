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
 * 去重重复侧输出（{@code dedup} 的 {@code duplicateTo}）验收。
 *
 * <p>口径：keep=first 时被挤掉的行原样走旁路给 {@code duplicateTo} 指向的节点 ——
 * 重复审计 / 落死信表有去处，不只丢个计数（与窗口的 {@code lateTo} 同一套旁路口径）。
 *
 * @author zifang
 */
class DedupDuplicateSideOutputTest {

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rows(String nodeId) {
        return ((SinkOperators.CollectSink) captured.get(nodeId)).rows();
    }

    private String yaml(String dedupExtraConfig) {
        return ""
                + "name: dedup-side\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order_id: 1, amount: 10}\n"
                + "        - {order_id: 1, amount: 10}\n"
                + "        - {order_id: 1, amount: 99}\n"
                + "        - {order_id: 2, amount: 20}\n"
                + "  - id: dedup\n"
                + "    type: dedup\n"
                + "    config:\n"
                + "      keyBy: [order_id]\n"
                + (dedupExtraConfig == null ? "" : "      " + dedupExtraConfig + "\n")
                + "  - id: dead\n"
                + "    type: collect.sink\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: dedup}\n"
                + "  - {from: dedup, to: out}\n";
    }

    @Test
    @DisplayName("被挤掉的行走侧输出：原行进 duplicateTo 指向的节点，主流只留首条")
    void duplicatesGoToSideOutput() throws Exception {
        PipelineRunResult result = run(yaml("keep: first\n      duplicateTo: dead"));
        List<Map<String, Object>> main = rows("out");
        List<Map<String, Object>> dead = rows("dead");
        assertEquals(2, main.size(), "两个 order_id 各留首条");
        assertEquals(Integer.valueOf(10), main.get(0).get("amount"));
        assertEquals(Integer.valueOf(20), main.get(1).get("amount"));
        assertEquals(2, dead.size(), "同键后到的两行（重复 + 新版本）都应进侧输出");
        assertEquals(Integer.valueOf(10), dead.get(0).get("amount"));
        assertEquals(Integer.valueOf(99), dead.get(1).get("amount"));
        Map<String, Long> metrics = result.getNodeMetrics().get("dedup");
        assertEquals(Long.valueOf(2L), metrics.get("dropped"));
        assertEquals(Long.valueOf(2L), metrics.get("side-output-sent"));
    }

    @Test
    @DisplayName("不声明 duplicateTo：行为不变，只丢计数")
    void withoutDuplicateToJustDrops() throws Exception {
        PipelineRunResult result = run(yaml("keep: first"));
        assertEquals(2, rows("out").size());
        assertEquals(0, rows("dead").size(), "没声明 duplicateTo ⇒ 侧输出节点应空");
        assertEquals(Long.valueOf(2L), result.getNodeMetrics().get("dedup").get("dropped"));
    }

    @Test
    @DisplayName("duplicateTo 指向不存在的节点 ⇒ 跑之前就报错")
    void duplicateToTargetMustExist() {
        String broken = yaml("keep: first\n      duplicateTo: ghost");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(broken));
        assertTrue(error.getMessage().contains("duplicateTo"), "应点名 duplicateTo: " + error.getMessage());
    }
}
