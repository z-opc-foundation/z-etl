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
 * 窗口迟到侧输出（{@code window.aggregate} 的 {@code lateTo}）验收。
 *
 * <p>口径：窗口已触发后仍对着旧窗口来的行（超 allowedLateness）原样走旁路给
 * {@code lateTo} 指向的节点 —— 被丢的迟到行有去处，不只丢个计数。
 *
 * @author zifang
 */
class WindowLateSideOutputTest {

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rows(String nodeId) {
        Operator sink = captured.get(nodeId);
        if (sink instanceof SinkOperators.CollectSink) {
            return ((SinkOperators.CollectSink) sink).rows();
        }
        throw new IllegalStateException("节点 " + nodeId + " 不是收集汇: " + sink);
    }

    private Map<String, Long> metrics(PipelineRunResult result, String nodeId) {
        return result.getNodeMetrics().get(nodeId);
    }

    private String yaml(String waggExtraConfig, String lateRow) {
        return ""
                + "name: late-side\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {t: LONG}\n"
                + "      eventTimeColumn: t\n"
                + "      rows:\n"
                + "        - {user_id: 1, t: 100, amount: 1}\n"
                + "        - {user_id: 1, t: 250, amount: 2}\n"
                + (lateRow == null ? "" : "        - " + lateRow + "\n")
                + "  - id: wagg\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [user_id]\n"
                + "      window: {type: tumbling, size: 100ms}\n"
                + (waggExtraConfig == null ? "" : "      " + waggExtraConfig + "\n")
                + "      aggregates:\n"
                + "        - {fn: SUM, column: amount, alias: total}\n"
                + "  - id: dead\n"
                + "    type: collect.sink\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wagg}\n"
                + "  - {from: wagg, to: out}\n";
    }

    @Test
    @DisplayName("迟到行走侧输出：原行进 lateTo 指向的节点，主流照常")
    void lateRowGoesToSideOutput() throws Exception {
        // t=250 使水位到 250 ⇒ [100,200) 触发；随后 t=120 对着已触发的窗口来 ⇒ 迟到丢弃
        PipelineRunResult result = run(yaml("lateTo: dead", "{user_id: 1, t: 120, amount: 3}"));
        List<Map<String, Object>> main = rows("out");
        List<Map<String, Object>> dead = rows("dead");
        assertEquals(2, main.size(), "两个窗口各一条聚合");
        assertEquals(1, dead.size(), "迟到的行应进侧输出节点");
        assertEquals(Integer.valueOf(3), dead.get(0).get("amount"), "侧输出的是原行");
        assertEquals(Long.valueOf(120L), dead.get(0).get("t"));
        Map<String, Long> waggMetrics = metrics(result, "wagg");
        assertEquals(Long.valueOf(1L), waggMetrics.get("late-dropped"), "丢弃计数保留");
        assertEquals(Long.valueOf(1L), waggMetrics.get("side-output-sent"));
    }

    @Test
    @DisplayName("不声明 lateTo：迟到行照旧只丢计数（行为不变）")
    void withoutLateToJustDrops() throws Exception {
        PipelineRunResult result = run(yaml(null, "{user_id: 1, t: 120, amount: 3}"));
        assertEquals(2, rows("out").size());
        assertEquals(0, rows("dead").size(), "没声明 lateTo ⇒ 侧输出节点应空");
        assertEquals(Long.valueOf(1L), metrics(result, "wagg").get("late-dropped"));
    }

    @Test
    @DisplayName("allowedLateness 内的迟到不丢：重触发照旧，侧输出不收")
    void withinLatenessRefires() throws Exception {
        // allowedLateness 1s ⇒ t=120 的行在 [100,200) 触发后仍被重算重发，不算迟到丢弃
        PipelineRunResult result = run(yaml("allowedLateness: 1s\n      lateTo: dead",
                "{user_id: 1, t: 120, amount: 3}"));
        List<Map<String, Object>> main = rows("out");
        assertEquals(3, main.size(), "两次正常触发 + 一次迟到重发");
        assertEquals(0, rows("dead").size(), "allowedLateness 内不该进侧输出");
        assertEquals(Long.valueOf(0L), (long) (metrics(result, "wagg").get("late-dropped") == null
                ? 0L : metrics(result, "wagg").get("late-dropped")));
        assertEquals(Long.valueOf(1L), metrics(result, "wagg").get("late-refired"));
    }

    @Test
    @DisplayName("lateTo 指向不存在的节点 ⇒ 跑之前就报错")
    void lateToTargetMustExist() {
        String broken = yaml("lateTo: ghost", "{user_id: 1, t: 120, amount: 3}");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(broken));
        assertTrue(error.getMessage().contains("lateTo"), "应点名 lateTo: " + error.getMessage());
    }
}
