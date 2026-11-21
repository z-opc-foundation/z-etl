package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.runtime.PipelineStateStore;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CEP 状态迁移兼容性验收：部分匹配进键控状态 ⇒ 快照 → 恢复 → 接着配成。
 *
 * <p>口径：跑一半（create 已配、pay 未到）→ 整个执行器关掉、状态序列化进存储 →
 * 新执行器 + 新状态后端从快照恢复（{@code restored} 备注）→ pay 来了照常配成，
 * 产出行带着恢复前配到的 {@code create.*}。Partial/StepMatch 的
 * {@code serialVersionUID} 固定为 1 —— 字段增改不破坏老快照的反序列化。
 *
 * @author zifang
 */
class CepStateMigrationTest {

    private static final String SPEC = ""
            + "nodes:\n"
            + "  - id: src\n"
            + "    type: inline.source\n"
            + "    config:\n"
            + "%s"
            + "  - id: cep\n"
            + "    type: cep.pattern\n"
            + "    config:\n"
            + "      keyBy: [order_id]\n"
            + "      steps:\n"
            + "        - {name: create, expression: \"type == 'CREATE'\"}\n"
            + "        - {name: pay, expression: \"type == 'PAY'\", optional: true}\n"
            + "  - id: out\n"
            + "    type: collect.sink\n"
            + "edges:\n"
            + "  - {from: src, to: cep}\n"
            + "  - {from: cep, to: out}\n";

    private static String rows(String row) {
        return "      rows:\n        - " + row + "\n";
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    /** 每个测试用独立管道名：InMemoryPipelineStateStore 的静态文本镜像会串同名快照。 */
    private PipelineRunResult run(String name, String yaml, HeapStateBackend backend,
                                  PipelineStateStore store) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), backend, new com.zifang.z.etl.core.transform.ScriptSandbox(),
                new InMemoryCheckpointStore(), store)
                .execute(PipelineParser.parse("name: " + name + "\n" + yaml), captured);
    }

    @Test
    @DisplayName("跑一半 → 快照 → 新执行器恢复 → pay 来了接着配成，create.* 从恢复状态里带出来")
    void resumeMidPattern() throws Exception {
        InMemoryPipelineStateStore store = new InMemoryPipelineStateStore();
        // pay 不是 optional ⇒ 不会在段尾被封口，真正的「跑一半」
        String spec = SPEC.replace(", optional: true}", "}");

        // 第一段：只来 create，部分匹配留在键控状态里，执行器关掉时快照落库
        PipelineRunResult first = run("cep-mid", String.format(spec, rows(
                "{order_id: 1, type: CREATE, amount: 10}")), new HeapStateBackend(), store);
        assertEquals(0, rowsOf(first).size(), "pay 没来不该产出");
        assertNotNull(store.load("cep-mid"), "状态快照落库");
        assertTrue(first.getNotes().stream().noneMatch(note -> note.contains("恢复")),
                "第一段没有恢复备注: " + first.getNotes());

        // 第二段：全新状态后端从快照恢复，pay 来了接着配成
        PipelineRunResult second = run("cep-mid", String.format(spec, rows(
                "{order_id: 1, type: PAY}")), new HeapStateBackend(), store);
        assertTrue(second.getNotes().stream().anyMatch(note -> note.contains("恢复")),
                "第二段应带恢复备注: " + second.getNotes());
        List<Map<String, Object>> rows = rowsOf(second);
        assertEquals(1, rows.size(), "恢复后配成");
        assertEquals("CREATE", rows.get(0).get("create.type"), "create.* 从恢复状态带出来");
        assertEquals(Integer.valueOf(10), rows.get(0).get("create.amount"));
        assertEquals("PAY", rows.get(0).get("pay.type"));
    }

    @Test
    @DisplayName("optional 拖尾 + 恢复：第二段键 1 的老匹配与新键 2 的匹配都在流尾封口")
    void resumeWithOptionalTailSealed() throws Exception {
        InMemoryPipelineStateStore store = new InMemoryPipelineStateStore();
        run("cep-tail", String.format(SPEC, rows("{order_id: 1, type: CREATE}")), new HeapStateBackend(), store);

        // 第二段：恢复后键 1 的拖尾匹配与新键 2 的匹配都在流尾封口产出
        PipelineRunResult second = run("cep-tail", String.format(SPEC, rows(
                "{order_id: 2, type: CREATE}")), new HeapStateBackend(), store);
        List<Map<String, Object>> rows = rowsOf(second);
        assertEquals(2, rows.size(), "恢复的键 1 + 新键 2 各封口一份");
        assertEquals(Integer.valueOf(1), rows.get(0).get("order_id"), "恢复的键先封口");
        assertEquals(Integer.valueOf(0), rows.get(0).get("pay#"));
        assertEquals(Integer.valueOf(2), rows.get(1).get("order_id"));
        assertEquals(Integer.valueOf(0), rows.get(1).get("pay#"));
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }
}
