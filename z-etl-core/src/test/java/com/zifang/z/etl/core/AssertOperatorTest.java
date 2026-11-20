package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据质量断言验收（{@code assert} 算子）。
 *
 * <p>口径：逐行判定（表达式 / 区间 / 允许值 / 唯一性）+ 流结束结算（行数 / 非空率）；
 * 违规处置三选一：失败 / 走旁路 / 只计数。
 *
 * @author zifang
 */
class AssertOperatorTest {

    /** 两行合规数据（id 唯一、code 非空、金额 100/200）。 */
    private static final String ROWS_OK = ""
            + "      rows:\n"
            + "        - {id: 1, code: A, amount: 100, status: 1}\n"
            + "        - {id: 2, code: B, amount: 200, status: 0}\n";

    /** 主键重复的数据（唯一性校验用）。 */
    private static final String ROWS_DUP = ""
            + "      rows:\n"
            + "        - {id: 1, code: A, amount: 100, status: 1}\n"
            + "        - {id: 1, code: B, amount: 200, status: 0}\n";

    /** 有一行缺 code（非空率校验用）。 */
    private static final String ROWS_MISSING = ""
            + "      rows:\n"
            + "        - {id: 1, code: A, amount: 100, status: 1}\n"
            + "        - {id: 2, amount: 200, status: 0}\n";

    @Test
    @DisplayName("全部合规：行原样流下去")
    void passesValidRows() throws Exception {
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(ROWS_OK,
                "    config:\n"
                        + "      expression: \"amount > 0\"\n"
                        + "      inRange: {amount: [0, 1000]}\n"
                        + "      allowedValues: {status: [0, 1]}\n"
                        + "      minRows: 2\n", "", ""), captured);
        assertEquals(2L, result.getRecordsWritten());
        List<Map<String, Object>> rows = collectRows(captured, "sink");
        assertEquals(2, rows.size());
        assertEquals("A", rows.get(0).get("code"));
    }

    @Test
    @DisplayName("默认 fail：表达式不成立 ⇒ 整条流水线失败")
    void failsOnViolation() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        try {
                            run(pipeline(ROWS_OK, "    config:\n      expression: \"amount > 150\"\n", "", ""),
                                    new LinkedHashMap<String, Operator>());
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
        assertTrue(rootMessage(error).contains("表达式不成立"), "应说明违规原因，实际: " + rootMessage(error));
        assertTrue(rootMessage(error).contains("断言节点 gate"), "应点名是哪个断言节点，实际: " + rootMessage(error));
    }

    @Test
    @DisplayName("mode=reject：违规行走旁路，好数据照常流")
    void rejectsViolatingRows() throws Exception {
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(ROWS_OK,
                "    config:\n"
                        + "      mode: reject\n"
                        + "      rejectedTo: dead\n"
                        + "      expression: \"amount > 150\"\n",
                "  - id: dead\n    type: collect.sink\n",
                ""), captured);
        assertEquals(2L, result.getRecordsRead());
        // written 是所有汇的合计：主链路 1 行 + 旁路 1 行
        assertEquals(2L, result.getRecordsWritten(), "主链路 1 行 + 旁路 1 行");
        List<Map<String, Object>> good = collectRows(captured, "sink");
        List<Map<String, Object>> bad = collectRows(captured, "dead");
        assertEquals(1, good.size());
        assertEquals("B", good.get(0).get("code"));
        assertEquals(1, bad.size(), "违规行不该静默丢");
        assertEquals("A", bad.get(0).get("code"));
    }

    @Test
    @DisplayName("mode=count：只计数不处置")
    void countsViolations() throws Exception {
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(ROWS_OK,
                "    config:\n"
                        + "      mode: count\n"
                        + "      expression: \"amount > 150\"\n", "", ""), captured);
        assertEquals(2L, result.getRecordsWritten(), "count 模式下行照常流");
        Map<String, Long> metrics = result.getNodeMetrics().get("gate");
        assertNotNull(metrics, "应能拿到节点指标，实际: " + result.getNodeMetrics());
        assertEquals(Long.valueOf(1L), metrics.get("violations"), "违规数应记在指标里，实际: " + metrics);
    }

    @Test
    @DisplayName("结算期判定：行数不足 ⇒ 流结束时失败")
    void settlesRowCount() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        try {
                            run(pipeline(ROWS_OK, "    config:\n      minRows: 5\n", "", ""),
                                    new LinkedHashMap<String, Operator>());
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
        assertTrue(rootMessage(error).contains("行数"), "应说明行数不达标，实际: " + rootMessage(error));
    }

    @Test
    @DisplayName("唯一性：撞上重复值即失败")
    void detectsDuplicates() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        try {
                            run(pipeline(ROWS_DUP, "    config:\n      unique: [id]\n", "", ""),
                                    new LinkedHashMap<String, Operator>());
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
        assertTrue(rootMessage(error).contains("重复"), "应说明重复，实际: " + rootMessage(error));
    }

    @Test
    @DisplayName("非空率：流结束时结算，空值超标即失败")
    void checksNotNullRate() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        try {
                            run(pipeline(ROWS_MISSING, "    config:\n      notNullRate: {code: 1.0}\n", "", ""),
                                    new LinkedHashMap<String, Operator>());
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
        assertTrue(rootMessage(error).contains("非空率"), "应说明非空率，实际: " + rootMessage(error));
    }

    @Test
    @DisplayName("mode=reject 却没声明 rejectedTo ⇒ 跑之前就报错")
    void rejectsModeWithoutRejectedTo() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        try {
                            run(pipeline(ROWS_OK,
                                    "    config:\n      mode: reject\n      expression: \"amount > 0\"\n", "", ""),
                                    new LinkedHashMap<String, Operator>());
                        } catch (Exception e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                });
        assertTrue(rootMessage(error).contains("rejectedTo"), "应提示补 rejectedTo，实际: " + rootMessage(error));
    }

    /** 断言节点 + 汇的流水线声明。 */
    private String pipeline(String rows, String gateConfig, String extraNodes, String extraEdges) {
        return "name: assert-demo\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + rows
                + "  - id: gate\n"
                + "    type: assert\n"
                + gateConfig
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + extraNodes
                + "edges:\n"
                + "  - {from: src, to: gate}\n"
                + "  - {from: gate, to: sink}\n"
                + extraEdges;
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured) throws Exception {
        PipelineSpec spec = PipelineParser.parse(yaml);
        PipelineExecutor executor = new PipelineExecutor(new MapDataSourceProvider(),
                new HeapStateBackend(), new com.zifang.z.etl.core.transform.ScriptSandbox());
        return executor.execute(spec, captured);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured, String nodeId) {
        Operator operator = captured.get(nodeId);
        assertNotNull(operator, "应捕获到汇算子 " + nodeId);
        return ((SinkOperators.CollectSink) operator).rows();
    }

    /** 最内层异常消息（断言错误常被包一层）。 */
    private static String rootMessage(Throwable error) {
        Throwable current = error;
        StringBuilder sb = new StringBuilder();
        while (current != null) {
            if (current.getMessage() != null) {
                sb.append(current.getMessage()).append('\n');
            }
            current = current.getCause();
        }
        return sb.toString();
    }
}
