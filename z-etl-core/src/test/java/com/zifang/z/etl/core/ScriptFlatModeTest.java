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
 * 脚本 flat 模式验收：一进多出（Flink FlatMapFunction 口径）。
 *
 * <p>口径：脚本返回数组逐元素出一行 —— 元素是 Map 就整行替换、标量落 {@code name} 列
 * （没声明 name 明确报错，不造魔法列）；null 元素跳过计数；返回 null 丢行计数；
 * 非数组返回值当单元素；mode=flat 与 type 互斥（类型归一下游挂 type.normalize）。
 *
 * @author zifang
 */
class ScriptFlatModeTest {

    @Test
    @DisplayName("Map 数组整行替换：一行拆成多行新结构")
    void mapElementsBecomeRows() throws Exception {
        PipelineRunResult result = run(""
                + "name: flat-maps\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {order: 'o1', items: 'a,b'}\n"
                + "  - id: flat\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: flat\n"
                + "      engine: GROOVY\n"
                + "      script: \"def parts = items.split(',')\\nparts.collect { [order: order, item: it] }\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: flat}\n"
                + "  - {from: flat, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "一行拆两行");
        assertEquals("o1", rows.get(0).get("order"));
        assertEquals("a", rows.get(0).get("item"));
        assertEquals("b", rows.get(1).get("item"));
    }

    @Test
    @DisplayName("标量数组落 name 列：原行保留 + 声明列逐元素")
    void scalarElementsLandInNamedColumn() throws Exception {
        PipelineRunResult result = run(""
                + "name: flat-scalars\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, tags: 'x|y|z'}\n"
                + "  - id: flat\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: flat\n"
                + "      engine: GROOVY\n"
                + "      name: tag\n"
                + "      script: \"tags.tokenize('|')\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: flat}\n"
                + "  - {from: flat, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(3, rows.size(), "一行拆三行");
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"), "原行保留");
        assertEquals("y", rows.get(1).get("tag"), "标量落 name 列");
    }

    @Test
    @DisplayName("null 元素跳过计数；返回 null 丢行计数；非数组返回值当单元素")
    void nullHandlingAndSingleValue() throws Exception {
        PipelineRunResult result = run(""
                + "name: flat-nulls\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, tags: 'a'}\n"
                + "        - {id: 2, tags: null}\n"
                + "        - {id: 3, tags: 'solo'}\n"
                + "  - id: flat\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: flat\n"
                + "      engine: GROOVY\n"
                + "      name: tag\n"
                + "      script: \"tags == null ? null : (tags == 'solo' ? tags : [tags, null])\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: flat}\n"
                + "  - {from: flat, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "'a' 一行 + 'solo' 非数组当单元素一行；null 结果行不产出");
        assertEquals("a", rows.get(0).get("tag"));
        assertEquals("solo", rows.get(1).get("tag"), "非数组返回值当单元素");
        Map<String, Long> metrics = result.getNodeMetrics().get("flat");
        assertEquals(Long.valueOf(1L), metrics.get("flat-null-result"), "tags=null ⇒ 脚本返回 null ⇒ 丢行计数");
        assertEquals(Long.valueOf(1L), metrics.get("flat-null-element"), "数组里的 null 元素跳过计数");
    }

    @Test
    @DisplayName("校验：mode=flat 声明 type 报错；标量元素没声明 name 明确报错")
    void validation() throws Exception {
        IllegalArgumentException typeConflict = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: flat-broken1\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: flat\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: flat\n"
                + "      engine: GROOVY\n"
                + "      name: v\n"
                + "      type: STRING\n"
                + "      script: \"[a]\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: flat}\n"
                + "  - {from: flat, to: out}\n"));
        assertTrue(typeConflict.getMessage().contains("type"), typeConflict.getMessage());

        IllegalStateException missingName = assertThrows(IllegalStateException.class, () -> run(""
                + "name: flat-broken2\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{a: 1}]\n"
                + "  - id: flat\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: flat\n"
                + "      engine: GROOVY\n"
                + "      script: \"[a]\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: flat}\n"
                + "  - {from: flat, to: out}\n"));
        assertTrue(missingName.getMessage().contains("name"), missingName.getMessage());
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
