package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.etl.core.spi.PipelineRefResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 子管道引用（{@code pipeline.ref}）验收：管道当视图复用。
 *
 * <p>口径：子管道的汇全部换成 collect（只取产出不落地）、不读不写水位与状态、
 * params 透传占位符、嵌套深度防护、缺 name / 引用不存在 / 无解析器都明确报错。
 *
 * @author zifang
 */
class PipelineRefTest {

    @TempDir
    Path temp;

    private final DataSourceProvider provider = new MapDataSourceProvider();

    /** 手写的名字 → 定义表（引擎层由落库管道清单实现，口径一致）。 */
    private PipelineRefResolver resolver(Map<String, PipelineSpec> pipelines) {
        return new PipelineRefResolver() {
            @Override
            public PipelineSpec resolve(String name) {
                return pipelines.get(name);
            }
        };
    }

    private PipelineRunResult run(String yaml, Map<String, PipelineSpec> pipelines,
                                  Map<String, Operator> captured) throws Exception {
        PipelineSpec spec = PipelineParser.parse(yaml);
        PipelineExecutor executor = new PipelineExecutor(provider, new HeapStateBackend(),
                new com.zifang.z.etl.core.transform.ScriptSandbox(), null, null, null, resolver(pipelines));
        return executor.execute(spec, captured);
    }

    @Test
    @DisplayName("管道当视图：引用已登记管道，产出接到下游继续加工")
    void refAsView() throws Exception {
        Map<String, PipelineSpec> pipelines = new LinkedHashMap<String, PipelineSpec>();
        pipelines.put("clean", PipelineParser.parse(""
                + "name: clean\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, name: 甲}\n"
                + "        - {id: 2, name: 乙}\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings: [{source: id, target: order_id}, {source: name, target: customer}]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: out}\n"));
        String parent = ""
                + "name: parent\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config:\n"
                + "      name: clean\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(parent, pipelines, captured);
        List<Map<String, Object>> rows = rows(captured);
        assertEquals(2, rows.size());
        assertEquals(Integer.valueOf(1), ((Number) rows.get(0).get("order_id")).intValue(),
                "子管道的 field.mapping 改名列对引用方生效: " + rows.get(0));
        assertEquals("甲", rows.get(0).get("customer"));
        assertEquals(2L, result.getRecordsWritten());
    }

    @Test
    @DisplayName("视图口径：子管道的汇被替换成 collect，文件不会真的落盘")
    void subSinksReplaced() throws Exception {
        Path csv = temp.resolve("should-not-exist.csv");
        Map<String, PipelineSpec> pipelines = new LinkedHashMap<String, PipelineSpec>();
        pipelines.put("with-sink", PipelineParser.parse(""
                + "name: with-sink\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "        - {id: 2}\n"
                + "  - id: out\n"
                + "    type: file.csv.sink\n"
                + "    config:\n"
                + "      path: " + csv + "\n"
                + "edges:\n"
                + "  - {from: src, to: out}\n"));
        String parent = ""
                + "name: parent\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config:\n"
                + "      name: with-sink\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(parent, pipelines, captured);
        assertEquals(2, rows(captured).size());
        assertFalse(Paths.get(csv.toUri()).toFile().exists(), "子管道的汇应被替换，不该真的落盘");
    }

    @Test
    @DisplayName("参数透传：params 填子管道占位符，同一份定义跑不同批次")
    void paramsPassThrough() throws Exception {
        Map<String, PipelineSpec> pipelines = new LinkedHashMap<String, PipelineSpec>();
        pipelines.put("by-status", PipelineParser.parse(""
                + "name: by-status\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, status: PAID}\n"
                + "        - {id: 2, status: OPEN}\n"
                + "        - {id: 3, status: PAID}\n"
                + "  - id: f\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"status == '${status}'\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: f}\n"
                + "  - {from: f, to: out}\n"));
        String parent = ""
                + "name: parent\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config:\n"
                + "      name: by-status\n"
                + "      params: {status: PAID}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(parent, pipelines, captured);
        List<Map<String, Object>> rows = rows(captured);
        assertEquals(2, rows.size(), "params 应替换子管道里的 ${status} 占位符");
        assertEquals(Integer.valueOf(1), ((Number) rows.get(0).get("id")).intValue());
        assertEquals(Integer.valueOf(3), ((Number) rows.get(1).get("id")).intValue());
    }

    @Test
    @DisplayName("自引用成环：嵌套深度超限直接报错，不无限递归")
    void selfReferenceBlocked() throws Exception {
        String loop = ""
                + "name: loop\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config:\n"
                + "      name: loop\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: out}\n";
        Map<String, PipelineSpec> pipelines = new LinkedHashMap<String, PipelineSpec>();
        pipelines.put("loop", PipelineParser.parse(loop));
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> run(loop, pipelines, captured));
        assertTrue(error.getMessage().contains("嵌套"), "应点名嵌套超限: " + error.getMessage());
    }

    @Test
    @DisplayName("缺 name / 引用不存在 / 无解析器，跑之前就报错")
    void rejectsBrokenConfigs() {
        String noName = ""
                + "name: broken\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: out}\n";
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                () -> run(noName, new LinkedHashMap<String, PipelineSpec>(), new LinkedHashMap<String, Operator>()));
        assertTrue(missing.getMessage().contains("name"), "缺 name 应报错: " + missing.getMessage());

        String unknown = noName.replace("config: {}", "config: {name: 幽灵}");
        IllegalArgumentException absent = assertThrows(IllegalArgumentException.class,
                () -> run(unknown, new LinkedHashMap<String, PipelineSpec>(), new LinkedHashMap<String, Operator>()));
        assertTrue(absent.getMessage().contains("不存在"), "引用不存在的管道应报错: " + absent.getMessage());

        String named = unknown.replace("name: 幽灵", "name: any");
        PipelineSpec spec = PipelineParser.parse(named);
        PipelineExecutor bare = new PipelineExecutor(provider, new HeapStateBackend());
        IllegalStateException noResolver = assertThrows(IllegalStateException.class,
                () -> bare.execute(spec, new LinkedHashMap<String, Operator>()));
        assertTrue(noResolver.getMessage().contains("PipelineRefResolver"),
                "无解析器应报错说明: " + noResolver.getMessage());
    }

    @Test
    @DisplayName("嵌套引用：子管道还能再引用管道，产出层层传递")
    void nestedRefs() throws Exception {
        Map<String, PipelineSpec> pipelines = new LinkedHashMap<String, PipelineSpec>();
        pipelines.put("base", PipelineParser.parse(""
                + "name: base\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {v: 1}\n"
                + "        - {v: 2}\n"
                + "        - {v: 3}\n"
                + "  - id: f\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"v > 1\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: f}\n"
                + "  - {from: f, to: out}\n"));
        pipelines.put("mid", PipelineParser.parse(""
                + "name: mid\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config:\n"
                + "      name: base\n"
                + "  - id: m\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns: [{name: double, expression: \"v * 2\"}]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: m}\n"
                + "  - {from: m, to: out}\n"));
        String parent = ""
                + "name: parent\n"
                + "nodes:\n"
                + "  - id: ref\n"
                + "    type: pipeline.ref\n"
                + "    config:\n"
                + "      name: mid\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: ref, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(parent, pipelines, captured);
        List<Map<String, Object>> rows = rows(captured);
        assertEquals(2, rows.size());
        assertEquals(Integer.valueOf(4), ((Number) rows.get(0).get("double")).intValue(),
                "base 过滤 + mid 加倍应层层生效: " + rows.get(0));
    }

    private List<Map<String, Object>> rows(Map<String, Operator> captured) {
        Operator sink = captured.get("out");
        assertNotNull(sink, "应捕获到汇算子");
        return ((SinkOperators.CollectSink) sink).rows();
    }
}
