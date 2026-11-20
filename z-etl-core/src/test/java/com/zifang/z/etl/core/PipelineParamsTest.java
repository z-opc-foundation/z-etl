package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcPipelineRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.pipeline.PipelineParams;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 管道参数验收（{@code ${name}} 占位符）。
 *
 * <p>口径：同一份声明跑不同批次；缺参数直接报错（不拿空串糊过去）；参数值过白名单。
 *
 * @author zifang
 */
class PipelineParamsTest {

    private EtlTestFixtures fixtures;
    private PipelineEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("params");
        fixtures.createSchema();
        fixtures.seedSource(10, 1L, new Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());
        engine = new PipelineEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, fixtures.target()),
                new JdbcPipelineRepository(fixtures.meta()),
                new InMemoryCheckpointStore(),
                null);
    }

    @Test
    @DisplayName("占位符替换：节点配置里的 ${name} 换成传入值")
    void substitutesPlaceholdersInConfig() {
        String yaml = ""
                + "name: param-demo\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: orders_${bizdate}\n"
                + "      where: \"dt = '${bizdate}'\"\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("bizdate", "2026-11-19");
        PipelineSpec spec = PipelineParser.parse(yaml, params);
        Map<String, Object> config = spec.node("src").getConfig();
        assertEquals("orders_2026-11-19", config.get("table"));
        assertEquals("dt = '2026-11-19'", config.get("where"));
    }

    @Test
    @DisplayName("内置参数：bizdate / yesterday / now 不传也能用")
    void builtinsAreAvailable() {
        String yaml = ""
                + "name: param-builtin\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      table: t_${bizdate}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        PipelineSpec spec = PipelineParser.parse(yaml, null);
        assertEquals("t_" + LocalDate.now(), spec.node("src").getConfig().get("table"));
        assertTrue(PipelineParams.builtins().containsKey("yesterday"), "应有昨天参数");
        assertTrue(PipelineParams.builtins().containsKey("now"), "应有当前时间参数");
    }

    @Test
    @DisplayName("缺参数直接报错（不拿空串糊过去）")
    void missingParamFails() {
        String yaml = ""
                + "name: param-missing\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      table: t_${noSuchParam}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        PipelineParser.parse(yaml, null);
                    }
                });
        assertTrue(error.getMessage().contains("noSuchParam"), "应点名缺哪个参数，实际: " + error.getMessage());
    }

    @Test
    @DisplayName("参数值过白名单：带引号 / 分号的值直接拒")
    void unsafeValueRejected() {
        String yaml = ""
                + "name: param-unsafe\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      where: \"dt = '${d}'\"\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("d", "2026-01-01'; DROP TABLE z_trade_order; --");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        PipelineParser.parse(yaml, params);
                    }
                });
        assertTrue(error.getMessage().contains("不合法"), "应说明值不合法，实际: " + error.getMessage());
    }

    @Test
    @DisplayName("嵌套结构（pushdown.filter / mappings 列表）里的占位符也替换")
    void substitutesNestedValues() {
        String yaml = ""
                + "name: param-nested\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      pushdown:\n"
                + "        filter: \"dt = '${bizdate}'\"\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: \"${targetCol}\", type: LONG}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: sink}\n";
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("bizdate", "2026-11-19");
        params.put("targetCol", "order_id");
        PipelineSpec spec = PipelineParser.parse(yaml, params);
        @SuppressWarnings("unchecked")
        Map<String, Object> pushdown = (Map<String, Object>) spec.node("src").getConfig().get("pushdown");
        assertEquals("dt = '2026-11-19'", pushdown.get("filter"));
        @SuppressWarnings("unchecked")
        Map<String, Object> mapping = spec.node("fm").mapList("mappings").get(0);
        assertEquals("order_id", mapping.get("target"));
    }

    @Test
    @DisplayName("落库的定义保留占位符 ⇒ 同一份声明天天跑不同批次")
    void storedDefinitionKeepsPlaceholders() throws Exception {
        String yaml = ""
                + "name: param-daily\n"
                + "mode: init\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      where: \"status = ${status}\"\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        engine.register(yaml);
        Map<String, String> paid = new LinkedHashMap<String, String>();
        paid.put("status", "1");
        Map<String, String> closed = new LinkedHashMap<String, String>();
        closed.put("status", "0");

        assertEquals(4L, engine.run("param-daily", paid).getRecordsRead(), "status=1 的行数");
        assertEquals(3L, engine.run("param-daily", closed).getRecordsRead(), "status=0 的行数");

        String stored = PipelineParser.toYaml(engine.get("param-daily"));
        assertTrue(stored.contains("${status}"), "落库的定义应保留占位符，实际: " + stored);
    }

    @Test
    @DisplayName("流水线名带占位符直接拒（名字是水位 / 状态的键）")
    void rejectsPlaceholderInName() {
        String yaml = ""
                + "name: etl_${bizdate}\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        PipelineParser.parse(yaml, null);
                    }
                });
        assertTrue(error.getMessage().contains("占位符"), "应说明名字不支持占位符，实际: " + error.getMessage());
    }

    @Test
    @DisplayName("参数化后照常跑：内联行里的占位符也被替换")
    void resolvedSpecStillRuns() throws Exception {
        String yaml = ""
                + "name: param-run\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, name: \"${who}\"}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("who", "zhangsan");
        Map<String, com.zifang.z.etl.core.runtime.Operator> captured =
                new LinkedHashMap<String, com.zifang.z.etl.core.runtime.Operator>();
        PipelineSpec spec = PipelineParser.parse(yaml, params);
        new com.zifang.z.etl.core.runtime.PipelineExecutor(new MapDataSourceProvider(),
                new com.zifang.z.etl.core.runtime.HeapStateBackend()).execute(spec, captured);
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> rows =
                ((com.zifang.z.etl.core.operator.SinkOperators.CollectSink) captured.get("sink")).rows();
        assertEquals(1, rows.size());
        assertEquals("zhangsan", rows.get(0).get("name"));
    }
}
