package com.zifang.z.etl.core;

import com.zifang.z.etl.core.optimize.PlanOptimizer;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 投影下推验收：把「下游用得上的列」回推到源库 SELECT 列表。
 *
 * <p>护栏：**优化前后跑出的行必须逐字段一致**（裁错列就是静默丢数据 ⇒ 必须有这条护栏）。
 *
 * @author zifang
 */
class ProjectionPushdownTest {

    private EtlTestFixtures fixtures;
    private MapDataSourceProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("projection");
        fixtures.createSchema();
        fixtures.seedSource(10, 1L, new Timestamp(1767225600000L));
        provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
    }

    @Test
    @DisplayName("裁剪生效：源 SELECT 列表收窄到「映射源列 + 表达式引用列」")
    void narrowsSourceColumns() {
        PipelineSpec spec = PipelineParser.parse(""
                + "name: proj\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, order_no, amount, status, updated_at]\n"
                + "  - id: paid\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"status == 1\"\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: paid}\n"
                + "  - {from: paid, to: fm}\n"
                + "  - {from: fm, to: sink}\n");

        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(spec);
        List<String> columns = optimized.spec().node("src").strings("columns");
        assertEquals(3, columns.size(), "应收窄为 3 列，实际: " + columns);
        assertTrue(columns.contains("id"), "映射源列要留: " + columns);
        assertTrue(columns.contains("amount"), "映射源列要留: " + columns);
        assertTrue(columns.contains("status"), "过滤表达式引用的列要留: " + columns);
        assertTrue(!columns.contains("order_no"), "没人用的列应被裁掉: " + columns);
        assertTrue(!columns.contains("updated_at"), "没人用且不是水位列 ⇒ 裁掉: " + columns);
    }

    @Test
    @DisplayName("水位列与平局列必保（裁掉 ⇒ 增量变全量）")
    void keepsWatermarkAndTiebreakColumns() {
        PipelineSpec spec = PipelineParser.parse(""
                + "name: proj-wm\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, amount, status, updated_at, order_no]\n"
                + "      eventTimeColumn: updated_at\n"
                + "      resume: {column: updated_at, type: TIMESTAMP, tiebreakColumn: id}\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: sink}\n");

        List<String> columns = new PlanOptimizer().optimize(spec).spec().node("src").strings("columns");
        assertTrue(columns.contains("updated_at"), "水位列必保: " + columns);
        assertTrue(columns.contains("id"), "平局列必保: " + columns);
        assertTrue(!columns.contains("order_no"), "没人用的列应被裁掉: " + columns);
    }

    @Test
    @DisplayName("源没声明列清单 ⇒ 不裁剪（全列取数，不敢猜）")
    void skipsWhenSourceHasNoColumnList() {
        PipelineSpec spec = PipelineParser.parse(""
                + "name: proj-nocols\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: sink}\n");

        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(spec);
        assertTrue(optimized.spec().node("src").strings("columns").isEmpty(), "没声明列清单就不裁");
        assertTrue(notes(optimized).contains("未声明列清单"), "备注应说明原因: " + notes(optimized));
    }

    @Test
    @DisplayName("链上没有 field.mapping ⇒ 不裁剪（下游需求不可知）")
    void skipsWithoutFieldMapping() {
        PipelineSpec spec = PipelineParser.parse(""
                + "name: proj-nofm\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, amount, status]\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n");

        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(spec);
        assertEquals(3, optimized.spec().node("src").strings("columns").size(), "不该裁");
        assertTrue(notes(optimized).contains("用得上全部列"), "备注应说明原因: " + notes(optimized));
    }

    @Test
    @DisplayName("join 在链上 ⇒ 不裁剪（左右两路列交织）")
    void skipsWithJoin() {
        PipelineSpec spec = PipelineParser.parse(""
                + "name: proj-join\n"
                + "nodes:\n"
                + "  - id: left\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, amount, status]\n"
                + "  - id: right\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, amount, status]\n"
                + "  - id: j\n"
                + "    type: join\n"
                + "    config:\n"
                + "      left: left\n"
                + "      right: right\n"
                + "      leftKey: [id]\n"
                + "      rightKey: [id]\n"
                + "      joinType: inner\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: left, to: j}\n"
                + "  - {from: right, to: j}\n"
                + "  - {from: j, to: sink}\n");

        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(spec);
        assertEquals(3, optimized.spec().node("left").strings("columns").size(), "join 链上不裁");
        assertEquals(3, optimized.spec().node("right").strings("columns").size(), "join 链上不裁");
    }

    @Test
    @DisplayName("护栏：优化前后跑出的行逐字段一致（裁错列就是静默丢数据）")
    void optimizedPlanProducesIdenticalRows() throws Exception {
        String yaml = ""
                + "name: proj-e2e\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, order_no, user_id, amount, status, payment_method, updated_at]\n"
                + "      eventTimeColumn: updated_at\n"
                + "  - id: paid\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"status == 1\"\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "        - {source: order_no, target: order_no, type: STRING}\n"
                + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                + "        - {source: status, target: status, type: LONG}\n"
                + "        - {source: updated_at, target: updated_at, type: TIMESTAMP}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: paid}\n"
                + "  - {from: paid, to: fm}\n"
                + "  - {from: fm, to: sink}\n";

        Map<String, Operator> optimizedCapture = new LinkedHashMap<String, Operator>();
        PipelineRunResult optimized = new PipelineExecutor(provider, new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), optimizedCapture);
        List<Map<String, Object>> optimizedRows = collectRows(optimizedCapture);

        // 手工把 SELECT 列表砍到「真需要的列」，再跑一次 ⇒ 结果必须一致
        Map<String, Operator> manualCapture = new LinkedHashMap<String, Operator>();
        PipelineSpec manual = PipelineParser.parse(yaml);
        manual.node("src").getConfig().put("columns",
                new java.util.ArrayList<String>(java.util.Arrays.asList("id", "order_no", "amount", "status", "updated_at")));
        PipelineRunResult manualRun = new PipelineExecutor(provider, new HeapStateBackend())
                .execute(manual, manualCapture);
        List<Map<String, Object>> manualRows = collectRows(manualCapture);

        assertEquals(optimized.getRecordsRead(), manualRun.getRecordsRead());
        assertEquals(optimizedRows.size(), manualRows.size(), "行数应一致: " + optimizedRows + " vs " + manualRows);
        for (int i = 0; i < optimizedRows.size(); i++) {
            assertEquals(manualRows.get(i).toString(), optimizedRows.get(i).toString(),
                    "第 " + i + " 行应逐字段一致");
        }
        assertTrue(optimized.getNotes().toString().contains("ProjectionPushdown"),
                "运行结果应带优化备注: " + optimized.getNotes());
    }

    private String notes(PlanOptimizer.Optimized optimized) {
        StringBuilder sb = new StringBuilder();
        for (String note : optimized.notes()) {
            sb.append(note).append('\n');
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured) {
        Operator operator = captured.get("sink");
        assertNotNull(operator, "应捕获到汇算子");
        return ((com.zifang.z.etl.core.operator.SinkOperators.CollectSink) operator).rows();
    }

    /** 源节点（取证用）。 */
    private PipelineNode source(PipelineSpec spec) {
        return spec.node("src");
    }
}
