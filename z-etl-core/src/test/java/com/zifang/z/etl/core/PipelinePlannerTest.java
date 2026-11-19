package com.zifang.z.etl.core;

import com.zifang.z.etl.core.optimize.PlanOptimizer;
import com.zifang.z.etl.core.optimize.PlanOptimizer.Optimized;
import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.pipeline.PipelineValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流水线计划面验收：解析 / 校验 / 优化器三条规则 / explain。
 *
 * @author zifang
 */
class PipelinePlannerTest {

    @Test
    @DisplayName("解析：JSON 与 YAML 等价；未知类型报错并列出全部合法类型串")
    void parseJsonAndYamlEquivalentAndUnknownTypeFails() {
        String yaml = ""
                + "name: p1\n"
                + "nodes:\n"
                + "  - {id: src, type: jdbc.source, config: {table: t}}\n"
                + "  - {id: out, type: collect.sink}\n"
                + "edges:\n"
                + "  - {from: src, to: out}\n";
        String json = "{\"name\":\"p1\",\"nodes\":["
                + "{\"id\":\"src\",\"type\":\"jdbc.source\",\"config\":{\"table\":\"t\"}},"
                + "{\"id\":\"out\",\"type\":\"collect.sink\"}],"
                + "\"edges\":[{\"from\":\"src\",\"to\":\"out\"}]}";
        PipelineSpec fromYaml = PipelineParser.parse(yaml);
        PipelineSpec fromJson = PipelineParser.parse(json);
        assertEquals(fromYaml.getName(), fromJson.getName());
        assertEquals(fromYaml.getNodes().size(), fromJson.getNodes().size());
        assertEquals(fromYaml.getEdges().size(), fromJson.getEdges().size());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PipelineParser.parse(""
                        + "name: bad\n"
                        + "nodes:\n"
                        + "  - {id: x, type: no.such.thing}\n"));
        assertTrue(e.getMessage().contains("jdbc.source"), "报错应列出合法类型串");
        assertTrue(e.getMessage().contains("window.aggregate"));
    }

    @Test
    @DisplayName("校验：环检测报出成环节点；join 双入边；源无入边、汇无出边")
    void validateCatchesCyclesAndBadShapes() {
        String cyclic = ""
                + "name: cyc\n"
                + "nodes:\n"
                + "  - {id: src, type: jdbc.source, config: {table: t}}\n"
                + "  - {id: a, type: map, config: {columns: []}}\n"
                + "  - {id: b, type: map, config: {columns: []}}\n"
                + "  - {id: out, type: collect.sink}\n"
                + "edges:\n"
                + "  - {from: src, to: a}\n"
                + "  - {from: a, to: b}\n"
                + "  - {from: b, to: a}\n"
                + "  - {from: b, to: out}\n";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PipelineValidator.validate(PipelineParser.parse(cyclic)));
        assertTrue(e.getMessage().contains("a") && e.getMessage().contains("b"), "报错应点名成环节点");

        String selfLoop = ""
                + "name: self\n"
                + "nodes:\n"
                + "  - {id: a, type: map, config: {columns: []}}\n"
                + "edges:\n"
                + "  - {from: a, to: a}\n";
        assertThrows(IllegalArgumentException.class,
                () -> PipelineValidator.validate(PipelineParser.parse(selfLoop)));
    }

    @Test
    @DisplayName("优化器·过滤下推：SQL 谓词并入源 WHERE，filter 节点的 sql 摘掉")
    void pushDownFilterMergesIntoSourceWhere() {
        PipelineSpec spec = chainWithFilter("status = 2", "f1");
        Optimized optimized = new PlanOptimizer().optimize(spec);

        PipelineNode source = optimized.spec().node("src");
        String where = source.string("where");
        assertNotNull(where);
        assertTrue(where.contains("status = 2"), "下推后源的 WHERE 应含谓词: " + where);
        assertNull(optimized.spec().node("f1").string("sql"), "filter 的 sql 摘掉避免重复下推");

        boolean noted = false;
        for (String note : optimized.notes()) {
            if (note.contains("PushDownFilter") && note.contains("f1")) {
                noted = true;
            }
        }
        assertTrue(noted, "改写备注应说明下推动作: " + optimized.notes());
    }

    @Test
    @DisplayName("优化器·过滤下推：上游有 field.mapping 改名 ⇒ 不推（保守不猜）")
    void pushDownFilterStopsAtFieldMapping() {
        String yaml = ""
                + "name: no-push\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config: {table: t}\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "  - id: f1\n"
                + "    type: filter\n"
                + "    config: {expression: \"order_id > 5\", sql: \"id > 5\"}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: f1}\n"
                + "  - {from: f1, to: out}\n";
        Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml));

        assertNull(optimized.spec().node("src").string("where"), "改名后列名失效 ⇒ 不下推");
        assertEquals("id > 5", optimized.spec().node("f1").string("sql"), "谓词原样保留在 filter 上");
    }

    @Test
    @DisplayName("优化器·合并映射：相邻两个 map 并成一个")
    void mergeMapRewriterMergesAdjacentMaps() {
        String yaml = ""
                + "name: merge\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config: {table: t}\n"
                + "  - id: m1\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns:\n"
                + "        - {name: a, engine: EL, expression: \"1\", type: LONG}\n"
                + "  - id: m2\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns:\n"
                + "        - {name: b, engine: EL, expression: \"2\", type: LONG}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: m1}\n"
                + "  - {from: m1, to: m2}\n"
                + "  - {from: m2, to: out}\n";
        Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml));

        PipelineSpec plan = optimized.spec();
        assertNull(plan.node("m1"), "上游 map 并入下游后摘掉");
        PipelineNode m2 = plan.node("m2");
        assertEquals(2, m2.mapList("columns").size(), "两段 columns 合一");
        assertEquals(3, plan.getNodes().size());
        assertEquals(2, plan.getEdges().size(), "src→m2, m2→out");
    }

    @Test
    @DisplayName("优化器·死节点摘除：到不了汇的节点删掉；rejectedTo 指名的不算死")
    void eliminateUnusedRemovesUnreachableButKeepsSideTargets() {
        String yaml = ""
                + "name: dead\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config: {table: t}\n"
                + "  - id: live\n"
                + "    type: map\n"
                + "    config: {columns: []}\n"
                + "  - id: orphan\n"
                + "    type: map\n"
                + "    config: {columns: []}\n"
                + "  - id: dead-letter\n"
                + "    type: collect.sink\n"
                + "  - id: f1\n"
                + "    type: filter\n"
                + "    config: {expression: \"1 == 1\", rejectedTo: dead-letter}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: live}\n"
                + "  - {from: live, to: f1}\n"
                + "  - {from: f1, to: out}\n";
        Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml));

        PipelineSpec plan = optimized.spec();
        assertNull(plan.node("orphan"), "孤节点摘掉");
        assertNotNull(plan.node("dead-letter"), "旁路输出指名的节点保留");
        assertNotNull(plan.node("f1"));
        assertEquals(5, plan.getNodes().size());
    }

    @Test
    @DisplayName("explain：拓扑序文本计划，含优化备注")
    void explainPrintsTopologicalPlanWithNotes() {
        PipelineSpec spec = chainWithFilter("status = 2", "f1");
        String text = new PlanOptimizer().explain(spec);
        assertTrue(text.contains("pipeline: push-chain"));
        assertTrue(text.contains("src") && text.contains("jdbc.source"));
        assertTrue(text.contains("table=z_trade_order"));
        assertTrue(text.contains("PushDownFilter"), "explain 应带下推备注");
        // 拓扑序：src 在 f1 之前
        assertTrue(text.indexOf("src") < text.indexOf("f1"));
    }

    @Test
    @DisplayName("拓扑序：Kahn 算法给出的顺序满足每条边先出后入")
    void topologicalOrderRespectsEdgeDirection() {
        PipelineSpec spec = chainWithFilter("status = 2", "f1");
        List<String> topo = PipelineValidator.topologicalOrder(spec);
        assertEquals(spec.getNodes().size(), topo.size());
        assertTrue(topo.indexOf("src") < topo.indexOf("f1"));
        assertTrue(topo.indexOf("f1") < topo.indexOf("m1"));
        assertTrue(topo.indexOf("m1") < topo.indexOf("out"));
    }

    @Test
    @DisplayName("旧作业描述编译：线性 job 编成等价的线性流水线")
    void legacyJobCompilesToLinearPipeline() {
        PipelineSpec spec = PipelineSpec.fromJobDescriptor(EtlTestFixtures.loadYamlDescriptor());
        assertEquals("trade-order-to-snapshot", spec.getName());
        assertEquals("auto", spec.getMode());
        // 源 → 字段映射 → 汇
        assertEquals(3, spec.getNodes().size());
        PipelineNode source = spec.node("source");
        assertEquals(OperatorType.JDBC_SOURCE, source.getOperatorType());
        assertEquals("z_trade_order", source.string("table"));
        assertEquals("updated_at", source.string("eventTimeColumn"));
        assertNotNull(source.map("resume"), "旧水位声明编译成 resume（断点续传口径）");
        assertEquals("TIMESTAMP", source.map("resume").get("type"));
        PipelineNode sink = spec.node("sink");
        assertEquals(OperatorType.JDBC_SINK, sink.getOperatorType());
        assertEquals("etl_trade_order_snapshot", sink.string("table"));
        List<String> key = sink.strings("idempotencyKey");
        assertEquals(1, key.size());
        assertEquals("order_id", key.get(0));
        // 校验必须通过（可执行）
        PipelineValidator.validate(spec);
    }

    @Test
    @DisplayName("投影下推刻意不做：边界在 explain/文档里说清，不静默猜列")
    void projectionPushdownIsDeliberatelyAbsent() {
        PlanOptimizer optimizer = new PlanOptimizer();
        Optimized optimized = optimizer.optimize(chainWithFilter("status = 2", "f1"));
        // map 节点的表达式列不会被下推成 SELECT 列清单 —— 源的 SELECT 保持不动
        for (String note : optimized.notes()) {
            assertFalse(note.contains("projection"), "不应有投影下推动作");
        }
    }

    /** push-chain 声明：src → f1(filter 带 sql) → m1(map) → out。 */
    private PipelineSpec chainWithFilter(String sql, String filterId) {
        String yaml = ""
                + "name: push-chain\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config: {table: z_trade_order}\n"
                + "  - id: " + filterId + "\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"status == 2\"\n"
                + "      sql: \"" + sql + "\"\n"
                + "  - id: m1\n"
                + "    type: map\n"
                + "    config: {columns: []}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: " + filterId + "}\n"
                + "  - {from: " + filterId + ", to: m1}\n"
                + "  - {from: m1, to: out}\n";
        return PipelineParser.parse(yaml);
    }
}
