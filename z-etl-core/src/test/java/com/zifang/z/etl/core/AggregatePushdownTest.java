package com.zifang.z.etl.core;

import com.zifang.z.etl.core.optimize.JdbcTableSchemaResolver;
import com.zifang.z.etl.core.optimize.PlanOptimizer;
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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合下推验收（DAG 层）：源库算 GROUP BY，引擎不重复算。
 *
 * <p>口径：**下推由用户在源节点显式声明**（像 where 一样写死在源上），引擎不猜。
 *
 * @author zifang
 */
class AggregatePushdownTest {

    private EtlTestFixtures fixtures;
    private MapDataSourceProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("aggpush");
        fixtures.createSchema();
        fixtures.seedSource(10, 1L, new Timestamp(1767225600000L));
        provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
    }

    /** 带聚合下推的声明：源声明 GROUP BY，下游有等价的 aggregate 节点。 */
    private static final String SPEC = ""
            + "name: agg-push\n"
            + "nodes:\n"
            + "  - id: src\n"
            + "    type: jdbc.source\n"
            + "    config:\n"
            + "      dataSourceRef: ds-oc-main\n"
            + "      table: z_trade_order\n"
            + "      pushdown:\n"
            + "        groupBy: [status]\n"
            + "        aggregates:\n"
            + "          - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
            + "          - {fn: SUM, column: amount, alias: total_amount}\n"
            + "  - id: agg\n"
            + "    type: aggregate\n"
            + "    config:\n"
            + "      keyBy: [status]\n"
            + "      aggregates:\n"
            + "        - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
            + "        - {fn: SUM, column: amount, alias: total_amount}\n"
            + "  - id: sink\n"
            + "    type: collect.sink\n"
            + "edges:\n"
            + "  - {from: src, to: agg}\n"
            + "  - {from: agg, to: sink}\n";

    /** 源**没有**声明聚合下推的声明。 */
    private static final String SPEC_WITHOUT_PUSHDOWN = ""
            + "name: agg-push\n"
            + "nodes:\n"
            + "  - id: src\n"
            + "    type: jdbc.source\n"
            + "    config:\n"
            + "      dataSourceRef: ds-oc-main\n"
            + "      table: z_trade_order\n"
            + "  - id: agg\n"
            + "    type: aggregate\n"
            + "    config:\n"
            + "      keyBy: [status]\n"
            + "      aggregates:\n"
            + "        - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
            + "        - {fn: SUM, column: amount, alias: total_amount}\n"
            + "  - id: sink\n"
            + "    type: collect.sink\n"
            + "edges:\n"
            + "  - {from: src, to: agg}\n"
            + "  - {from: agg, to: sink}\n";

    @Test
    @DisplayName("下推生效：aggregate 节点被摘掉，结果与 SQL GROUP BY 逐字段一致")
    void aggregateIsPushedToSource() throws Exception {
        PipelineSpec spec = PipelineParser.parse(SPEC);
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(spec);
        assertNull(optimized.spec().node("agg"), "引擎侧 aggregate 应被摘掉");
        assertTrue(notes(optimized).contains("AggregatePushdown"), "备注应说明: " + notes(optimized));

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = new PipelineExecutor(provider, new HeapStateBackend())
                .execute(PipelineParser.parse(SPEC), captured);
        List<Map<String, Object>> rows = collectRows(captured);

        // 与源库 GROUP BY 对照
        Map<Integer, long[]> expected = sqlGroupBy();
        assertEquals(expected.size(), rows.size(), "行数应等于分组数: " + rows);
        for (Map<String, Object> row : rows) {
            int status = ((Number) row.get("status")).intValue();
            long[] values = expected.get(Integer.valueOf(status));
            assertNotNull(values, "分组不该多出来: " + status);
            assertEquals(values[0], ((Number) row.get("order_cnt")).longValue(), "计数应一致: " + row);
            assertEquals(0, new BigDecimal(values[1]).compareTo((BigDecimal) row.get("total_amount")),
                    "金额应一致: " + row);
        }
        assertEquals(3L, result.getRecordsRead(), "读到的应是聚合结果行（3 个分组），不是 10 行明细");
    }

    @Test
    @DisplayName("源没声明聚合下推 ⇒ 不摘 aggregate（引擎侧照常算）")
    void keepsAggregateWithoutPushdown() {
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(SPEC_WITHOUT_PUSHDOWN));
        assertNotNull(optimized.spec().node("agg"), "没声明下推就不该摘");
    }

    @Test
    @DisplayName("源声明的聚合与节点不一致 ⇒ 不摘（只提示）")
    void keepsAggregateWhenMismatch() {
        String yaml = SPEC.replace("{fn: SUM, column: amount, alias: total_amount}\n"
                + "  - id: agg", "{fn: SUM, column: amount, alias: sum_amount}\n"
                + "  - id: agg");
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml));
        assertNotNull(optimized.spec().node("agg"), "不一致就不该摘");
        assertTrue(notes(optimized).contains("不一致"), "备注应提示: " + notes(optimized));
    }

    @Test
    @DisplayName("下游用了下推结果里没有的真实列 ⇒ 不摘（防静默丢数）")
    void keepsAggregateWhenDownstreamNeedsLostColumn() {
        String yaml = ""
                + "name: agg-push\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      pushdown:\n"
                + "        groupBy: [status]\n"
                + "        aggregates:\n"
                + "          - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [status]\n"
                + "      aggregates:\n"
                + "        - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "  - id: calc\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns:\n"
                + "        - {target: cnt2, expression: \"order_cnt * 2 + amount\"}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: agg}\n"
                + "  - {from: agg, to: calc}\n"
                + "  - {from: calc, to: sink}\n";
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml),
                new JdbcTableSchemaResolver(provider));
        assertNotNull(optimized.spec().node("agg"), "下游用了真实列 amount ⇒ 不该摘");
        assertTrue(notes(optimized).contains("amount"), "备注应说明缺哪列: " + notes(optimized));
    }

    @Test
    @DisplayName("下游是整行脚本（引用不可知）⇒ 不摘")
    void keepsAggregateWhenDownstreamIsOpaque() {
        String yaml = ""
                + "name: agg-push\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      pushdown:\n"
                + "        groupBy: [status]\n"
                + "        aggregates:\n"
                + "          - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [status]\n"
                + "      aggregates:\n"
                + "        - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "  - id: clean\n"
                + "    type: script\n"
                + "    config:\n"
                + "      mode: row\n"
                + "      script: \"[k: row.status, v: row.order_cnt]\"\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: agg}\n"
                + "  - {from: agg, to: clean}\n"
                + "  - {from: clean, to: sink}\n";
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml),
                new JdbcTableSchemaResolver(provider));
        assertNotNull(optimized.spec().node("agg"), "脚本引用不可知 ⇒ 不该摘");
        assertTrue(notes(optimized).contains("不可知"), "备注应说明: " + notes(optimized));
    }

    @Test
    @DisplayName("下游只用下推结果里有的列 ⇒ 照常摘")
    void pushesWhenDownstreamOnlyUsesAvailableColumns() {
        String yaml = ""
                + "name: agg-push\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      pushdown:\n"
                + "        groupBy: [status]\n"
                + "        aggregates:\n"
                + "          - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [status]\n"
                + "      aggregates:\n"
                + "        - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "  - id: calc\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns:\n"
                + "        - {target: cnt2, expression: \"order_cnt * 2\"}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: agg}\n"
                + "  - {from: agg, to: calc}\n"
                + "  - {from: calc, to: sink}\n";
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(PipelineParser.parse(yaml),
                new JdbcTableSchemaResolver(provider));
        assertNull(optimized.spec().node("agg"), "下游只用得到的列 ⇒ 该摘");
    }

    @Test
    @DisplayName("非法下推声明在跑之前就被拦下（函数白名单 / 列白名单 / 与水位互斥）")
    void rejectsIllegalPushdown() {
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                run(SPEC.replace("{fn: SUM, column: amount, alias: total_amount}",
                        "{fn: GROUP_CONCAT, column: amount, alias: total_amount}"));
            }
        });
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                run(SPEC.replace("groupBy: [status]", "groupBy: [status; DROP TABLE x]"));
            }
        });
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                run(SPEC.replace("      table: z_trade_order",
                        "      table: z_trade_order\n      eventTimeColumn: updated_at"));
            }
        });
    }

    /** 跑一条声明（校验期的异常原样抛出）。 */
    private void run(String yaml) {
        try {
            new PipelineExecutor(provider, new HeapStateBackend()).execute(PipelineParser.parse(yaml));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 源库 GROUP BY 的对照数据。 */
    private Map<Integer, long[]> sqlGroupBy() throws Exception {
        Map<Integer, long[]> result = new LinkedHashMap<Integer, long[]>();
        Connection connection = fixtures.source().getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT status, COUNT(*) AS cnt, SUM(amount) AS total FROM z_trade_order GROUP BY status");
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                result.put(Integer.valueOf(rs.getInt(1)),
                        new long[]{rs.getLong(2), rs.getBigDecimal(3).longValue()});
            }
            rs.close();
            statement.close();
        } finally {
            connection.close();
        }
        return result;
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
}
