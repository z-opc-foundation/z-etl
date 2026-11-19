package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流水线引擎端到端验收（Flink 式声明管道）。
 *
 * <p>⚠️ 硬边界：全在本地 H2 内存库上跑，数据本地生成，不连生产 RDS、不在生产库建表。
 *
 * @author zifang
 */
class PipelineEndToEndTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L); // 2026-01-01 00:00:00

    private EtlTestFixtures fixtures;
    private MapDataSourceProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("pipeline");
        fixtures.createSchema();
        fixtures.seedSource(10, 1L, BASE);
        provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
    }

    private PipelineExecutor executor() {
        return new PipelineExecutor(provider, new HeapStateBackend());
    }

    @Test
    @DisplayName("声明式 DAG：filter → map → script → collect 一条链端到端跑通")
    void declarativeChainRunsEndToEnd() throws Exception {
        String yaml = ""
                + "name: filter-chain\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, order_no, status, payment_method, amount, updated_at]\n"
                + "      where: \"status IN (1, 2)\"\n"
                + "      pageSize: 4\n"
                + "  - id: f1\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"status == 1\"\n"
                + "  - id: m1\n"
                + "    type: map\n"
                + "    config:\n"
                + "      columns:\n"
                + "        - {name: amount_x2, engine: GROOVY, expression: \"amount * 2\", type: BIGDECIMAL}\n"
                + "  - id: s1\n"
                + "    type: script\n"
                + "    config:\n"
                + "      name: pay_channel\n"
                + "      engine: GROOVY\n"
                + "      script: \"def raw = payment_method\\n(raw == null ? '' : raw.toString().trim().toUpperCase())\\n\"\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: f1}\n"
                + "  - {from: f1, to: m1}\n"
                + "  - {from: m1, to: s1}\n"
                + "  - {from: s1, to: out}\n";
        PipelineSpec spec = PipelineParser.parse(yaml);

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = executor().execute(spec, captured);

        assertEquals(7L, result.getRecordsRead(), "status IN (1,2) 的 7 行");
        assertEquals(4L, result.getRecordsWritten(), "status == 1 的 4 行进 collect");

        List<Map<String, Object>> rows = collectRows(captured, "out");
        assertEquals(4, rows.size());
        for (Map<String, Object> row : rows) {
            assertEquals(1, ((Number) row.get("status")).intValue());
            assertEquals("ALIPAY", row.get("pay_channel"), "脚本清洗产出");
            assertEquals(0, ((BigDecimal) row.get("amount_x2"))
                            .compareTo(((BigDecimal) row.get("amount")).multiply(new BigDecimal("2"))),
                    "map 表达式 amount * 2");
        }
        assertNotNull(result.getNodeMetrics().get("src"));
    }

    @Test
    @DisplayName("键控聚合：按 user_id 滚动累计，终值与 SQL GROUP BY 一致")
    void keyedAggregateMatchesSqlGroupBy() throws Exception {
        fixtures.seedSource(990, 11L, BASE); // 共 1000 行
        String yaml = ""
                + "name: keyed-agg\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      pageSize: 100\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [user_id]\n"
                + "      aggregates:\n"
                + "        - {fn: SUM, column: amount, alias: total_amount}\n"
                + "        - {fn: COUNT, column: \"*\", alias: cnt}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: agg}\n"
                + "  - {from: agg, to: out}\n";
        PipelineSpec spec = PipelineParser.parse(yaml);

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = executor().execute(spec, captured);

        assertEquals(1000L, result.getRecordsWritten(), "滚动口径：每条输入都发一次");

        Map<String, BigDecimal> expected = new LinkedHashMap<String, BigDecimal>();
        Connection connection = fixtures.source().getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet rs = statement.executeQuery(
                        "SELECT user_id, SUM(amount) FROM z_trade_order GROUP BY user_id");
                try {
                    while (rs.next()) {
                        expected.put(String.valueOf(rs.getLong(1)), rs.getBigDecimal(2));
                    }
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
        Map<String, BigDecimal> actual = new LinkedHashMap<String, BigDecimal>();
        for (Map<String, Object> row : collectRows(captured, "out")) {
            actual.put(String.valueOf(row.get("user_id")), (BigDecimal) row.get("total_amount"));
        }
        assertEquals(expected, actual, "每个键的滚动累计终值应与 SQL GROUP BY 一致");
    }

    @Test
    @DisplayName("窗口聚合：滚动 5 分钟窗口，水位到点即结算（不用等流结束）")
    void tumblingWindowFiresByWatermark() throws Exception {
        String yaml = ""
                + "name: window-agg\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      pageSize: 3\n"
                + "      eventTimeColumn: updated_at\n"
                + "      eventTimeType: TIMESTAMP\n"
                + "      maxOutOfOrderness: 0s\n"
                + "  - id: wagg\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [payment_method]\n"
                + "      window: {type: tumbling, size: 5m}\n"
                + "      aggregates:\n"
                + "        - {fn: COUNT, column: \"*\", alias: cnt}\n"
                + "        - {fn: SUM, column: amount, alias: total_amount}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: wagg}\n"
                + "  - {from: wagg, to: out}\n";
        PipelineSpec spec = PipelineParser.parse(yaml);

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = executor().execute(spec, captured);

        assertEquals(10L, result.getRecordsRead());
        List<Map<String, Object>> rows = collectRows(captured, "out");
        assertEquals(2, rows.size(), "10 行按 1 分钟间隔 ⇒ 恰好两个 5 分钟窗口");
        for (Map<String, Object> row : rows) {
            assertEquals(5L, ((Number) row.get("cnt")).longValue(), "每窗口 5 行: " + row);
            assertEquals("alipay", row.get("payment_method"));
            assertNotNull(row.get("windowStart"));
            assertNotNull(row.get("windowEnd"));
        }
    }

    @Test
    @DisplayName("去重：按键保留首条，重复键只发一次")
    void dedupKeepsFirstPerKey() throws Exception {
        DataSource source = EtlTestFixtures.h2("pipeline_dedup_src");
        Connection connection = source.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                statement.execute("CREATE TABLE t (id BIGINT, k VARCHAR(16))");
                statement.execute("INSERT INTO t VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c')");
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
        MapDataSourceProvider dedupProvider = new MapDataSourceProvider().register("ds-x", source);
        String yaml = ""
                + "name: dedup-pipe\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-x\n"
                + "      table: t\n"
                + "  - id: dd\n"
                + "    type: dedup\n"
                + "    config:\n"
                + "      keyBy: [k]\n"
                + "      keep: first\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: dd}\n"
                + "  - {from: dd, to: out}\n";
        PipelineSpec spec = PipelineParser.parse(yaml);

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        new PipelineExecutor(dedupProvider, new HeapStateBackend()).execute(spec, captured);

        Map<String, Object> byKey = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> rows = collectRows(captured, "out");
        assertEquals(3, rows.size(), "5 行里 3 个键 ⇒ 只发 3 条");
        for (Map<String, Object> row : rows) {
            byKey.put(String.valueOf(row.get("k")), row.get("id"));
        }
        assertEquals("1", String.valueOf(byKey.get("a")));
        assertEquals("3", String.valueOf(byKey.get("b")));
        assertEquals("5", String.valueOf(byKey.get("c")));
    }

    @Test
    @DisplayName("旁路输出：filter 未命中的行走 rejectedTo 死信汇，主链不受影响")
    void filterSideOutputRoutesToDeadLetterSink() throws Exception {
        String yaml = ""
                + "name: side-output\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "  - id: f1\n"
                + "    type: filter\n"
                + "    config:\n"
                + "      expression: \"status == 1\"\n"
                + "      rejectedTo: dead\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "  - id: dead\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: f1}\n"
                + "  - {from: f1, to: out}\n";
        PipelineSpec spec = PipelineParser.parse(yaml);

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        executor().execute(spec, captured);

        assertEquals(4L, collectRows(captured, "out").size(), "status == 1 的 4 行走主输出");
        assertEquals(6L, collectRows(captured, "dead").size(), "其余 6 行走死信旁路");
    }

    @Test
    @DisplayName("JDBC 落地 + 增量接力：首跑全量，新行到位后只搬增量")
    void jdbcSinkIncrementalResume() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        InMemoryPipelineStateStore states = new InMemoryPipelineStateStore();
        PipelineExecutor exec = new PipelineExecutor(provider, new HeapStateBackend(),
                new com.zifang.z.etl.core.transform.ScriptSandbox(), checkpoints, states);

        PipelineSpec spec = snapshotPipeline();
        PipelineRunResult first = exec.execute(spec);
        assertEquals(10L, first.getRecordsWritten(), "首跑全量 10 行");
        assertEquals(10L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));
        assertFalse(first.isRestored(), "首跑无快照");
        assertNotNull(first.getWatermarkAfter(), "首跑落水位");

        PipelineRunResult second = exec.execute(spec);
        assertEquals(0L, second.getRecordsWritten(), "无新行 ⇒ 零搬运");
        assertTrue(second.isRestored(), "第二轮从快照恢复");

        try (Connection connection = fixtures.source().getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO z_trade_order (id, order_no, user_id, amount, status, updated_at)"
                             + " VALUES (?, ?, ?, ?, ?, ?)")) {
            for (int i = 0; i < 2; i++) {
                statement.setLong(1, 11L + i);
                statement.setString(2, "SO" + (11L + i));
                statement.setLong(3, 42L);
                statement.setBigDecimal(4, new BigDecimal("999.00"));
                statement.setInt(5, 1);
                statement.setTimestamp(6, new Timestamp(BASE.getTime() + (10L + i) * 60000L));
                statement.addBatch();
            }
            statement.executeBatch();
        }
        PipelineRunResult third = exec.execute(spec);
        assertEquals(2L, third.getRecordsWritten(), "只搬 2 条增量");
        assertEquals(12L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));
    }

    @Test
    @DisplayName("幂等重跑：mode=init 全量重跑，目标表不重行")
    void jdbcSinkIdempotentRerun() throws Exception {
        PipelineSpec spec = snapshotPipeline();
        spec.setMode("init");
        PipelineExecutor exec = executor();
        exec.execute(spec);
        exec.execute(spec);
        assertEquals(10L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "upsert 幂等 ⇒ 物理行数不变");
    }

    @Test
    @DisplayName("旧作业描述编译成流水线，产出与旧引擎逐字段一致")
    void legacyJobCompiledToPipelineProducesIdenticalRows() throws Exception {
        // 旧引擎侧
        EtlTestFixtures legacyFixtures = new EtlTestFixtures("legacy");
        legacyFixtures.createSchema();
        legacyFixtures.seedSource(50, 1L, BASE);
        EtlSchema.ensure(legacyFixtures.meta());
        EtlEngine legacyEngine = new EtlEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, legacyFixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, legacyFixtures.target()),
                new JdbcCheckpointStore(legacyFixtures.meta()));
        JobDescriptor descriptor = EtlTestFixtures.loadYamlDescriptor();
        EtlRunResult legacyResult = legacyEngine.run(descriptor);
        assertEquals(50L, legacyResult.getRowsWritten());

        // 流水线侧（同一份描述原文编译）
        EtlTestFixtures pipelineFixtures = new EtlTestFixtures("pipeline-legacy");
        pipelineFixtures.createSchema();
        pipelineFixtures.seedSource(50, 1L, BASE);
        MapDataSourceProvider pipelineProvider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, pipelineFixtures.source())
                .register(EtlTestFixtures.TARGET_REF, pipelineFixtures.target());
        PipelineSpec spec = PipelineSpec.fromJobDescriptor(descriptor);
        PipelineExecutor exec = new PipelineExecutor(pipelineProvider, new HeapStateBackend(),
                new com.zifang.z.etl.core.transform.ScriptSandbox(),
                new InMemoryCheckpointStore(), new InMemoryPipelineStateStore());
        PipelineRunResult pipelineResult = exec.execute(spec);
        assertEquals(50L, pipelineResult.getRecordsWritten());

        String legacy = EtlTestFixtures.dumpTable(legacyFixtures.target(), "etl_trade_order_snapshot", "order_id");
        String pipeline = EtlTestFixtures.dumpTable(pipelineFixtures.target(), "etl_trade_order_snapshot", "order_id");
        assertEquals(legacy, pipeline, "两条执行路径的落表内容应逐字段一致");
    }

    /** 从捕获的算子里取 collect 汇的行。 */
    private static List<Map<String, Object>> collectRows(Map<String, Operator> captured, String sinkId) {
        Operator operator = captured.get(sinkId);
        assertTrue(operator instanceof SinkOperators.CollectSink, "节点 " + sinkId + " 应是 collect 汇");
        return ((SinkOperators.CollectSink) operator).rows();
    }

    /** 快照搬运流水线（JDBC 汇 + 断点续传口径）。 */
    private PipelineSpec snapshotPipeline() {
        String yaml = ""
                + "name: snapshot-pipe\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, order_no, user_id, amount, status, updated_at]\n"
                + "      pageSize: 3\n"
                + "      eventTimeColumn: updated_at\n"
                + "      eventTimeType: TIMESTAMP\n"
                + "      resume:\n"
                + "        column: updated_at\n"
                + "        type: TIMESTAMP\n"
                + "        tiebreakColumn: id\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "        - {source: order_no, target: order_no, type: STRING}\n"
                + "        - {source: user_id, target: user_id, type: LONG}\n"
                + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                + "        - {source: status, target: status, type: INT}\n"
                + "        - {source: updated_at, target: updated_at, type: TIMESTAMP}\n"
                + "  - id: sink\n"
                + "    type: jdbc.sink\n"
                + "    config:\n"
                + "      dataSourceRef: ds-etl-landing\n"
                + "      table: etl_trade_order_snapshot\n"
                + "      batchSize: 4\n"
                + "      writeMode: upsert\n"
                + "      idempotencyKey: [order_id]\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: sink}\n";
        return PipelineParser.parse(yaml);
    }
}
