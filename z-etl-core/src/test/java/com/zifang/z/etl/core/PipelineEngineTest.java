package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcPipelineRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.runtime.JdbcPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流水线管控引擎验收：登记 → 触发 → 状态快照落库（跨重启恢复）→ 增量接力 → 履历。
 *
 * <p>⚠️ 硬边界：引擎元库 / 源库 / 目标库全是本地 H2 内存库，不连生产 RDS。
 *
 * @author zifang
 */
class PipelineEngineTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L); // 2026-01-01 00:00:00

    private EtlTestFixtures fixtures;
    private PipelineEngine engine;
    private JdbcPipelineStateStore stateStore;
    private InMemoryCheckpointStore checkpointStore;

    private static final String SNAPSHOT_YAML = ""
            + "name: engine-snapshot\n"
            + "description: 管控面验收：快照搬运\n"
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
            + "      resume: {column: updated_at, type: TIMESTAMP, tiebreakColumn: id}\n"
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
            + "      writeMode: upsert\n"
            + "      idempotencyKey: [order_id]\n"
            + "edges:\n"
            + "  - {from: src, to: fm}\n"
            + "  - {from: fm, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("pipeline-engine");
        fixtures.createSchema();
        fixtures.seedSource(10, 1L, BASE);
        EtlSchema.ensure(fixtures.meta());
        stateStore = new JdbcPipelineStateStore(fixtures.meta());
        checkpointStore = new InMemoryCheckpointStore();
        engine = new PipelineEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, fixtures.target()),
                new JdbcPipelineRepository(fixtures.meta()),
                checkpointStore,
                stateStore);
    }

    @Test
    @DisplayName("登记 → 触发 → 快照落库 → 跨执行器恢复 → 增量接力 → 履历落库")
    void registerRunResumeAndHistory() throws Exception {
        // 登记落库
        assertEquals("engine-snapshot", engine.register(SNAPSHOT_YAML));
        assertEquals(1, engine.listNames().size());
        assertNotNull(engine.get("engine-snapshot"), "定义已落库");
        assertTrue(engine.explain("engine-snapshot").contains("engine-snapshot"), "计划解释可读");

        // 首跑全量
        PipelineRunResult first = engine.run("engine-snapshot");
        assertEquals(10L, first.getRecordsWritten());
        assertEquals(10L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));
        assertNotNull(first.getWatermarkAfter());
        assertTrue(stateStore.contains("engine-snapshot"), "状态快照已落库");

        // 模拟跨重启：同一个水位库 + 状态库，全新引擎（新执行器从快照 + 水位恢复）
        PipelineEngine restarted = new PipelineEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, fixtures.target()),
                new JdbcPipelineRepository(fixtures.meta()),
                checkpointStore,
                stateStore);
        PipelineRunResult second = restarted.run("engine-snapshot");
        assertTrue(second.isRestored(), "重启后从快照恢复");
        assertEquals(0L, second.getRecordsWritten(), "无新行 ⇒ 零搬运");

        // 来 2 条增量 ⇒ 只搬增量
        insertRows(11L, 2);
        PipelineRunResult third = restarted.run("engine-snapshot");
        assertEquals(2L, third.getRecordsWritten(), "只搬 2 条增量");
        assertEquals(12L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));

        // 履历落库（新→旧）
        assertEquals(3, engine.history("engine-snapshot", 0).size());
        assertEquals("engine-snapshot", engine.history("engine-snapshot", 1).get(0).getPipelineName());
        assertEquals(2L, engine.history("engine-snapshot", 1).get(0).getRecordsWritten());

        // 水位可见
        assertNotNull(engine.watermark("engine-snapshot"), "水位记录可见");
    }

    @Test
    @DisplayName("未登记就触发 ⇒ 明确报错；不合法声明登记被拒")
    void registerRejectsInvalidAndRunFailsOnUnknown() {
        Exception e = null;
        try {
            engine.run("no-such-pipeline");
        } catch (Exception ex) {
            e = ex;
        }
        assertNotNull(e, "未登记触发应报错");

        Exception parseError = null;
        try {
            engine.register("name: bad\nnodes:\n  - {id: x, type: no.such.thing}\n");
        } catch (Exception ex) {
            parseError = ex;
        }
        assertNotNull(parseError, "不合法声明应被拒");
    }

    private void insertRows(long startId, int rows) throws Exception {
        try (Connection connection = fixtures.source().getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO z_trade_order (id, order_no, user_id, amount, status, updated_at)"
                             + " VALUES (?, ?, ?, ?, ?, ?)")) {
            for (int i = 0; i < rows; i++) {
                long id = startId + i;
                statement.setLong(1, id);
                statement.setString(2, "SO" + id);
                statement.setLong(3, 42L);
                statement.setBigDecimal(4, new BigDecimal("888.00"));
                statement.setInt(5, 1);
                statement.setTimestamp(6, new Timestamp(BASE.getTime() + (20L + i) * 60000L));
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }
}
