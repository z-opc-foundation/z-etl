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

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 试跑验收（{@code dryRun}）：定义完一条流水线，先跑 N 行看看链路通不通。
 *
 * <p>口径：汇只计数不落地（不把试验数据写进目标表）；不读不写水位（试跑不能当增量起点）；
 * 不读不写状态快照（试跑不算正式运行，也不落履历）。
 *
 * @author zifang
 */
class PipelineDryRunTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L); // 2026-01-01 00:00:00

    private EtlTestFixtures fixtures;
    private PipelineEngine engine;
    private JdbcPipelineStateStore stateStore;
    private InMemoryCheckpointStore checkpointStore;

    private static final String SNAPSHOT_YAML = ""
            + "name: dry-run-demo\n"
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
        fixtures = new EtlTestFixtures("dry-run");
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
    @DisplayName("试跑 3 行：读到 3 行、会写 3 行，但目标表一行没有、水位没落")
    void stopsAfterSampleRowsWithoutWriting() throws Exception {
        engine.register(SNAPSHOT_YAML);
        PipelineRunResult result = engine.dryRun("dry-run-demo", null, 3L);
        assertEquals(3L, result.getRecordsRead(), "最多处理 3 行");
        assertEquals(3L, result.getRecordsWritten(), "recordsWritten 是「会写多少行」");
        assertEquals(0L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "试跑不落地 ⇒ 目标表一行没有");
        assertNull(checkpointStore.load("dry-run-demo"), "试跑不落水位");
        assertFalse(stateStore.contains("dry-run-demo"), "试跑不落状态快照");
        assertTrue(result.getNotes().toString().contains("dry-run"), "备注应说明试跑口径: " + result.getNotes());
    }

    @Test
    @DisplayName("行数不足 sampleRows ⇒ 跑完为止")
    void fewerRowsThanSampleRunsToCompletion() throws Exception {
        engine.register(SNAPSHOT_YAML);
        PipelineRunResult result = engine.dryRun("dry-run-demo", null, 100L);
        assertEquals(10L, result.getRecordsRead(), "只有 10 行源数据");
        assertEquals(10L, result.getRecordsWritten());
        assertEquals(0L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "跑完也不落地");
    }

    @Test
    @DisplayName("先正式跑再试跑：水位与状态快照不被试跑动过")
    void dryRunDoesNotTouchRealState() throws Exception {
        engine.register(SNAPSHOT_YAML);
        // 正式跑：水位 + 状态快照都落库
        engine.run("dry-run-demo");
        assertNotNull(checkpointStore.load("dry-run-demo"), "正式跑落水位");
        assertTrue(stateStore.contains("dry-run-demo"), "正式跑落状态快照");
        String watermarkBefore = checkpointStore.load("dry-run-demo").getWatermarkValue();

        // 试跑：不读水位 ⇒ 从全量跑（这才是「第一次跑会是什么样」的可预测口径）；行数由 sampleRows 兜底
        insertRows(11L, 2);
        PipelineRunResult trial = engine.dryRun("dry-run-demo", null, 100L);
        assertEquals(12L, trial.getRecordsRead(), "试跑不读水位 ⇒ 全量试跑");
        assertEquals(watermarkBefore, checkpointStore.load("dry-run-demo").getWatermarkValue(),
                "试跑后水位纹丝不动");
        assertEquals(10L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "试跑没把增量写进目标表（还是正式跑留下的 10 行）");
    }

    private void insertRows(long startId, int rows) throws Exception {
        java.sql.Connection connection = fixtures.source().getConnection();
        try {
            java.sql.PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO z_trade_order (id, order_no, user_id, amount, status, updated_at)"
                            + " VALUES (?, ?, ?, ?, ?, ?)");
            try {
                for (int i = 0; i < rows; i++) {
                    long id = startId + i;
                    statement.setLong(1, id);
                    statement.setString(2, "SO" + id);
                    statement.setLong(3, 42L);
                    statement.setBigDecimal(4, new java.math.BigDecimal("888.00"));
                    statement.setInt(5, 1);
                    statement.setTimestamp(6, new Timestamp(BASE.getTime() + (20L + i) * 60000L));
                    statement.addBatch();
                }
                statement.executeBatch();
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }
}
