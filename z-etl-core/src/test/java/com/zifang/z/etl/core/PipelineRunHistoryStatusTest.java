package com.zifang.z.etl.core;

import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcPipelineRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行履历状态口径验收（运行总览的地基）：成功落 SUCCESS、失败也落 FAILED（带异常摘要）、
 * 跨流水线最近运行、老库补 status 列。
 *
 * <p>⚠️ 硬边界：元库 / 源库 / 目标库全是本地 H2 内存库，不连生产 RDS。
 *
 * @author zifang
 */
class PipelineRunHistoryStatusTest {

    private EtlTestFixtures fixtures;
    private PipelineEngine engine;
    private JdbcPipelineRepository repository;

    private static final String OK_YAML = ""
            + "name: status-ok\n"
            + "nodes:\n"
            + "  - id: src\n"
            + "    type: jdbc.source\n"
            + "    config:\n"
            + "      dataSourceRef: ds-oc-main\n"
            + "      table: z_trade_order\n"
            + "      columns: [id, order_no, user_id, user_name, product_code, amount, quantity, status, payment_method, created_at, updated_at]\n"
            + "      pageSize: 5\n"
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

    private static final String BROKEN_YAML = ""
            + "name: status-broken\n"
            + "nodes:\n"
            + "  - id: src\n"
            + "    type: jdbc.source\n"
            + "    config:\n"
            + "      dataSourceRef: ds-missing\n"
            + "      table: z_trade_order\n"
            + "  - id: sink\n"
            + "    type: jdbc.sink\n"
            + "    config:\n"
            + "      dataSourceRef: ds-etl-landing\n"
            + "      table: etl_landing\n"
            + "      writeMode: append\n"
            + "edges:\n"
            + "  - {from: src, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("pipeline-run-status");
        fixtures.createSchema();
        fixtures.seedSource(3, 1L, new java.sql.Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());
        repository = new JdbcPipelineRepository(fixtures.meta());
        engine = new PipelineEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, fixtures.target()),
                repository,
                null, null);
        engine.register(OK_YAML);
        engine.register(BROKEN_YAML);
    }

    @Test
    @DisplayName("成功运行落 SUCCESS 履历；失败运行也落 FAILED 履历（异常摘要进 notes）")
    void successAndFailureBothRecorded() throws Exception {
        engine.run("status-ok");
        try {
            engine.run("status-broken");
            throw new AssertionError("坏引用流水线应当失败");
        } catch (IllegalArgumentException expected) {
            assertTrue(String.valueOf(expected.getMessage()).contains("ds-missing"));
        }

        PipelineRunResult ok = repository.runHistory("status-ok", 5).get(0);
        assertEquals(PipelineRunResult.STATUS_SUCCESS, ok.getStatus(), "成功运行标 SUCCESS");

        PipelineRunResult broken = repository.runHistory("status-broken", 5).get(0);
        assertEquals(PipelineRunResult.STATUS_FAILED, broken.getStatus(), "失败运行标 FAILED");
        assertNotNull(broken.getNotes());
        assertTrue(!broken.getNotes().isEmpty(), "异常摘要进 notes");
        assertTrue(String.join("；", broken.getNotes()).contains("ds-missing"), "摘要里能看到坏引用");
    }

    @Test
    @DisplayName("recentRuns 跨流水线按时间倒序返回")
    void recentRunsAcrossPipelines() throws Exception {
        engine.run("status-ok");
        try {
            engine.run("status-broken");
        } catch (Exception expected) {
            // 失败即目标
        }
        java.util.List<PipelineRunResult> runs = repository.recentRuns(10);
        assertEquals(2, runs.size(), "两条履历都在（成功 + 失败）");
        assertTrue(runs.get(0).getStartedAt() >= runs.get(1).getStartedAt(), "时间倒序");
    }

    @Test
    @DisplayName("老库升级：无 status 列的旧表在 EtlSchema.ensure 后补列可写")
    void legacyTableGetsStatusColumn() throws Exception {
        // 造一个「老库」：履历表是旧口径（没有 status 列），ensure 在建表之后补列
        javax.sql.DataSource legacyMeta = EtlTestFixtures.h2("legacy-meta-" + System.nanoTime());
        try (Connection connection = legacyMeta.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE etl_pipeline_run ("
                    + "run_id VARCHAR(64) PRIMARY KEY, pipeline_name VARCHAR(128), mode VARCHAR(32),"
                    + " records_read BIGINT, records_written BIGINT, watermark_before VARCHAR(256),"
                    + " watermark_after VARCHAR(256), restored INT, notes TEXT,"
                    + " started_at BIGINT, finished_at BIGINT)");
        }
        EtlSchema.ensure(legacyMeta);
        JdbcPipelineRepository legacyRepository = new JdbcPipelineRepository(legacyMeta);
        PipelineRunResult result = new PipelineRunResult();
        result.setPipelineName("legacy-pipe");
        result.setStartedAt(System.currentTimeMillis());
        result.setFinishedAt(System.currentTimeMillis() + 5);
        legacyRepository.saveRun(result);
        assertEquals(PipelineRunResult.STATUS_SUCCESS, legacyRepository.runHistory("legacy-pipe", 1).get(0).getStatus(),
                "ensure 补列后 saveRun 可写，旧口径缺省当 SUCCESS");
    }

    @Test
    @DisplayName("老库补列后 ensure 可重复执行（幂等）")
    void ensureIsIdempotent() throws SQLException {
        EtlSchema.ensure(fixtures.meta());
        EtlSchema.ensure(fixtures.meta());
        assertTrue(true, "重复 ensure 不抛");
    }
}
