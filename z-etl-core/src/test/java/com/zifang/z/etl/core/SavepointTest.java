package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.checkpoint.Savepoint;
import com.zifang.z.etl.core.checkpoint.Savepoints;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcPipelineRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 保存点验收（savepoint：状态快照 + 水位游标打包成可搬运报文）。
 *
 * <p>口径照抄 Flink：导出纯读不动现场；灌回覆盖写；可换名迁移（作业复制）。
 * 迁移语义用「两个环境」实证：A 环境跑到一半 → 导出 → B 环境（全新库与存储）灌回 → 直接增量续跑。
 *
 * <p>⚠️ 硬边界：源库 / 目标库 / 引擎元库全是本地 H2 内存库，不连生产 RDS。
 *
 * @author zifang
 */
class SavepointTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L); // 2026-01-01 00:00:00

    private EtlTestFixtures fixturesA;
    private InMemoryCheckpointStore checkpointsA;
    private InMemoryPipelineStateStore statesA;
    private PipelineEngine engineA;

    private static final String JOB_YAML = ""
            + "name: mig-job\n"
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
            + "  - {from: src, to: fm}\n"+ "  - {from: fm, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        fixturesA = new EtlTestFixtures("savepoint-a");
        fixturesA.createSchema();
        fixturesA.seedSource(10, 1L, BASE);
        EtlSchema.ensure(fixturesA.meta());
        checkpointsA = new InMemoryCheckpointStore();
        statesA = new InMemoryPipelineStateStore();
        engineA = engine(fixturesA, checkpointsA, statesA);
        engineA.register(JOB_YAML);
    }

    @Test
    @DisplayName("导出：状态快照 + 复合水位都进报文，且导出不动现场")
    void exportCarriesStateAndWatermarkWithoutMutating() throws Exception {
        engineA.run("mig-job");

        Savepoint savepoint = engineA.exportSavepoint("mig-job");
        assertEquals("mig-job", savepoint.getPipeline());
        assertTrue(savepoint.hasState(), "应带状态快照");
        assertTrue(Savepoints.stateBytes(savepoint) > 0);
        assertTrue(savepoint.hasWatermark(), "应带水位游标");
        assertEquals("updated_at", savepoint.getWatermarkColumn());
        assertEquals("TIMESTAMP", savepoint.getWatermarkType());
        assertNotNull(savepoint.getWatermarkValue());
        assertEquals("id", savepoint.getTiebreakColumn(), "复合游标的平局列应进报文");
        assertEquals("auto", savepoint.getLastMode(), "DAG 层缺省模式是 auto");

        // 导出纯读：现场纹丝不动，两次导出的状态字节一致
        String first = savepoint.getStateBase64();
        String second = engineA.exportSavepoint("mig-job").getStateBase64();
        assertEquals(first, second, "导出不应改变状态快照");
    }

    @Test
    @DisplayName("迁移：A 环境导出 → B 环境灌回 → 直接增量续跑（新行只搬增量）")
    void importMigratesJobToFreshEnvironment() throws Exception {
        engineA.run("mig-job");
        String json = Savepoints.toJson(engineA.exportSavepoint("mig-job"));

        // B 环境：全新的库、全新的存储、同名定义
        EtlTestFixtures fixturesB = new EtlTestFixtures("savepoint-b");
        fixturesB.createSchema();
        fixturesB.seedSource(12, 1L, BASE); // 12 行 = A 的 10 行 + 2 条新行
        EtlSchema.ensure(fixturesB.meta());
        PipelineEngine engineB = engine(fixturesB, new InMemoryCheckpointStore(),
                new InMemoryPipelineStateStore());
        engineB.register(JOB_YAML);

        engineB.importSavepoint(json);
        PipelineRunResult run = engineB.run("mig-job");
        assertEquals(2L, run.getRecordsWritten(), "水位与状态已随保存点迁移 ⇒ B 环境只搬 2 条增量");
        assertTrue(run.isRestored(), "灌回的状态快照应被识别为恢复");
        assertEquals(2L, EtlTestFixtures.count(fixturesB.target(), "etl_trade_order_snapshot"),
                "B 环境目标表从空开始，只落水位之后的 2 条增量");
    }

    @Test
    @DisplayName("换名迁移：同一份报文灌到新名字下照样续跑")
    void importToRenamedPipeline() throws Exception {
        engineA.run("mig-job");
        String json = Savepoints.toJson(engineA.exportSavepoint("mig-job"));

        EtlTestFixtures fixturesB = new EtlTestFixtures("savepoint-rename");
        fixturesB.createSchema();
        fixturesB.seedSource(12, 1L, BASE);
        EtlSchema.ensure(fixturesB.meta());
        PipelineEngine engineB = engine(fixturesB, new InMemoryCheckpointStore(),
                new InMemoryPipelineStateStore());
        engineB.register(JOB_YAML.replace("name: mig-job", "name: mig-copy"));

        engineB.importSavepoint(json, "mig-copy");
        PipelineRunResult run = engineB.run("mig-copy");
        assertEquals(2L, run.getRecordsWritten(), "换名后水位照常生效");
        assertEquals("mig-copy", engineB.watermark("mig-copy").getJobName());
    }

    @Test
    @DisplayName("报文校验：坏版本 / 缺 pipeline / 空报文都拒绝")
    void validationRejects() throws Exception {
        engineA.run("mig-job");
        Savepoint savepoint = engineA.exportSavepoint("mig-job");

        savepoint.setVersion(99);
        assertThrows(IllegalArgumentException.class, () -> Savepoints.fromJson(Savepoints.toJson(savepoint)),
                "坏版本应拒绝");
        savepoint.setVersion(1);
        savepoint.setPipeline(" ");
        assertThrows(IllegalArgumentException.class, () -> Savepoints.fromJson(Savepoints.toJson(savepoint)),
                "缺 pipeline 应拒绝");
        savepoint.setPipeline("mig-job");
        savepoint.setStateBase64(null);
        savepoint.setWatermarkValue(null);
        assertThrows(IllegalArgumentException.class, () -> Savepoints.fromJson(Savepoints.toJson(savepoint)),
                "空报文应拒绝");
    }

    @Test
    @DisplayName("没跑过的空管道导不出")
    void emptyPipelineCannotExport() throws Exception {
        engineA.register(JOB_YAML.replace("name: mig-job", "name: never-ran"));
        assertThrows(IllegalArgumentException.class, () -> engineA.exportSavepoint("never-ran"),
                "状态与水位都没有 ⇒ 没东西可搬");
    }

    private PipelineEngine engine(EtlTestFixtures fixtures, InMemoryCheckpointStore checkpoints,
                                  InMemoryPipelineStateStore states) {
        return new PipelineEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, fixtures.target()),
                new JdbcPipelineRepository(fixtures.meta()),
                checkpoints,
                states);
    }
}
