package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobMode;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 2：job 描述 JSON/yml 可落库、可重跑；重跑幂等（不重不漏）。
 *
 * @author zifang
 */
class IdempotentRerunTest {

    private EtlTestFixtures fixtures;
    private EtlEngine engine;
    private JobDescriptor descriptor;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("idem");
        fixtures.createSchema();
        fixtures.seedSource(1000, 1L, new Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());
        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
        engine = new EtlEngine(provider, new JdbcCheckpointStore(fixtures.meta()));
        descriptor = EtlTestFixtures.loadYamlDescriptor();
    }

    @Test
    @DisplayName("落库后可按作业名重跑")
    void jobCanBeRerunFromRepository() throws Exception {
        com.zifang.z.etl.core.engine.JdbcJobRepository repository =
                new com.zifang.z.etl.core.engine.JdbcJobRepository(fixtures.meta());
        repository.save(descriptor);

        // 落库后取回的定义与原定义一致
        JobDescriptor loaded = repository.find(descriptor.getName());
        assertEquals(descriptor.getName(), loaded.getName());
        assertEquals("z_trade_order", loaded.getSource().getTable());
        assertEquals("etl_trade_order_snapshot", loaded.getTarget().getTable());
        assertEquals(1, loaded.getIdempotencyKey().size());
        assertEquals("order_id", loaded.getIdempotencyKey().get(0));
        assertTrue(repository.listNames().contains(descriptor.getName()));

        // 按作业名跑（不依赖原始描述文件）
        EtlEngine engineWithRepo = new EtlEngine(
                new MapDataSourceProvider()
                        .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                        .register(EtlTestFixtures.TARGET_REF, fixtures.target()),
                repository, new JdbcCheckpointStore(fixtures.meta()));
        EtlRunResult result = engineWithRepo.run(descriptor.getName());
        assertEquals(1000L, result.getRowsWritten());
        assertTrue(repository.runHistory(descriptor.getName(), 10).size() >= 1);
    }

    @Test
    @DisplayName("增量重跑：不重不漏（目标行数不变）")
    void incrementalRerunIsIdempotent() throws Exception {
        engine.run(descriptor);

        // 第二次跑：auto 模式 + 已有水位 ⇒ 自动切增量，源表无新数据 ⇒ 一行都不该再搬
        EtlRunResult second = engine.run(descriptor);
        assertEquals("incremental", second.getMode(), "已有水位后应自动切增量");
        assertEquals(0L, second.getRowsRead(), "无新数据时不应重复搬运");
        assertEquals(1000L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "重跑后目标行数不变（不重）");
        assertTrue(second.isReconciled());
    }

    @Test
    @DisplayName("强制全量重跑：upsert 幂等，不产生重复行")
    void forcedFullRerunDoesNotDuplicate() throws Exception {
        engine.run(descriptor);
        assertEquals(1000L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));

        // 强制全量重跑（mode=init）：整表重读 + 按幂等键 upsert
        descriptor.setMode(JobMode.INIT.id());
        EtlRunResult rerun = engine.run(descriptor);
        assertEquals("init", rerun.getMode());
        assertEquals(1000L, rerun.getRowsRead(), "全量重跑应重新读全表");
        assertEquals(1000L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "全量重跑后目标行数仍应等于源行数（不重）");
        assertEquals(1000L, distinctOrderIds(fixtures), "幂等键不得出现重复（不重）");
        assertTrue(rerun.isReconciled(), "重跑后对账仍应一致");
    }

    /** 目标表里不重复的幂等键数量。 */
    private long distinctOrderIds(EtlTestFixtures f) throws Exception {
        java.sql.Connection connection = f.target().getConnection();
        try {
            java.sql.Statement statement = connection.createStatement();
            try {
                java.sql.ResultSet rs = statement.executeQuery(
                        "SELECT COUNT(DISTINCT order_id) FROM etl_trade_order_snapshot");
                try {
                    return rs.next() ? rs.getLong(1) : 0L;
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }
}
