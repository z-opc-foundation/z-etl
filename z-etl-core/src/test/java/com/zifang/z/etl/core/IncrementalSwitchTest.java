package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 3：首跑后自动切增量模式（水位记录可见）。
 *
 * @author zifang
 */
class IncrementalSwitchTest {

    private EtlTestFixtures fixtures;
    private JdbcCheckpointStore checkpointStore;
    private EtlEngine engine;
    private JobDescriptor descriptor;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("inc");
        fixtures.createSchema();
        fixtures.seedSource(1000, 1L, new Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());
        checkpointStore = new JdbcCheckpointStore(fixtures.meta());
        engine = new EtlEngine(new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target()), checkpointStore);
        descriptor = EtlTestFixtures.loadYamlDescriptor();
    }

    @Test
    @DisplayName("首跑全量后落水位，下一次自动切增量并只搬新增行")
    void firstRunFullThenAutoSwitchToIncremental() throws Exception {
        // —— 首跑：全量
        EtlRunResult first = engine.run(descriptor);
        assertEquals("init", first.getMode(), "首跑应为全量");
        assertEquals(1000L, first.getRowsRead());
        assertTrue(first.isReconciled());

        // —— 水位记录可见（落库）
        Checkpoint checkpoint = checkpointStore.load(descriptor.getName());
        assertNotNull(checkpoint, "首跑后应落水位记录");
        assertNotNull(checkpoint.getWatermarkValue(), "水位值不应为空");
        assertEquals("init", checkpoint.getLastMode(), "水位记录应标出上次是初始化模式");
        assertEquals("updated_at", checkpoint.getWatermarkColumn());
        assertEquals("TIMESTAMP", checkpoint.getWatermarkType());

        // —— 源表新增 5 行（时间戳晚于水位）
        fixtures.seedSource(5, 2001L, new Timestamp(1767225600000L + 2000L * 60000L));

        // —— 第二次：自动切增量，只搬新增的 5 行
        EtlRunResult second = engine.run(descriptor);
        assertEquals("incremental", second.getMode(), "首跑后应自动切增量");
        assertEquals(5L, second.getRowsRead(), "增量只应读新增行");
        assertEquals(5L, second.getRowsWritten());
        assertEquals(1005L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"),
                "目标表应为 1000 + 5 行");
        assertTrue(second.isReconciled(), "增量跑完对账应一致");

        // —— 水位推进且可见
        Checkpoint after = checkpointStore.load(descriptor.getName());
        assertNotNull(after);
        assertEquals("incremental", after.getLastMode());
        assertTrue(after.getWatermarkValue().compareTo(checkpoint.getWatermarkValue()) > 0,
                "水位应向前推进: " + checkpoint.getWatermarkValue() + " → " + after.getWatermarkValue());
        assertEquals(after.getWatermarkValue(), second.getWatermarkAfter());
    }

    @Test
    @DisplayName("声明 incremental 但无水位时直接失败，不静默退化成全量")
    void incrementalWithoutWatermarkFails() {
        descriptor.setMode("incremental");
        try {
            engine.run(descriptor);
            org.junit.jupiter.api.Assertions.fail("无水位却声明 incremental 时应失败");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("水位"), "错误信息应指明水位缺失");
        }
    }
}
