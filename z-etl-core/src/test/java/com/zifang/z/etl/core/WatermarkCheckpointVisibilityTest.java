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

import javax.sql.DataSource;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 3：游标持久化可查（当前值 / 上次推进时间）。
 *
 * <p>口径：水位落在引擎元库表 {@code etl_job_checkpoint} 里，
 * <b>两条路径</b>都能查：
 * <ul>
 *   <li>存储口 {@link JdbcCheckpointStore#load(String)}（引擎内部与 web 层 REST 走的就是它）；</li>
 *   <li>直查表（运维/BI 侧的口径：{@code SELECT watermark_value, last_run_at ...}）。</li>
 * </ul>
 *
 * <p>全部在本地 H2 上跑，不连任何真实库。
 *
 * @author zifang
 */
class WatermarkCheckpointVisibilityTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L);

    private DataSource source;
    private DataSource target;
    private DataSource meta;
    private JdbcCheckpointStore checkpointStore;
    private EtlEngine engine;
    private JobDescriptor descriptor;

    @BeforeEach
    void setUp() throws Exception {
        source = WatermarkTestFixtures.h2Mem("wm_v_src");
        target = WatermarkTestFixtures.h2Mem("wm_v_dst");
        meta = WatermarkTestFixtures.h2Mem("wm_v_meta");
        WatermarkTestFixtures.createSchema(source, target);
        EtlSchema.ensure(meta);
        checkpointStore = new JdbcCheckpointStore(meta);
        engine = new EtlEngine(new MapDataSourceProvider()
                .register(WatermarkTestFixtures.SOURCE_REF, source)
                .register(WatermarkTestFixtures.TARGET_REF, target), checkpointStore);
        descriptor = WatermarkTestFixtures.job("wm-visible");
    }

    @Test
    @DisplayName("水位当前值与上次推进时间都能查（存储口 + 直查表）")
    void checkpointIsQueryable() throws Exception {
        WatermarkTestFixtures.insertOrders(source, 1L, 100, BASE, 60000L);

        EtlRunResult first = engine.run(descriptor);
        assertEquals(100L, first.getRowsRead());

        // —— 存储口 ——
        Checkpoint checkpoint = checkpointStore.load(descriptor.getName());
        assertNotNull(checkpoint, "跑完应能查到水位记录");
        assertEquals(descriptor.getName(), checkpoint.getJobName());
        assertEquals("updated_at", checkpoint.getWatermarkColumn());
        assertEquals("TIMESTAMP", checkpoint.getWatermarkType());
        assertNotNull(checkpoint.getWatermarkValue(), "水位当前值应可查");
        assertEquals("id", checkpoint.getTiebreakColumn(), "复合游标的平局列应可查");
        assertNotNull(checkpoint.getTiebreakValue(), "平局列当前值应可查");
        assertEquals("init", checkpoint.getLastMode());
        assertTrue(checkpoint.getLastRunAt() > 0L, "上次推进时间应可查");
        assertTrue(checkpoint.getLastRunAt() <= System.currentTimeMillis() + 1000L);

        // —— 直查表 ——
        String valueInTable = WatermarkTestFixtures.checkpointValue(meta, descriptor.getName());
        assertEquals(checkpoint.getWatermarkValue(), valueInTable, "表里的水位值应与存储口一致");
        Long lastRunAtInTable = WatermarkTestFixtures.checkpointLastRunAt(meta, descriptor.getName());
        assertNotNull(lastRunAtInTable, "表里的上次推进时间应可查");
        assertEquals(Long.valueOf(checkpoint.getLastRunAt()), lastRunAtInTable);

        // —— 再跑一次：水位推进 + 上次推进时间刷新 ——
        long lastRunAtBefore = checkpoint.getLastRunAt();
        Thread.sleep(5L);
        WatermarkTestFixtures.insertOrders(source, 101L, 10, new Timestamp(BASE.getTime() + 100 * 60000L), 60000L);
        EtlRunResult second = engine.run(descriptor);
        assertEquals("incremental", second.getMode());

        Checkpoint after = checkpointStore.load(descriptor.getName());
        assertNotNull(after);
        assertTrue(after.getWatermarkValue().compareTo(checkpoint.getWatermarkValue()) > 0,
                "水位当前值应向前推进");
        assertTrue(after.getLastRunAt() > lastRunAtBefore, "上次推进时间应刷新");
        assertEquals("incremental", after.getLastMode(), "上次模式应可查");
        assertEquals(after.getWatermarkValue(), second.getWatermarkAfter(), "结果里的水位与表里应一致");
    }
}
