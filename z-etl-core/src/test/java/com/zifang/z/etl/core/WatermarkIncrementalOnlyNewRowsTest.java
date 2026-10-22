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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 1：源表持续写入场景下，增量轮只搬新行（行数对账零差异）。
 *
 * <p>口径：
 * <ul>
 *   <li>首跑全量 ⇒ 500 行搬完，源/目标行数一致、校验和一致；</li>
 *   <li>随后**并发**写入 300 行（搬运过程中还在写）；</li>
 *   <li>增量轮只读「水位之后」的行 ⇒ 老行不会被重搬（增量轮读数 == 增量轮写数，且两轮增量读数之和 == 新增行数）；</li>
 *   <li>最后对账：源/目标行数相等、校验和相等（零差异）。</li>
 * </ul>
 *
 * <p>全部在本地 H2 上跑，不连任何真实库。
 *
 * @author zifang
 */
class WatermarkIncrementalOnlyNewRowsTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L);

    private DataSource source;
    private DataSource target;
    private DataSource meta;
    private JdbcCheckpointStore checkpointStore;
    private EtlEngine engine;
    private JobDescriptor descriptor;

    @BeforeEach
    void setUp() throws Exception {
        source = WatermarkTestFixtures.h2Mem("wm_src");
        target = WatermarkTestFixtures.h2Mem("wm_dst");
        meta = WatermarkTestFixtures.h2Mem("wm_meta");
        WatermarkTestFixtures.createSchema(source, target);
        EtlSchema.ensure(meta);
        checkpointStore = new JdbcCheckpointStore(meta);
        engine = new EtlEngine(new MapDataSourceProvider()
                .register(WatermarkTestFixtures.SOURCE_REF, source)
                .register(WatermarkTestFixtures.TARGET_REF, target), checkpointStore);
        descriptor = WatermarkTestFixtures.job("wm-incremental");
    }

    @Test
    @DisplayName("源表持续写入时增量轮只搬新行，最终对账零差异")
    void incrementalMovesOnlyNewRowsUnderContinuousWrites() throws Exception {
        WatermarkTestFixtures.insertOrders(source, 1L, 500, BASE, 60000L);

        // —— 首跑：全量 ——
        EtlRunResult first = engine.run(descriptor);
        assertEquals("init", first.getMode(), "首跑应是全量");
        assertEquals(500L, first.getRowsRead());
        assertEquals(500L, first.getRowsWritten());
        assertTrue(first.isReconciled(), "首跑对账应一致");
        assertEquals(500L, WatermarkTestFixtures.count(target, WatermarkTestFixtures.TARGET_TABLE));

        Checkpoint afterFirst = checkpointStore.load(descriptor.getName());
        assertNotNull(afterFirst, "首跑后应落水位");
        String watermarkAfterFirst = afterFirst.getWatermarkValue();

        // —— 并发写入：搬运过程中源表还在长 ——
        final AtomicLong written = new AtomicLong(0L);
        Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int chunk = 0; chunk < 3; chunk++) {
                        // 时间戳必须晚于首跑的水位（首跑 500 行 ⇒ 水位 = BASE + 499*60000）
                        WatermarkTestFixtures.insertOrders(source, 1001L + chunk * 100L, 100,
                                new Timestamp(BASE.getTime() + 30000000L + chunk * 100000L), 1000L);
                        written.addAndGet(100L);
                        Thread.sleep(30L);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        writer.start();

        // —— 增量轮 1（与写入并发）——
        EtlRunResult second = engine.run(descriptor);
        assertEquals("incremental", second.getMode(), "有水位后应自动切增量");
        assertEquals(second.getRowsRead(), second.getRowsWritten(),
                "增量轮读到的行应全部是新行（读数 == 写数）");
        assertTrue(second.getRowsRead() <= 300L, "增量轮最多只能读到新增的 300 行，实际 " + second.getRowsRead());

        writer.join();
        assertEquals(300L, written.get(), "并发写入应写入 300 行");

        // —— 增量轮 2：把并发写入剩下补齐 ——
        EtlRunResult third = engine.run(descriptor);
        assertEquals("incremental", third.getMode());
        assertEquals(third.getRowsRead(), third.getRowsWritten(), "增量轮读到的行应全部是新行");

        long totalMoved = first.getRowsWritten() + second.getRowsWritten() + third.getRowsWritten();
        assertEquals(800L, totalMoved, "三轮搬运的行数之和应等于源表总行数（不重不漏）");

        // —— 对账：行数零差异 + 校验和一致 ——
        long sourceCount = WatermarkTestFixtures.count(source, WatermarkTestFixtures.SOURCE_TABLE);
        long targetCount = WatermarkTestFixtures.count(target, WatermarkTestFixtures.TARGET_TABLE);
        assertEquals(800L, sourceCount, "源表应有 800 行");
        assertEquals(sourceCount, targetCount, "行数对账零差异");
        assertTrue(third.isReconciled(), "最后一轮对账应一致（行数 + 校验和）");
        assertEquals(third.getReport().getSourceCount(), third.getReport().getTargetCount());
        assertEquals(third.getReport().getSourceChecksum(), third.getReport().getTargetChecksum());

        // —— 水位推进 ——
        Checkpoint afterThird = checkpointStore.load(descriptor.getName());
        assertNotNull(afterThird);
        assertTrue(afterThird.getWatermarkValue().compareTo(watermarkAfterFirst) > 0,
                "水位应向前推进: " + watermarkAfterFirst + " → " + afterThird.getWatermarkValue());
        assertTrue(third.getCheckpointAdvances() > 0, "增量轮应按页推进水位（页级断点续传）");
    }
}
