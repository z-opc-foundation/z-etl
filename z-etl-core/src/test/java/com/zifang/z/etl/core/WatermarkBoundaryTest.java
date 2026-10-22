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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 卡面「边界处理」四类行为的可判据实测：
 * <ol>
 *   <li><b>同秒多条</b>：时间戳相同的行靠平局列（自增 id）接力，不漏行；单键游标会漏（对照组）。</li>
 *   <li><b>时钟回拨 / 迟到数据</b>：源侧水位早于游标的行会被识别并报数（滞留行数）；配回溯窗口能兜住。</li>
 *   <li><b>游标越界（源表被清）</b>：明确失败且**不推进水位**；显式配 RESET 才清水位退回全量。</li>
 *   <li><b>失败不推进（断点续传语义）</b>：中途写失败 ⇒ 水位停在上一个成功页；修好后重跑补齐，对账一致。</li>
 * </ol>
 *
 * <p>全部在本地 H2 上跑，不连任何真实库。
 *
 * @author zifang
 */
class WatermarkBoundaryTest {

    private static final Timestamp BASE = new Timestamp(1767225600000L);

    private DataSource source;
    private DataSource target;
    private DataSource meta;
    private JdbcCheckpointStore checkpointStore;
    private EtlEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        source = WatermarkTestFixtures.h2Mem("wm_b_src");
        target = WatermarkTestFixtures.h2Mem("wm_b_dst");
        meta = WatermarkTestFixtures.h2Mem("wm_b_meta");
        WatermarkTestFixtures.createSchema(source, target);
        EtlSchema.ensure(meta);
        checkpointStore = new JdbcCheckpointStore(meta);
        engine = new EtlEngine(new MapDataSourceProvider()
                .register(WatermarkTestFixtures.SOURCE_REF, source)
                .register(WatermarkTestFixtures.TARGET_REF, target), checkpointStore);
    }

    // ——— 1. 同秒多条 ————————————————————————————————————————————

    @Test
    @DisplayName("同秒多条：复合游标（平局列）不漏行，单键游标会漏（对照组）")
    void sameTimestampRowsNeedTiebreakColumn() throws Exception {
        // 首跑：10 行，时间戳**完全相同**
        WatermarkTestFixtures.insertSameTimestamp(source, 1L, 10, BASE);
        JobDescriptor tiebreakJob = WatermarkTestFixtures.job("wm-tiebreak", "id", 0L, "FAIL", 100);
        EtlRunResult first = engine.run(tiebreakJob);
        assertEquals("init", first.getMode());
        assertEquals(10L, first.getRowsRead());

        Checkpoint checkpoint = checkpointStore.load(tiebreakJob.getName());
        assertNotNull(checkpoint);
        assertEquals("id", checkpoint.getTiebreakColumn(), "复合游标应记下平局列");
        assertEquals("10", checkpoint.getTiebreakValue(), "平局列值应停在已搬走的最大 id 上");

        // 再来 5 行，**时间戳与上一批完全相同**（同秒多条）
        WatermarkTestFixtures.insertSameTimestamp(source, 11L, 5, BASE);

        EtlRunResult second = engine.run(tiebreakJob);
        assertEquals("incremental", second.getMode());
        assertEquals(5L, second.getRowsRead(), "同秒的新行必须被搬走（靠平局列接力）");
        assertEquals(15L, WatermarkTestFixtures.count(target, WatermarkTestFixtures.TARGET_TABLE));

        // 对照组：同一份数据，单键游标（同秒的行会被漏掉）
        DataSource source2 = WatermarkTestFixtures.h2Mem("wm_b_src2");
        DataSource target2 = WatermarkTestFixtures.h2Mem("wm_b_dst2");
        DataSource meta2 = WatermarkTestFixtures.h2Mem("wm_b_meta2");
        WatermarkTestFixtures.createSchema(source2, target2);
        EtlSchema.ensure(meta2);
        JdbcCheckpointStore store2 = new JdbcCheckpointStore(meta2);
        EtlEngine engine2 = new EtlEngine(new MapDataSourceProvider()
                .register(WatermarkTestFixtures.SOURCE_REF, source2)
                .register(WatermarkTestFixtures.TARGET_REF, target2), store2);
        JobDescriptor singleKeyJob = WatermarkTestFixtures.job("wm-singlekey", null, 0L, "FAIL", 100);
        WatermarkTestFixtures.insertSameTimestamp(source2, 1L, 10, BASE);
        engine2.run(singleKeyJob);
        WatermarkTestFixtures.insertSameTimestamp(source2, 11L, 5, BASE);
        EtlRunResult singleKeySecond = engine2.run(singleKeyJob);
        assertEquals(0L, singleKeySecond.getRowsRead(),
                "对照组：单键游标（> 严格大于）会把同秒的新行漏掉 —— 这正是要配平局列的原因");
        assertEquals(10L, WatermarkTestFixtures.count(target2, WatermarkTestFixtures.TARGET_TABLE));
    }

    // ——— 2. 时钟回拨 / 迟到数据 ————————————————————————————————————

    @Test
    @DisplayName("时钟回拨：滞留行被识别并报数；配回溯窗口可兜住")
    void clockRollbackIsDetectedAndRecoverableWithOverlap() throws Exception {
        WatermarkTestFixtures.insertOrders(source, 1L, 10, BASE, 60000L);
        JobDescriptor job = WatermarkTestFixtures.job("wm-rollback", "id", 0L, "FAIL", 100);
        EtlRunResult first = engine.run(job);
        assertEquals(10L, first.getRowsRead());
        Checkpoint afterFirst = checkpointStore.load(job.getName());
        assertNotNull(afterFirst);

        // 时钟回拨：写入一行**时间戳早于当前水位**的行（源库时钟往回拨 / 迟到数据）
        WatermarkTestFixtures.insertOrder(source, 999L, "SO-rollback", new Timestamp(BASE.getTime() - 600000L));

        EtlRunResult second = engine.run(job);
        assertEquals("incremental", second.getMode());
        assertEquals(1L, second.getStaleRows(),
                "引擎应识别出 1 行水位值早于当前游标且还没搬进目标（时钟回拨 / 迟到数据）");
        assertTrue(hasNote(second, "时钟回拨"),
                "结果里应有明确告警，实际 notes=" + second.getNotes());
        assertEquals(0L, second.getRowsRead(), "纯时间游标搬不到水位之前的行（口径限制，已告警）");
        assertFalse(second.isReconciled(), "目标侧少那一行 ⇒ 对账应报不一致");

        // 配回溯窗口（overlap）⇒ 同样的迟到行能被兜住
        DataSource source2 = WatermarkTestFixtures.h2Mem("wm_b_src3");
        DataSource target2 = WatermarkTestFixtures.h2Mem("wm_b_dst3");
        DataSource meta2 = WatermarkTestFixtures.h2Mem("wm_b_meta3");
        WatermarkTestFixtures.createSchema(source2, target2);
        EtlSchema.ensure(meta2);
        JdbcCheckpointStore store2 = new JdbcCheckpointStore(meta2);
        EtlEngine engine2 = new EtlEngine(new MapDataSourceProvider()
                .register(WatermarkTestFixtures.SOURCE_REF, source2)
                .register(WatermarkTestFixtures.TARGET_REF, target2), store2);
        // 窗口 2000 秒：水位 BASE+540000，迟到行在 BASE-600000 ⇒ 落在窗口内
        JobDescriptor overlapJob = WatermarkTestFixtures.job("wm-rollback-overlap", "id", 2000L, "FAIL", 100);
        WatermarkTestFixtures.insertOrders(source2, 1L, 10, BASE, 60000L);
        engine2.run(overlapJob);
        WatermarkTestFixtures.insertOrder(source2, 999L, "SO-rollback", new Timestamp(BASE.getTime() - 600000L));
        EtlRunResult overlapRun = engine2.run(overlapJob);
        assertTrue(overlapRun.getRowsRead() >= 1, "回溯窗口内的迟到行应被重新读到");
        assertEquals(11L, WatermarkTestFixtures.count(target2, WatermarkTestFixtures.TARGET_TABLE),
                "目标侧 upsert 幂等 ⇒ 迟到行补进目标，不产生重复行");
        assertTrue(overlapRun.isReconciled(), "补捞之后对账应一致");
        assertEquals(0L, overlapRun.getStaleRows(), "补捞之后不应再有滞留行");
    }

    // ——— 3. 游标越界（源表被清） ————————————————————————————————————

    @Test
    @DisplayName("游标越界：明确失败且不推进水位；RESET 策略才清水位退回全量")
    void outOfRangeFailsWithoutAdvancingWatermark() throws Exception {
        WatermarkTestFixtures.insertOrders(source, 1L, 100, BASE, 60000L);
        JobDescriptor job = WatermarkTestFixtures.job("wm-outofrange", "id", 0L, "FAIL", 100);
        engine.run(job);
        Checkpoint before = checkpointStore.load(job.getName());
        assertNotNull(before);
        String watermarkBefore = before.getWatermarkValue();

        // 源表被清
        WatermarkTestFixtures.truncateSource(source);

        IllegalStateException error = assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                engine.run(job);
            }
        });
        assertTrue(error.getMessage().contains("游标越界"), "错误信息应明确指出游标越界: " + error.getMessage());
        assertTrue(error.getMessage().contains("不推进水位"), "错误信息应说明不推进水位: " + error.getMessage());

        Checkpoint after = checkpointStore.load(job.getName());
        assertNotNull(after, "越界时水位记录不应被抹掉");
        assertEquals(watermarkBefore, after.getWatermarkValue(), "越界时水位**不推进**（保持原值）");

        // RESET 策略：显式清水位 ⇒ 本轮退回全量
        JobDescriptor resetJob = WatermarkTestFixtures.job("wm-outofrange-reset", "id", 0L, "RESET", 100);
        engine.run(resetJob);
        WatermarkTestFixtures.insertOrders(source, 5001L, 20, BASE, 60000L);
        EtlRunResult resetRun = engine.run(resetJob);
        assertEquals("init", resetRun.getMode(), "RESET 策略下越界后应退回全量重跑");
        assertEquals(20L, resetRun.getRowsRead());
        assertEquals(120L, WatermarkTestFixtures.count(target, WatermarkTestFixtures.TARGET_TABLE),
                "前一个作业搬的 100 行 + 本次重跑补的 20 行");
    }

    // ——— 4. 失败不推进（断点续传语义） ——————————————————————————————————

    @Test
    @DisplayName("失败不推进：中途写失败水位停在成功页，修好后重跑补齐并对账一致")
    void failedPageDoesNotAdvanceWatermark() throws Exception {
        // 目标侧加一条 CHECK 约束：status = 999 的行写不进去 ⇒ 制造「中途写失败」
        WatermarkTestFixtures.execSql(target,
                "ALTER TABLE " + WatermarkTestFixtures.TARGET_TABLE + " ADD CONSTRAINT ck_status CHECK (status <> 999)");
        // 500 行，分页 100 ⇒ 5 页；坏行 id=401 落在第 5 页 ⇒ 前 4 页已提交
        WatermarkTestFixtures.insertOrders(source, 1L, 400, BASE, 60000L);
        WatermarkTestFixtures.insertOrder(source, 401L, "SO-bad-401", 999,
                new Timestamp(BASE.getTime() + 400 * 60000L));
        WatermarkTestFixtures.insertOrders(source, 402L, 99, new Timestamp(BASE.getTime() + 401 * 60000L), 60000L);

        final JobDescriptor job = WatermarkTestFixtures.job("wm-failstop", "id", 0L, "FAIL", 100);
        assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                engine.run(job);
            }
        }, "第 5 页写失败应让整轮失败");

        Checkpoint afterFail = checkpointStore.load(job.getName());
        assertNotNull(afterFail, "失败前已成功的页应留下水位（断点）");
        assertNotNull(afterFail.getWatermarkValue());
        long committed = WatermarkTestFixtures.count(target, WatermarkTestFixtures.TARGET_TABLE);
        assertTrue(committed > 0L && committed < 500L,
                "失败时应只提交了前面成功的页，实际提交行数 " + committed);

        // 修好坏行后重跑 ⇒ 从断点继续，最终对账一致
        WatermarkTestFixtures.updateStatus(source, 401L, 1);
        EtlRunResult rerun = engine.run(job);
        assertEquals("incremental", rerun.getMode(), "有水位 ⇒ 重跑应是增量（从断点继续）");
        assertEquals(500L, WatermarkTestFixtures.count(target, WatermarkTestFixtures.TARGET_TABLE));
        assertEquals(500L, WatermarkTestFixtures.count(source, WatermarkTestFixtures.SOURCE_TABLE));
        assertTrue(rerun.isReconciled(), "重跑后对账应一致");
        assertFalse(rerun.isWatermarkOutOfRange());
    }

    private static boolean hasNote(EtlRunResult result, String keyword) {
        for (String note : result.getNotes()) {
            if (note != null && note.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
}
