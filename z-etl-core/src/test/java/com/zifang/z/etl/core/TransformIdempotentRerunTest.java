package com.zifang.z.etl.core;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 3：同一 job 重跑，**转换结果**幂等。
 *
 * <p>与 013 的区别：013 验的是「搬运」幂等（同样输入搬过去不重不漏）；
 * 本卡验的是「**转换后结果**」幂等 —— 同样输入重跑，转换输出**逐字段**一致。
 *
 * <p>两个重跑口径都跑：
 * <ul>
 *   <li><b>强制全量重跑</b>（mode=init）：整批重读 + 重算 + upsert ⇒ 目标行数不增长、幂等键去重数不变、逐字段一致。</li>
 *   <li><b>自动增量重跑</b>（mode=auto，二次跑）：自动切增量、读 0 行 ⇒ 目标保持不变。</li>
 * </ul>
 *
 * @author zifang
 */
class TransformIdempotentRerunTest {

    private EtlTestFixtures fixtures;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("idem");
        fixtures.createTransformSchema();
        fixtures.seedSource(1000, 1L, new Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());
    }

    private EtlEngine engine() {
        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
        return new EtlEngine(provider, new com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore(fixtures.meta()));
    }

    @Test
    @DisplayName("强制全量重跑：行数不增长、幂等键去重数不变、转换结果逐字段一致")
    void forcedFullRerunKeepsTransformedResultIdentical() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();
        descriptor.setMode("init");

        EtlRunResult first = engine().run(descriptor);
        long rowsAfterFirst = EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed");
        long distinctAfterFirst = EtlTestFixtures.countDistinct(fixtures.target(),
                "etl_trade_order_transformed", "order_id");
        String dumpAfterFirst = EtlTestFixtures.dumpTable(fixtures.target(),
                "etl_trade_order_transformed", "order_id");

        // —— 重跑（同样输入、同样规则、强制全量）
        EtlRunResult second = engine().run(descriptor);

        long rowsAfterSecond = EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed");
        long distinctAfterSecond = EtlTestFixtures.countDistinct(fixtures.target(),
                "etl_trade_order_transformed", "order_id");
        String dumpAfterSecond = EtlTestFixtures.dumpTable(fixtures.target(),
                "etl_trade_order_transformed", "order_id");

        // 行数不增长
        assertEquals(334L, rowsAfterFirst, "首跑行数");
        assertEquals(rowsAfterFirst, rowsAfterSecond, "重跑后行数不应增长");
        // 幂等键去重数不变
        assertEquals(334L, distinctAfterFirst, "首跑幂等键去重数");
        assertEquals(distinctAfterFirst, distinctAfterSecond, "重跑后幂等键去重数不应变化");
        // 逐字段一致（整表规范化导出完全相同）
        assertEquals(dumpAfterFirst, dumpAfterSecond, "重跑后转换结果应逐字段一致");
        // 两次跑读的行数相同（重跑确实把同样的输入重算了一遍）
        assertEquals(first.getRowsRead(), second.getRowsRead(), "两次读取行数应相同");
        assertEquals(334L, second.getRowsRead(), "重跑仍应读 334 行（下推口径不变）");
        // 双口径对账在重跑后依然一致
        assertTrue(second.isReconciled(), "重跑后对账仍应一致");
    }

    @Test
    @DisplayName("自动模式重跑：自动切增量、读 0 行、目标保持不变")
    void autoRerunSwitchesToIncrementalAndKeepsTarget() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();

        EtlRunResult first = engine().run(descriptor);
        assertEquals("init", first.getMode(), "首跑应为全量");
        long rowsAfterFirst = EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed");
        String dumpAfterFirst = EtlTestFixtures.dumpTable(fixtures.target(),
                "etl_trade_order_transformed", "order_id");

        EtlRunResult second = engine().run(descriptor);

        assertEquals("incremental", second.getMode(), "二次跑应自动切增量");
        assertEquals(0L, second.getRowsRead(), "无新数据时增量应读 0 行");
        assertEquals(rowsAfterFirst, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed"),
                "增量重跑后目标行数不变");
        assertEquals(dumpAfterFirst, EtlTestFixtures.dumpTable(fixtures.target(),
                "etl_trade_order_transformed", "order_id"), "增量重跑后转换结果逐字段一致");
    }

    @Test
    @DisplayName("聚合下推作业重跑：分组结果幂等（行数、去重数、逐字段一致）")
    void aggregateJobRerunIsIdempotent() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadSummaryDescriptor();
        descriptor.setMode("init");

        engine().run(descriptor);
        long rowsAfterFirst = EtlTestFixtures.count(fixtures.target(), "etl_status_summary");
        long distinctAfterFirst = EtlTestFixtures.countDistinct(fixtures.target(), "etl_status_summary", "status");
        String dumpAfterFirst = EtlTestFixtures.dumpTable(fixtures.target(), "etl_status_summary", "status");

        engine().run(descriptor);

        assertEquals(3L, rowsAfterFirst, "首跑聚合行数");
        assertEquals(rowsAfterFirst, EtlTestFixtures.count(fixtures.target(), "etl_status_summary"),
                "重跑后聚合行数不应增长");
        assertEquals(distinctAfterFirst, EtlTestFixtures.countDistinct(fixtures.target(),
                "etl_status_summary", "status"), "重跑后分组键去重数不变");
        assertEquals(dumpAfterFirst, EtlTestFixtures.dumpTable(fixtures.target(), "etl_status_summary", "status"),
                "重跑后聚合结果逐字段一致");
    }
}
