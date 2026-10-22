package com.zifang.z.etl.core;

import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.reconcile.Reconciler;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

/**
 * 证据输出：把三项验收的**读数**原样打到 stdout（取证脚本抓这一段进 evidence）。
 *
 * <p>这不是断言型用例，是取证用的读数打印：断言在
 * {@link TransformPipelineEndToEndTest} / {@link SandboxGuardTest} /
 * {@link TransformIdempotentRerunTest} 里，本类只负责把数字摊平到日志里。
 *
 * @author zifang
 */
class TransformEvidenceDumpTest {

    @Test
    @DisplayName("证据读数：四类转换端到端 + 双口径对账 + 重跑幂等")
    void dumpEvidenceReadings() throws Exception {
        EtlTestFixtures fixtures = new EtlTestFixtures("evidence");
        fixtures.createTransformSchema();
        fixtures.seedSource(1000, 1L, new Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());

        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
        EtlEngine engine = new EtlEngine(provider,
                new com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore(fixtures.meta()));
        Reconciler reconciler = new Reconciler();

        System.out.println("=== 读数开始 ===");
        System.out.println("源表 z_trade_order 行数 = " + EtlTestFixtures.count(fixtures.source(), "z_trade_order"));
        System.out.println("源表 status = 1 的行数（过滤下推口径） = "
                + reconciler.count(fixtures.source(), "z_trade_order", "status = 1"));

        // —— 四类转换端到端
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();
        descriptor.setMode("init");
        EtlRunResult result = engine.run(descriptor);
        System.out.println("[明细作业] mode = " + result.getMode()
                + " / 读 " + result.getRowsRead() + " 行 / 写 " + result.getRowsWritten() + " 行");
        System.out.println("[明细作业] 对账：源侧行数 = " + result.getReport().getSourceCount()
                + " / 目标侧行数 = " + result.getReport().getTargetCount()
                + " / 行数一致 = " + result.getReport().isCountMatched()
                + " / 校验和一致 = " + result.getReport().isChecksumMatched()
                + " / 源校验和 = " + result.getReport().getSourceChecksum()
                + " / 目标校验和 = " + result.getReport().getTargetChecksum());
        System.out.println("[明细作业] 目标表 etl_trade_order_transformed 行数 = "
                + EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed")
                + " / COUNT(DISTINCT order_id) = "
                + EtlTestFixtures.countDistinct(fixtures.target(), "etl_trade_order_transformed", "order_id"));
        System.out.println("[明细作业] 抽查 order_id=1：order_no = "
                + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "order_no")
                + " / amount = " + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "amount")
                + " / status = " + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "status")
                + " / status_text = " + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "status_text")
                + " / gross_amount = " + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "gross_amount")
                + " / tax_amount = " + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "tax_amount")
                + " / pay_channel = " + EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "pay_channel"));

        // —— 重跑幂等（强制全量）
        long rowsBefore = EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed");
        long distinctBefore = EtlTestFixtures.countDistinct(fixtures.target(),
                "etl_trade_order_transformed", "order_id");
        String dumpBefore = EtlTestFixtures.dumpTable(fixtures.target(), "etl_trade_order_transformed", "order_id");
        EtlRunResult rerun = engine.run(descriptor);
        long rowsAfter = EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed");
        long distinctAfter = EtlTestFixtures.countDistinct(fixtures.target(),
                "etl_trade_order_transformed", "order_id");
        String dumpAfter = EtlTestFixtures.dumpTable(fixtures.target(), "etl_trade_order_transformed", "order_id");
        System.out.println("[重跑幂等] mode = " + rerun.getMode() + " / 读 " + rerun.getRowsRead() + " 行");
        System.out.println("[重跑幂等] 行数 " + rowsBefore + " → " + rowsAfter
                + "（不增长 = " + (rowsBefore == rowsAfter) + "）");
        System.out.println("[重跑幂等] COUNT(DISTINCT order_id) " + distinctBefore + " → " + distinctAfter
                + "（不变 = " + (distinctBefore == distinctAfter) + "）");
        System.out.println("[重跑幂等] 逐字段一致（整表规范化导出全等）= " + dumpBefore.equals(dumpAfter));
        System.out.println("[重跑幂等] 整表校验和（全部列）= "
                + reconciler.checksum(fixtures.target(), "etl_trade_order_transformed"));

        // —— 聚合下推
        JobDescriptor summary = EtlTestFixtures.loadSummaryDescriptor();
        summary.setMode("init");
        EtlRunResult summaryResult = engine.run(summary);
        System.out.println("[聚合下推作业] 读 " + summaryResult.getRowsRead() + " 行（源表明细 1000 行，聚合在源库算完）");
        System.out.println("[聚合下推作业] 对账：源侧行数 = " + summaryResult.getReport().getSourceCount()
                + " / 目标侧行数 = " + summaryResult.getReport().getTargetCount()
                + " / 行数一致 = " + summaryResult.getReport().isCountMatched()
                + " / 校验和一致 = " + summaryResult.getReport().isChecksumMatched());
        System.out.println("[聚合下推作业] status=1：order_cnt = "
                + EtlTestFixtures.valueOf(fixtures.target(), "etl_status_summary", "status", Integer.valueOf(1), "order_cnt")
                + " / total_amount = "
                + EtlTestFixtures.valueOf(fixtures.target(), "etl_status_summary", "status", Integer.valueOf(1), "total_amount")
                + " / avg_amount = "
                + EtlTestFixtures.valueOf(fixtures.target(), "etl_status_summary", "status", Integer.valueOf(1), "avg_amount"));
        System.out.println("=== 读数结束 ===");
    }
}
