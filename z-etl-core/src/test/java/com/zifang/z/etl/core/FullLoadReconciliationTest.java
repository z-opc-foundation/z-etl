package com.zifang.z.etl.core;

import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 1：一条真实存量表 → 另一张表的全量搬运跑通，行数对账一致。
 *
 * <p>源表照生产 {@code z_trade_order} 的形状，数据本地生成，跑在两个独立的 H2 内存库上
 * （跨数据源搬运），**不连任何生产库**。
 *
 * @author zifang
 */
class FullLoadReconciliationTest {

    private EtlTestFixtures fixtures;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("full");
        fixtures.createSchema();
        fixtures.seedSource(1000, 1L, new Timestamp(1767225600000L)); // 2026-01-01 00:00:00
        EtlSchema.ensure(fixtures.meta());
    }

    @Test
    @DisplayName("全量搬运 1000 行，源/目标行数一致且校验和一致")
    void fullLoadMovesAllRowsAndReconciles() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadYamlDescriptor();
        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
        EtlEngine engine = new EtlEngine(provider, new com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore(
                fixtures.meta()));

        EtlRunResult result = engine.run(descriptor);

        // 首跑走全量（初始化模式）
        assertEquals("init", result.getMode(), "首跑应为初始化模式（全量）");
        assertEquals(1000L, result.getRowsRead(), "读出行数");
        assertEquals(1000L, result.getRowsWritten(), "写出行数");

        // 双口径对账：行数 + 校验和
        assertNotNull(result.getReport());
        assertEquals(1000L, result.getReport().getSourceCount(), "源侧行数");
        assertEquals(1000L, result.getReport().getTargetCount(), "目标侧行数");
        assertTrue(result.getReport().isCountMatched(), "行数应一致");
        assertTrue(result.getReport().isChecksumMatched(), "校验和应一致");
        assertTrue(result.isReconciled(), "整体对账应一致");

        // 物理落库口径复核（不只看报告对象）
        assertEquals(1000L, EtlTestFixtures.count(fixtures.source(), "z_trade_order"));
        assertEquals(1000L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));

        // 字段映射 + 类型归一抽查：id → order_id（改名）、amount → DECIMAL、status → INT
        assertEquals("SO00000001", EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_snapshot",
                "order_id", Long.valueOf(1L), "order_no"));
        BigDecimal amount = (BigDecimal) EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_snapshot",
                "order_id", Long.valueOf(1L), "amount");
        assertNotNull(amount);
        assertEquals(0, new BigDecimal("101.00").compareTo(amount), "金额应保持 BigDecimal 精度");
        Object status = EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_snapshot",
                "order_id", Long.valueOf(1L), "status");
        assertEquals(Integer.valueOf(1), status, "状态列应归一成 INT");
    }
}
