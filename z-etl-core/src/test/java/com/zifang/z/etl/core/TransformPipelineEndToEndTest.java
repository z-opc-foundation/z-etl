package com.zifang.z.etl.core;

import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.reader.JdbcRowReader;
import com.zifang.z.etl.core.reader.RowConsumer;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.etl.core.transform.ExpressionTransformer;
import com.zifang.z.etl.core.transform.FieldMappingTransformer;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.transform.ScriptTransformer;
import com.zifang.z.etl.core.transform.TransformChain;
import com.zifang.z.etl.core.transform.TypeNormalizationTransformer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 1：四类转换各配一条真实规则并跑通端到端（源 → 转换 → 目标，对账一致）。
 *
 * <p>四类转换与本用例的对应关系：
 * <ol>
 *   <li><b>字段映射</b>：{@code id → order_id} 改名 + 落库类型声明。</li>
 *   <li><b>SQL pushdown</b>：过滤下推（{@code status = 1}）+ 聚合下推（{@code GROUP BY} + 聚合）。</li>
 *   <li><b>行级表达式</b>：EL（z-util-expr）算状态文案；GROOVY（z-script 沙箱）算含税金额。</li>
 *   <li><b>类型归一</b>：链尾统一校类型（LONG）与定点小数位（scale 2）。</li>
 * </ol>
 *
 * <p>⚠️ 硬边界：全在本地 H2 内存库上跑，数据本地生成，**不连生产 RDS、不在生产库建表**。
 *
 * @author zifang
 */
class TransformPipelineEndToEndTest {

    private EtlTestFixtures fixtures;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("transform");
        fixtures.createTransformSchema();
        fixtures.seedSource(1000, 1L, new Timestamp(1767225600000L)); // 2026-01-01 00:00:00
        EtlSchema.ensure(fixtures.meta());
    }

    private EtlEngine engine() {
        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
        return new EtlEngine(provider, new com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore(fixtures.meta()));
    }

    @Test
    @DisplayName("四类转换各一条真实规则：端到端跑通 + 行数/校验和双口径对账一致")
    void fourTransformKindsRunEndToEndAndReconcile() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();

        EtlRunResult result = engine().run(descriptor);

        // —— 下推口径：过滤下推 ⇒ 只读 status = 1 的明细（源表 1000 行 ⇒ 实际读 334 行）
        assertEquals(334L, result.getRowsRead(), "过滤下推后应只读 status = 1 的 334 行");
        assertEquals(334L, result.getRowsWritten(), "写出行数");
        assertEquals(334L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed"),
                "目标表物理行数");

        // —— 双口径对账：行数 + 幂等键多重集校验和（源侧复现同一个过滤口径）
        assertNotNull(result.getReport());
        assertEquals(334L, result.getReport().getSourceCount(), "源侧行数（带下推过滤）");
        assertEquals(334L, result.getReport().getTargetCount(), "目标侧行数");
        assertTrue(result.getReport().isCountMatched(), "行数应一致");
        assertTrue(result.getReport().isChecksumMatched(), "校验和应一致");
        assertTrue(result.isReconciled(), "整体对账应一致");

        // —— ① 字段映射：id → order_id 改名（源列 id 不再落库）
        assertEquals("SO00000001", EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "order_no"), "订单号应原样搬过来");

        // —— ④ 类型归一：amount 定点 2 位；status 归一成 LONG（目标列是 BIGINT）
        BigDecimal amount = (BigDecimal) EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "amount");
        assertEquals(0, new BigDecimal("101.00").compareTo(amount), "金额应保持定点精度");
        Object status = EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "status");
        assertEquals(Long.valueOf(1L), status, "status 应归一成 LONG");

        // —— ③ 行级表达式：EL 算文案、GROOVY 沙箱算含税金额（BigDecimal 精确，无浮点尾数）
        assertEquals("PAID", EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "status_text"), "EL 表达式应算出状态文案");
        BigDecimal gross = (BigDecimal) EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "gross_amount");
        assertEquals(0, new BigDecimal("114.13").compareTo(gross), "含税金额应为 101.00 * 1.13 = 114.13");
        BigDecimal tax = (BigDecimal) EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "tax_amount");
        assertEquals(0, new BigDecimal("13.13").compareTo(tax), "税额应为 13.13");

        // —— 复杂清洗：脚本沙箱（GROOVY）把支付方式清洗成大写渠道
        assertEquals("ALIPAY", EtlTestFixtures.valueOf(fixtures.target(), "etl_trade_order_transformed",
                "order_id", Long.valueOf(1L), "pay_channel"), "脚本沙箱应清洗出支付渠道");
    }

    @Test
    @DisplayName("过滤下推确实进了源库 SQL（不是把明细拉回引擎再滤）")
    void filterPushdownIsInTheSourceSql() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();
        JdbcRowReader reader = new JdbcRowReader(fixtures.source(), descriptor.getSource(),
                null, null, "TIMESTAMP");
        final int[] read = new int[1];
        reader.read(new RowConsumer() {
            @Override
            public void accept(Map<String, Object> row) {
                read[0]++;
            }
        });
        String sql = reader.lastExecutedSql();
        assertNotNull(sql, "应能取到实际执行的 SQL");
        assertTrue(sql.contains("status = 1"), "过滤条件应下推到源库 SQL，实际: " + sql);
        assertTrue(sql.contains("LIMIT"), "应仍是分页取数，实际: " + sql);
        assertTrue(sql.contains("id, order_no, user_id, amount, status, payment_method, updated_at"),
                "应是列裁剪投影（只取用得上的列），实际: " + sql);
    }

    @Test
    @DisplayName("聚合下推：GROUP BY + 聚合在源库算完，引擎只拿 3 行聚合结果")
    void aggregatePushdownMovesAggregationToSource() throws Exception {
        JobDescriptor descriptor = EtlTestFixtures.loadSummaryDescriptor();

        EtlRunResult result = engine().run(descriptor);

        assertEquals(3L, result.getRowsRead(), "聚合下推后引擎只应读到 3 行分组结果（不是 1000 行明细）");
        assertEquals(3L, result.getRowsWritten(), "写出行数");
        assertEquals(3L, EtlTestFixtures.count(fixtures.target(), "etl_status_summary"), "目标表行数");

        assertNotNull(result.getReport());
        assertEquals(3L, result.getReport().getSourceCount(), "源侧行数（聚合口径）");
        assertEquals(3L, result.getReport().getTargetCount(), "目标侧行数");
        assertTrue(result.getReport().isCountMatched(), "行数应一致");
        assertTrue(result.getReport().isChecksumMatched(), "校验和应一致");

        // status = 1 的分组：334 行，金额合计与均值由源库算好后经表达式/归一落库
        assertEquals(334L, EtlTestFixtures.valueOf(fixtures.target(), "etl_status_summary",
                "status", Integer.valueOf(1), "order_cnt"), "分组计数应与明细口径一致");
        BigDecimal total = (BigDecimal) EtlTestFixtures.valueOf(fixtures.target(), "etl_status_summary",
                "status", Integer.valueOf(1), "total_amount");
        assertNotNull(total);
        BigDecimal avg = (BigDecimal) EtlTestFixtures.valueOf(fixtures.target(), "etl_status_summary",
                "status", Integer.valueOf(1), "avg_amount");
        assertNotNull(avg);
        assertEquals(0, total.divide(new BigDecimal("334"), 2, java.math.RoundingMode.HALF_UP)
                .compareTo(avg), "均值应等于合计 / 计数");
    }

    @Test
    @DisplayName("转换链逐算子核对：表达式产出、类型归一（LONG / 定点 2 位）")
    void chainLevelAssertionsOnEachOperator() {
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();
        ScriptSandbox sandbox = new ScriptSandbox();
        try {
            TransformChain chain = new TransformChain();
            chain.add(new FieldMappingTransformer(descriptor.getTransform()));
            chain.add(new ExpressionTransformer(descriptor.getTransform().getExpressions(), sandbox));
            chain.add(new ScriptTransformer(descriptor.getTransform().getScripts(), sandbox));
            chain.add(new TypeNormalizationTransformer(descriptor.getTransform().getTypeNormalizations()));

            Map<String, Object> source = new LinkedHashMap<String, Object>();
            source.put("id", Long.valueOf(7L));
            source.put("order_no", "SO00000007");
            source.put("user_id", Long.valueOf(1007L));
            source.put("amount", new BigDecimal("101.00"));
            source.put("status", Integer.valueOf(1));
            source.put("payment_method", "alipay");
            source.put("updated_at", new Timestamp(1767225600000L));

            Map<String, Object> out = chain.transform(source);

            // 字段映射：id → order_id
            assertEquals(Long.valueOf(7L), out.get("order_id"), "id 应映射为 order_id");
            // 行级表达式
            assertEquals("PAID", out.get("status_text"), "EL 表达式应产出状态文案");
            assertEquals(0, new BigDecimal("114.130000").compareTo((BigDecimal) out.get("gross_amount")),
                    "GROOVY 沙箱应产出 BigDecimal 含税金额");
            // 脚本沙箱
            assertEquals("ALIPAY", out.get("pay_channel"), "脚本沙箱应产出清洗后的渠道");
            // 类型归一：status → LONG；金额定点 2 位
            assertEquals(Long.valueOf(1L), out.get("status"), "status 应归一成 LONG");
            assertEquals(2, ((BigDecimal) out.get("gross_amount")).scale(), "含税金额应收口成 2 位小数");
            assertEquals(2, ((BigDecimal) out.get("amount")).scale(), "金额应收口成 2 位小数");
        } finally {
            sandbox.close();
        }
    }

    @Test
    @DisplayName("同一份作业的 JSON 形态与 yml 形态等价（新算子字段两形态都解析得到）")
    void jsonAndYamlFormsAreEquivalent() {
        JobDescriptor yaml = EtlTestFixtures.loadTransformDescriptor();
        JobDescriptor json = EtlTestFixtures.loadTransformJsonDescriptor();

        assertEquals(3, yaml.getTransform().getExpressions().size(), "yml 形态应有 3 条表达式");
        assertEquals(3, json.getTransform().getExpressions().size(), "JSON 形态应有 3 条表达式");
        assertEquals(1, yaml.getTransform().getScripts().size(), "yml 形态应有 1 个脚本步骤");
        assertEquals(1, json.getTransform().getScripts().size(), "JSON 形态应有 1 个脚本步骤");
        assertEquals(4, yaml.getTransform().getTypeNormalizations().size(), "yml 形态应有 4 条类型归一");
        assertEquals(4, json.getTransform().getTypeNormalizations().size(), "JSON 形态应有 4 条类型归一");
        assertEquals("status = 1", yaml.getSource().getPushdown().getFilter(), "yml 形态应带下推过滤");
        assertEquals("status = 1", json.getSource().getPushdown().getFilter(), "JSON 形态应带下推过滤");
    }
}
