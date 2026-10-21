package com.zifang.z.etl.core;

import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import com.zifang.z.etl.core.job.JobDescriptorValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业描述解析与校验（JSON / yml 两种形态等价）。
 *
 * @author zifang
 */
class JobDescriptorParserTest {

    @Test
    @DisplayName("yml 形态解析正确")
    void parsesYaml() {
        JobDescriptor descriptor = JobDescriptorParser.parse(
                EtlTestFixtures.readResource("/jobs/trade-order-to-snapshot.yml"));
        assertEquals("trade-order-to-snapshot", descriptor.getName());
        assertEquals("auto", descriptor.getMode());
        assertEquals("ds-oc-main", descriptor.getSource().getDataSourceRef());
        assertEquals("z_trade_order", descriptor.getSource().getTable());
        assertEquals(200, descriptor.getSource().getPageSize());
        assertEquals("updated_at", descriptor.getSource().getWatermarkColumn());
        assertEquals(6, descriptor.getTransform().getFieldMappings().size());
        assertEquals("order_id", descriptor.getTransform().getFieldMappings().get(0).effectiveTarget());
        assertEquals("BIGDECIMAL", descriptor.getTransform().getFieldMappings().get(3).getType());
        assertEquals("ds-etl-landing", descriptor.getTarget().getDataSourceRef());
        assertEquals(100, descriptor.getTarget().getBatchSize());
        assertEquals(1, descriptor.getIdempotencyKey().size());
        assertTrue(descriptor.hasWatermark());
        JobDescriptorValidator.validate(descriptor);
    }

    @Test
    @DisplayName("JSON 形态解析正确，且与 yml 形态等价")
    void parsesJson() {
        JobDescriptor json = JobDescriptorParser.parse(
                EtlTestFixtures.readResource("/jobs/trade-order-to-snapshot.json"));
        JobDescriptor yaml = JobDescriptorParser.parse(
                EtlTestFixtures.readResource("/jobs/trade-order-to-snapshot.yml"));
        assertEquals(json.getSource().getTable(), yaml.getSource().getTable());
        assertEquals(json.getTarget().getTable(), yaml.getTarget().getTable());
        assertEquals(json.getIdempotencyKey(), yaml.getIdempotencyKey());
        assertEquals(json.getTransform().getFieldMappings().size(),
                yaml.getTransform().getFieldMappings().size());
        assertEquals(json.getWatermark().getType(), yaml.getWatermark().getType());
        assertEquals("trade-order-to-snapshot-json", json.getName());
        JobDescriptorValidator.validate(json);
    }

    @Test
    @DisplayName("描述可反向序列化回 JSON / yml（落库两形态）")
    void serializesBackToBothForms() {
        JobDescriptor descriptor = EtlTestFixtures.loadYamlDescriptor();
        String json = JobDescriptorParser.toJson(descriptor);
        String yml = JobDescriptorParser.toYaml(descriptor);
        assertTrue(json.contains("trade-order-to-snapshot"));
        assertTrue(yml.contains("trade-order-to-snapshot"));
        // 往返一致
        JobDescriptor roundTrip = JobDescriptorParser.parse(yml);
        assertEquals(descriptor.getName(), roundTrip.getName());
        assertEquals(descriptor.getSource().getTable(), roundTrip.getSource().getTable());
        assertEquals(descriptor.getIdempotencyKey(), roundTrip.getIdempotencyKey());
    }

    @Test
    @DisplayName("非法标识符 / 缺幂等键 被校验拦下")
    void rejectsIllegalDescriptors() {
        JobDescriptor descriptor = EtlTestFixtures.loadYamlDescriptor();

        // 表名注入
        descriptor.getSource().setTable("z_trade_order; DROP TABLE x");
        List<String> errors = JobDescriptorValidator.errors(descriptor);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("source.table"));

        // 缺幂等键
        descriptor = EtlTestFixtures.loadYamlDescriptor();
        descriptor.setIdempotencyKey(new java.util.ArrayList<String>());
        errors = JobDescriptorValidator.errors(descriptor);
        assertTrue(errors.contains("idempotencyKey 不能为空（重跑幂等靠它）"));

        // 非法模式
        descriptor = EtlTestFixtures.loadYamlDescriptor();
        descriptor.setMode("whatever");
        errors = JobDescriptorValidator.errors(descriptor);
        assertTrue(errors.get(0).contains("mode 非法"));
    }
}
