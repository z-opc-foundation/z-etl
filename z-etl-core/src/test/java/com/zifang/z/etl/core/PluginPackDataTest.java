package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 插件包（数据侧）验收：hash / mask / date.format / sort。
 *
 * <p>hash 是确定性摘要（同值同摘要）；mask 四规则各有形状；date.format 三种输入口径；
 * sort 是物化算子（有界数据整流）。
 *
 * @author zifang
 */
class PluginPackDataTest {

    @Test
    @DisplayName("hash：SHA-256 原地覆盖；suffix 保留原列加新列；null 保持 null；同值同摘要")
    void hash() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-hash\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {phone: '13800001111', remark: null}\n"
                + "        - {phone: '13800001111', remark: x}\n"
                + "  - id: hs\n"
                + "    type: hash\n"
                + "    config:\n"
                + "      columns: [phone]\n"
                + "      algorithm: SHA-256\n"
                + "      suffix: _fp\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: hs}\n"
                + "  - {from: hs, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(rows.get(0).get("phone_fp"), rows.get(1).get("phone_fp"), "同值同摘要");
        assertEquals(Long.valueOf(13800001111L), rows.get(0).get("phone"),
                "带 suffix 保留原列（inline 源把数字串推断成 Long）");
        assertEquals(64, String.valueOf(rows.get(0).get("phone_fp")).length(), "SHA-256 十六进制 64 位");
        assertNull(rows.get(0).get("remark_fp"), "null 值不出指纹列");
    }

    @Test
    @DisplayName("hash 原地覆盖（缺省无 suffix）+ 算法白名单")
    void hashInPlaceAndValidation() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-hash-inplace\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{phone: '13800001111'}]}\n"
                + "  - id: hs\n"
                + "    type: hash\n"
                + "    config:\n"
                + "      columns: [phone]\n"
                + "      algorithm: MD5\n"
                + "      salt: pepper\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: hs}\n"
                + "  - {from: hs, to: out}\n");
        String hashed = String.valueOf(rowsOf(result).get(0).get("phone"));
        assertEquals(32, hashed.length(), "MD5 十六进制 32 位");
        assertNotEquals("13800001111", hashed);
    }

    @Test
    @DisplayName("hash 声明坏算法 ⇒ 声明期报错")
    void hashBadAlgorithm() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: plugin-hash-bad\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{a: 1}]}\n"
                + "  - id: hs\n"
                + "    type: hash\n"
                + "    config: {columns: [a], algorithm: CRC32}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: hs}\n"
                + "  - {from: hs, to: out}\n"));
        assertTrue(error.getMessage().contains("algorithm"), error.getMessage());
    }

    @Test
    @DisplayName("mask：phone / email / half / full 四规则；null 不动")
    void mask() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-mask\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {phone: '13800001111', email: 'zhang@example.com', remark: '内部备注', note: null}\n"
                + "  - id: mk\n"
                + "    type: mask\n"
                + "    config:\n"
                + "      masks: {phone: phone, email: email, remark: full, note: half}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: mk}\n"
                + "  - {from: mk, to: out}\n");
        Map<String, Object> row = rowsOf(result).get(0);
        assertEquals("138****1111", row.get("phone"));
        assertEquals("z***@example.com", row.get("email"));
        assertEquals("****", row.get("remark"));
        assertNull(row.get("note"), "null 不动");
    }

    @Test
    @DisplayName("mask 声明坏规则 ⇒ 声明期报错")
    void maskBadRule() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: plugin-mask-bad\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{a: 1}]}\n"
                + "  - id: mk\n"
                + "    type: mask\n"
                + "    config: {masks: {a: scramble}}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: mk}\n"
                + "  - {from: mk, to: out}\n"));
        assertTrue(error.getMessage().contains("phone|email|half|full"), error.getMessage());
    }

    @Test
    @DisplayName("date.format：epoch 毫秒 / ISO / 自定义 fromPattern 三口径")
    void dateFormat() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-date\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {k: a, at: 1700000000000}\n"
                + "        - {k: b, at: '2026-11-19 10:00:00'}\n"
                + "  - id: d1\n"
                + "    type: date.format\n"
                + "    config: {column: at, toPattern: 'yyyy/MM/dd'}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: d1}\n"
                + "  - {from: d1, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("2023/11/15", rows.get(0).get("at"), "epoch 毫秒 1700000000000 = 2023-11-15 (系统时区)");
        assertEquals("2026/11/19", rows.get(1).get("at"), "ISO 字符串");
    }

    @Test
    @DisplayName("date.format fromPattern 解析自定义格式 + name 输出到新列")
    void dateFormatFromPattern() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-date-pattern\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{at: '19/11/2026 10:00'}]}\n"
                + "  - id: d1\n"
                + "    type: date.format\n"
                + "    config: {column: at, fromPattern: 'dd/MM/yyyy HH:mm', toPattern: 'yyyy-MM-dd HH:mm:ss', name: at_std}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: d1}\n"
                + "  - {from: d1, to: out}\n");
        Map<String, Object> row = rowsOf(result).get(0);
        assertEquals("2026-11-19 10:00:00", row.get("at_std"));
        assertEquals("19/11/2026 10:00", row.get("at"), "name 落新列，原列保留");
    }

    @Test
    @DisplayName("date.format 解析失败炸行（不静默 null）")
    void dateFormatFailFast() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> run(""
                + "name: plugin-date-bad\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{at: 'not-a-date'}]}\n"
                + "  - id: d1\n"
                + "    type: date.format\n"
                + "    config: {column: at}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: d1}\n"
                + "  - {from: d1, to: out}\n"));
        assertTrue(error.getMessage().contains("解析不了时间值"), error.getMessage());
    }

    @Test
    @DisplayName("sort：多列 + 降序前缀；数值按数值比；flush 时整流输出")
    void sort() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-sort\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {city: 北京, amount: 50}\n"
                + "        - {city: 上海, amount: 90}\n"
                + "        - {city: 北京, amount: 80}\n"
                + "  - id: st\n"
                + "    type: sort\n"
                + "    config:\n"
                + "      columns: [city, -amount]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: st}\n"
                + "  - {from: st, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("上海", rows.get(0).get("city"), "列序按字典（码点）序：上 < 北");
        assertEquals(Integer.valueOf(90), rows.get(0).get("amount"));
        assertEquals("北京", rows.get(1).get("city"));
        assertEquals(Integer.valueOf(80), rows.get(1).get("amount"), "同城内金额降序");
        assertEquals(Integer.valueOf(50), rows.get(2).get("amount"));
    }

    @Test
    @DisplayName("sort 缺 columns ⇒ 声明期报错")
    void sortValidation() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: plugin-sort-bad\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{a: 1}]}\n"
                + "  - id: st\n"
                + "    type: sort\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: st}\n"
                + "  - {from: st, to: out}\n"));
        assertTrue(error.getMessage().contains("columns"), error.getMessage());
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }
}
