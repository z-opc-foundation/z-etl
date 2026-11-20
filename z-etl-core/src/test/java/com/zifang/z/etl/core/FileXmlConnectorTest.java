package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * XML 连接器验收（{@code file.xml.sink} / {@code file.xml.source}，零依赖 StAX）。
 *
 * <p>口径：sink 流式写（XML 转义交给 writer），source 流式读（重复元素即记录）；
 * 属性 → {@code @name} 列；同名子元素 → List；嵌套子元素 → Map；时间走推断。
 *
 * @author zifang
 */
class FileXmlConnectorTest {

    @TempDir
    Path temp;

    @Test
    @DisplayName("写 XML 再读回来：标量 / 特殊字符 / 时间逐字段一致")
    void writesAndRoundTrips() throws Exception {
        Path xml = temp.resolve("out.xml");
        String write = ""
                + "name: xml-sink\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {updated_at: TIMESTAMP}\n"
                + "      rows:\n"
                + "        - {id: 1, name: 甲, amount: 100.5, updated_at: '2026-01-01 12:00:00'}\n"
                + "        - {id: 2, name: 'A&B<C>', amount: 200, updated_at: '2026-01-02 00:00:00'}\n"
                + "  - id: out\n"
                + "    type: file.xml.sink\n"
                + "    config:\n"
                + "      path: " + xml + "\n"
                + "      columns: [id, name, amount, updated_at]\n"
                + "edges:\n"
                + "  - {from: src, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(write, captured);
        assertEquals(2L, result.getRecordsWritten());

        String text = new String(Files.readAllBytes(xml), StandardCharsets.UTF_8);
        assertTrue(text.contains("<rows>"), "应有根元素: " + text);
        assertTrue(text.contains("A&amp;B&lt;C&gt;"), "特殊字符应转义: " + text);

        String read = ""
                + "name: xml-roundtrip\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: file.xml.source\n"
                + "    config:\n"
                + "      path: " + xml + "\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, Operator> readCapture = new LinkedHashMap<String, Operator>();
        run(read, readCapture);
        List<Map<String, Object>> rows = collectRows(readCapture);
        assertEquals(2, rows.size());
        assertEquals(1L, ((Number) rows.get(0).get("id")).longValue(), "数字走推断应为 LONG: " + rows.get(0));
        assertEquals("甲", rows.get(0).get("name"));
        assertEquals(Timestamp.valueOf("2026-01-01 12:00:00"), rows.get(0).get("updated_at"),
                "时间列应推断还原成 Timestamp: " + rows.get(0));
        assertEquals("A&B<C>", rows.get(1).get("name"), "特殊字符应原样往返");
        assertEquals(Timestamp.valueOf("2026-01-02 00:00:00"), rows.get(1).get("updated_at"));
    }

    @Test
    @DisplayName("外部文件：recordPath 定位 + 属性成 @列 + 嵌套成 Map + 重复子元素成 List")
    void readsForeignXmlShapes() throws Exception {
        Path xml = temp.resolve("foreign.xml");
        Files.write(xml, ( ""
                + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<data>\n"
                + "  <item id='7' level='high'>\n"
                + "    <name>订单甲</name>\n"
                + "    <amount>10.50</amount>\n"
                + "    <detail><city>杭州</city><zip>310000</zip></detail>\n"
                + "    <tag>vip</tag><tag>urgent</tag>\n"
                + "  </item>\n"
                + "  <item id='8'>\n"
                + "    <name>订单乙</name>\n"
                + "    <amount></amount>\n"
                + "  </item>\n"
                + "</data>\n").getBytes(StandardCharsets.UTF_8));
        String yaml = ""
                + "name: xml-foreign\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: file.xml.source\n"
                + "    config:\n"
                + "      path: " + xml + "\n"
                + "      recordPath: data.item\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(2, rows.size());

        Map<String, Object> first = rows.get(0);
        assertEquals(7L, ((Number) first.get("@id")).longValue(), "属性应成 @列: " + first);
        assertEquals("high", first.get("@level"));
        assertEquals("订单甲", first.get("name"));
        assertEquals(0, new java.math.BigDecimal("10.50")
                .compareTo(new java.math.BigDecimal(String.valueOf(first.get("amount")))));
        @SuppressWarnings("unchecked")
        Map<String, Object> detail = (Map<String, Object>) first.get("detail");
        assertNotNull(detail, "嵌套元素应递归成 Map: " + first);
        assertEquals("杭州", detail.get("city"));
        assertEquals(java.util.Arrays.asList("vip", "urgent"), first.get("tag"),
                "重复子元素应合成 List: " + first);

        Map<String, Object> second = rows.get(1);
        assertEquals("订单乙", second.get("name"));
        assertTrue(!second.containsKey("amount") || second.get("amount") == null,
                "空元素应按 null 处理: " + second);
    }

    @Test
    @DisplayName("投影 + limit：只要声明的列，最多产 limit 行")
    void projectionAndLimit() throws Exception {
        Path xml = temp.resolve("many.xml");
        StringBuilder body = new StringBuilder("<rows>");
        for (int i = 1; i <= 10; i++) {
            body.append("<row><id>").append(i).append("</id><name>n").append(i).append("</name></row>");
        }
        body.append("</rows>");
        Files.write(xml, body.toString().getBytes(StandardCharsets.UTF_8));
        String yaml = ""
                + "name: xml-limit\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: file.xml.source\n"
                + "    config:\n"
                + "      path: " + xml + "\n"
                + "      columns: [id]\n"
                + "      limit: 3\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(3, rows.size(), "limit 应截断: " + rows.size());
        assertEquals(1L, ((Number) rows.get(0).get("id")).longValue());
        assertTrue(!rows.get(0).containsKey("name"), "投影应只留声明的列: " + rows.get(0));
    }

    @Test
    @DisplayName("sink 缺 path ⇒ 跑之前就报错；类型别名双向可解析")
    void validationAndAliases() throws Exception {
        assertEquals(OperatorType.FILE_XML_SOURCE, OperatorType.fromId("xml.source"));
        assertEquals(OperatorType.FILE_XML_SOURCE, OperatorType.fromId("file.xml"));
        assertEquals(OperatorType.FILE_XML_SINK, OperatorType.fromId("xml.sink"));
        assertEquals(OperatorType.FILE_XML_SINK, OperatorType.fromId("file.xml.out"));

        String yaml = ""
                + "name: xml-no-path\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "  - id: out\n"
                + "    type: file.xml.sink\n"
                + "    config: {}\n"
                + "edges:\n"
                + "  - {from: src, to: out}\n";
        try {
            run(yaml, new LinkedHashMap<String, Operator>());
            org.junit.jupiter.api.Assertions.fail("缺 path 应该报错");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("path"), "应点名 path，实际: " + expected.getMessage());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured) throws Exception {
        PipelineSpec spec = PipelineParser.parse(yaml);
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend()).execute(spec, captured);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured) {
        Operator operator = captured.get("sink");
        assertNotNull(operator, "应捕获到汇算子");
        return ((SinkOperators.CollectSink) operator).rows();
    }
}
