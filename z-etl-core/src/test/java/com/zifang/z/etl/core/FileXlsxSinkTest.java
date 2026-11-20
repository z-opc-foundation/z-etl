package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
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

import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Excel 落地验收（{@code file.xlsx.sink}，零依赖写 xlsx）。
 *
 * <p>口径：数字 → 数值单元格；时间 → Excel 序列数 + 日期样式（numFmtId 14）⇒
 * 用 {@code file.xlsx.source} 读回来还是 Timestamp（真往返）；字符串 XML 转义。
 *
 * @author zifang
 */
class FileXlsxSinkTest {

    @TempDir
    Path temp;

    @Test
    @DisplayName("写 xlsx：表头 + 数值/文本/时间列，再用 xlsx 源读回来逐字段一致")
    void writesAndRoundTrips() throws Exception {
        Path xlsx = temp.resolve("out.xlsx");
        String write = ""
                + "name: xlsx-sink\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      columnTypes: {updated_at: TIMESTAMP}\n"
                + "      rows:\n"
                + "        - {id: 1, name: 甲, amount: 100.5, updated_at: '2026-01-01 12:00:00'}\n"
                + "        - {id: 2, name: 'A&B<C>', amount: 200, updated_at: '2026-01-02 00:00:00'}\n"
                + "  - id: out\n"
                + "    type: file.xlsx.sink\n"
                + "    config:\n"
                + "      path: " + xlsx + "\n"
                + "      columns: [id, name, amount, updated_at]\n"
                + "edges:\n"
                + "  - {from: src, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(write, captured);
        assertEquals(2L, result.getRecordsWritten());

        String read = ""
                + "name: xlsx-roundtrip\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: file.xlsx.source\n"
                + "    config:\n"
                + "      path: " + xlsx + "\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, Operator> readCapture = new LinkedHashMap<String, Operator>();
        run(read, readCapture);
        // 默认 header=true ⇒ 第一行是列名，读到的是 2 条数据行；时间列靠样式（numFmtId 14）还原成 Timestamp
        List<Map<String, Object>> rows = collectRows(readCapture);
        assertEquals(2, rows.size());
        assertEquals(Integer.valueOf(1), ((Number) rows.get(0).get("id")).intValue());
        assertEquals("甲", rows.get(0).get("name"));
        assertEquals(0, new java.math.BigDecimal("100.5")
                .compareTo(new java.math.BigDecimal(String.valueOf(rows.get(0).get("amount")))),
                "数值列应往返一致: " + rows.get(0));
        assertEquals(Timestamp.valueOf("2026-01-01 12:00:00"), rows.get(0).get("updated_at"),
                "时间列应写回序列数并还原成 Timestamp: " + rows.get(0));
        assertEquals("A&B<C>", rows.get(1).get("name"), "特殊字符应转义后原样往返");
        assertEquals(Timestamp.valueOf("2026-01-02 00:00:00"), rows.get(1).get("updated_at"));
    }

    @Test
    @DisplayName("header=false + 未声明列 ⇒ 按首行键序写数据行")
    void headerlessWritesDataRows() throws Exception {
        Path xlsx = temp.resolve("no-header.xlsx");
        String yaml = ""
                + "name: xlsx-no-header\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {code: A, name: 正常}\n"
                + "  - id: out\n"
                + "    type: file.xlsx.sink\n"
                + "    config:\n"
                + "      path: " + xlsx + "\n"
                + "      header: false\n"
                + "edges:\n"
                + "  - {from: src, to: out}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        run(yaml, captured);

        String read = ""
                + "name: xlsx-no-header-read\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: file.xlsx.source\n"
                + "    config:\n"
                + "      path: " + xlsx + "\n"
                + "      header: false\n"
                + "      columns: [code, name]\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
        Map<String, Operator> readCapture = new LinkedHashMap<String, Operator>();
        run(read, readCapture);
        List<Map<String, Object>> rows = collectRows(readCapture);
        assertEquals(1, rows.size(), "没有表头行 ⇒ 读回来就是数据行");
        assertEquals("A", rows.get(0).get("code"));
    }

    @Test
    @DisplayName("缺 path ⇒ 跑之前就报错")
    void requiresPath() {
        String yaml = ""
                + "name: xlsx-no-path\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1}\n"
                + "  - id: out\n"
                + "    type: file.xlsx.sink\n"
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
