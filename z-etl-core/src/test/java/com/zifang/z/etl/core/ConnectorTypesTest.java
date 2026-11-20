package com.zifang.z.etl.core;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.connector.JdbcMessageBus;
import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 连接器矩阵验收：**输入类型** 与 **输出类型** 各家都真跑一遍。
 *
 * <p>输入：内联常量 / CSV 文件 / JSON 文件（NDJSON + 数组）/ Excel 文件 / HTTP 接口 / 脚本 / 消息队列。
 * 输出：JDBC / CSV 文件 / JSON 文件 / HTTP 接口 / 消息队列 / 控制台 / 内存收集。
 *
 * <p>⚠️ 全部跑在本地 H2 + 临时目录 + 本地 HTTP 服务上，不连生产库、不发外网请求。
 *
 * @author zifang
 */
class ConnectorTypesTest {

    @TempDir
    Path temp;

    // ——— 输入：内联常量 ———

    @Test
    @DisplayName("输入 inline.source：声明里的静态行直接成流")
    void inlineSourceFeedsStream() throws Exception {
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(
                "inline.source",
                "    config:\n"
                        + "      rows:\n"
                        + "        - {code: A, name: 正常}\n"
                        + "        - {code: B, name: 冻结}\n",
                "collect.sink", ""), captured);
        assertEquals(2L, result.getRecordsRead());
        assertEquals(2L, result.getRecordsWritten());
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(2, rows.size());
        assertEquals("A", rows.get(0).get("code"));
    }

    // ——— 输入：CSV 文件 ———

    @Test
    @DisplayName("输入 file.csv.source：表头 + 类型归一 + 事件时间水位增量接力")
    void csvSourceReadsFileAndResumesByWatermark() throws Exception {
        Path csv = temp.resolve("orders.csv");
        Files.write(csv, ("id,amount,updated_at\n"
                + "1,10.50,2026-01-01 00:00:00\n"
                + "2,20.50,2026-01-01 00:01:00\n").getBytes(StandardCharsets.UTF_8));

        CheckpointStore checkpoints = new InMemoryCheckpointStore();
        String yaml = pipeline(
                "file.csv.source",
                "    config:\n"
                        + "      path: " + csv + "\n"
                        + "      columnTypes: {amount: BIGDECIMAL, updated_at: TIMESTAMP}\n"
                        + "      eventTimeColumn: updated_at\n"
                        + "      eventTimeType: TIMESTAMP\n",
                "collect.sink", "");

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult first = run(yaml, captured, checkpoints);
        assertEquals(2L, first.getRecordsWritten());
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(2, rows.size());
        assertEquals(0, new java.math.BigDecimal("10.50")
                .compareTo((java.math.BigDecimal) rows.get(0).get("amount")), "金额应按声明归一成定点小数");

        // 追加一行 ⇒ 第二轮只搬新增行（事件时间游标接力）
        Files.write(csv, ("id,amount,updated_at\n"
                + "1,10.50,2026-01-01 00:00:00\n"
                + "2,20.50,2026-01-01 00:01:00\n"
                + "3,30.50,2026-01-01 00:02:00\n").getBytes(StandardCharsets.UTF_8));
        Map<String, Operator> second = new LinkedHashMap<String, Operator>();
        PipelineRunResult incremental = run(yaml, second, checkpoints);
        assertEquals(1L, incremental.getRecordsRead(), "第二轮只应读到新增行");
        assertEquals(1L, incremental.getRecordsWritten());
    }

    // ——— 输入：JSON 文件 ———

    @Test
    @DisplayName("输入 file.json.source：NDJSON 与 JSON 数组（带 itemPath）都读")
    void jsonSourceReadsNdjsonAndArray() throws Exception {
        Path ndjson = temp.resolve("events.ndjson");
        Files.write(ndjson, ("{\"id\":1,\"amount\":10.5}\n"
                + "{\"id\":2,\"amount\":20.5}\n").getBytes(StandardCharsets.UTF_8));
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult ndjsonResult = run(pipeline(
                "file.json.source",
                "    config:\n"
                        + "      path: " + ndjson + "\n"
                        + "      format: ndjson\n",
                "collect.sink", ""), captured);
        assertEquals(2L, ndjsonResult.getRecordsWritten());

        Path array = temp.resolve("events.json");
        Files.write(array, ("{\"data\":{\"items\":[{\"id\":7,\"name\":\"甲\"}]}}")
                .getBytes(StandardCharsets.UTF_8));
        Map<String, Operator> arrayCaptured = new LinkedHashMap<String, Operator>();
        PipelineRunResult arrayResult = run(pipeline(
                "file.json.source",
                "    config:\n"
                        + "      path: " + array + "\n"
                        + "      format: json\n"
                        + "      itemPath: data.items\n",
                "collect.sink", ""), arrayCaptured);
        assertEquals(1L, arrayResult.getRecordsWritten());
        assertEquals("甲", collectRows(arrayCaptured).get(0).get("name"));
    }

    // ——— 输入：Excel 文件 ———

    @Test
    @DisplayName("输入 file.xlsx.source：零依赖读第一张工作表")
    void xlsxSourceReadsWorkbook() throws Exception {
        Path xlsx = temp.resolve("book.xlsx");
        writeXlsx(xlsx, new String[][]{
                {"id", "name", "amount"},
                {"1", "甲", "10.5"},
                {"2", "乙", "20.5"}});
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(
                "file.xlsx.source",
                "    config:\n"
                        + "      path: " + xlsx + "\n"
                        + "      columnTypes: {amount: BIGDECIMAL}\n",
                "collect.sink", ""), captured);
        assertEquals(2L, result.getRecordsWritten());
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals("甲", rows.get(0).get("name"));
        assertEquals(0, new java.math.BigDecimal("10.5")
                .compareTo((java.math.BigDecimal) rows.get(0).get("amount")));
    }

    // ——— 输入：HTTP 接口 ———

    @Test
    @DisplayName("输入 http.source：本地接口拉 JSON 数组（带 itemPath）")
    void httpSourceFetchesJson() throws Exception {
        final List<String> requests = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/orders", new com.sun.net.httpserver.HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws java.io.IOException {
                requests.add(exchange.getRequestURI().toString());
                byte[] body = ("{\"data\":{\"items\":[{\"id\":1,\"name\":\"甲\"},{\"id\":2,\"name\":\"乙\"}]}}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                OutputStream out = exchange.getResponseBody();
                out.write(body);
                out.close();
            }
        });
        server.start();
        try {
            Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
            PipelineRunResult result = run(pipeline(
                    "http.source",
                    "    config:\n"
                            + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/orders\n"
                            + "      itemPath: data.items\n"
                            + "      allowedHosts: [127.0.0.1]\n",
                    "collect.sink", ""), captured);
            assertEquals(2L, result.getRecordsWritten());
            assertEquals(1, requests.size());
            assertEquals("甲", collectRows(captured).get(0).get("name"));
        } finally {
            server.stop(0);
        }
    }

    // ——— 输入：脚本 ———

    @Test
    @DisplayName("输入 script.source：沙箱脚本生成数据")
    void scriptSourceGeneratesRows() throws Exception {
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(
                "script.source",
                "    config:\n"
                        + "      engine: GROOVY\n"
                        + "      script: \"[[id: 1, name: '甲'], [id: 2, name: '乙']]\"\n",
                "collect.sink", ""), captured);
        assertEquals(2L, result.getRecordsWritten());
        assertEquals("甲", collectRows(captured).get(0).get("name"));
    }

    // ——— 输入：消息队列（库表队列） ———

    @Test
    @DisplayName("输入 message.source：发消息 → 收消息，一条流走通")
    void messageSourceConsumesPublishedMessages() throws Exception {
        DataSource meta = h2("connector_msg_" + System.nanoTime());
        JdbcMessageBus.ensure(meta);
        MessageBus bus = new JdbcMessageBus(meta);

        run(pipeline(
                "inline.source",
                "    config:\n"
                        + "      rows:\n"
                        + "        - {id: 1, name: 甲}\n"
                        + "        - {id: 2, name: 乙}\n",
                "message.sink",
                "    config:\n"
                        + "      topic: orders\n"
                        + "      keyColumn: id\n"),
                new LinkedHashMap<String, Operator>(), new InMemoryCheckpointStore(), bus);

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult consumed = run(pipeline(
                "message.source",
                "    config:\n"
                        + "      topic: orders\n",
                "collect.sink", ""), captured, new InMemoryCheckpointStore(), bus);
        assertEquals(2L, consumed.getRecordsWritten());
        assertEquals("甲", collectRows(captured).get(0).get("name"));
    }

    // ——— 输出：CSV 文件 ———

    @Test
    @DisplayName("输出 file.csv.sink：写 CSV 文件（含表头与列顺序）")
    void csvSinkWritesFile() throws Exception {
        Path out = temp.resolve("out.csv");
        PipelineRunResult result = run(pipeline(
                "inline.source",
                "    config:\n"
                        + "      rows:\n"
                        + "        - {id: 1, name: 甲}\n"
                        + "        - {id: 2, name: 乙}\n",
                "file.csv.sink",
                "    config:\n"
                        + "      path: " + out + "\n"
                        + "      columns: [id, name]\n"),
                new LinkedHashMap<String, Operator>());
        assertEquals(2L, result.getRecordsWritten());
        String content = new String(Files.readAllBytes(out), StandardCharsets.UTF_8);
        assertTrue(content.startsWith("id,name"), "首行应是表头，实际: " + content);
        assertTrue(content.contains("1,甲"), "应有数据行，实际: " + content);
    }

    // ——— 输出：JSON 文件 ———

    @Test
    @DisplayName("输出 file.json.sink：NDJSON 与 JSON 数组两种形态")
    void jsonSinkWritesFile() throws Exception {
        Path ndjson = temp.resolve("out.ndjson");
        run(pipeline(
                "inline.source",
                "    config:\n"
                        + "      rows:\n"
                        + "        - {id: 1, name: 甲}\n",
                "file.json.sink",
                "    config:\n"
                        + "      path: " + ndjson + "\n"
                        + "      format: ndjson\n"),
                new LinkedHashMap<String, Operator>());
        String ndjsonText = new String(Files.readAllBytes(ndjson), StandardCharsets.UTF_8);
        assertTrue(ndjsonText.trim().startsWith("{"), "NDJSON 每行一个 JSON 对象，实际: " + ndjsonText);
        assertTrue(ndjsonText.contains("\"name\":\"甲\""), "实际: " + ndjsonText);

        Path array = temp.resolve("out.json");
        run(pipeline(
                "inline.source",
                "    config:\n"
                        + "      rows:\n"
                        + "        - {id: 1, name: 甲}\n",
                "file.json.sink",
                "    config:\n"
                        + "      path: " + array + "\n"
                        + "      format: json\n"),
                new LinkedHashMap<String, Operator>());
        String arrayText = new String(Files.readAllBytes(array), StandardCharsets.UTF_8);
        assertTrue(arrayText.trim().startsWith("["), "JSON 形态应是数组，实际: " + arrayText);
    }

    // ——— 输出：HTTP 接口 ———

    @Test
    @DisplayName("输出 http.sink：POST 到本地接口")
    void httpSinkPostsRows() throws Exception {
        final List<String> payloads = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ingest", new com.sun.net.httpserver.HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws java.io.IOException {
                java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
                byte[] chunk = new byte[1024];
                int read;
                while ((read = exchange.getRequestBody().read(chunk)) > 0) {
                    buffer.write(chunk, 0, read);
                }
                payloads.add(new String(buffer.toByteArray(), StandardCharsets.UTF_8));
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            }
        });
        server.start();
        try {
            PipelineRunResult result = run(pipeline(
                    "inline.source",
                    "    config:\n"
                            + "      rows:\n"
                            + "        - {id: 1, name: 甲}\n",
                    "http.sink",
                    "    config:\n"
                            + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/ingest\n"),
                    new LinkedHashMap<String, Operator>());
            assertEquals(1L, result.getRecordsWritten());
            assertEquals(1, payloads.size());
            assertTrue(payloads.get(0).contains("\"name\":\"甲\""), "实际: " + payloads.get(0));
        } finally {
            server.stop(0);
        }
    }

    // ——— 输入：Excel 日期列 ———

    @Test
    @DisplayName("输入 file.xlsx.source：日期样式（numFmtId 14）还原成 Timestamp")
    void xlsxSourceParsesDateColumns() throws Exception {
        Path xlsx = temp.resolve("dated.xlsx");
        writeXlsxWithStyles(xlsx, new String[][]{
                {"id", "updated_at"},
                {"1", "46023.5"},
                {"2", "46024"}});
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(
                "file.xlsx.source",
                "    config:\n"
                        + "      path: " + xlsx + "\n"
                        + "      columnTypes: {updated_at: TIMESTAMP}\n",
                "collect.sink", ""), captured);
        assertEquals(2L, result.getRecordsWritten());
        List<Map<String, Object>> rows = collectRows(captured);
        Object first = rows.get(0).get("updated_at");
        assertTrue(first instanceof java.sql.Timestamp, "日期单元格应还原成 Timestamp，实际 "
                + (first == null ? "null" : first.getClass().getName()));
        assertEquals(java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), first,
                "Excel 序列数 46023.5 = 2026-01-01 12:00（基日 1899-12-30）");
    }

    // ——— 输入：CSV 无表头 + 跳行 + 限量 ———

    @Test
    @DisplayName("输入 file.csv.source：header=false + 声明列名 + skipRows + limit")
    void csvSourceHeaderlessWithSkipAndLimit() throws Exception {
        Path csv = temp.resolve("raw.csv");
        Files.write(csv, ("这不是数据的前导行\n"
                + "这也不是\n"
                + "1,甲\n"
                + "2,乙\n"
                + "3,丙\n").getBytes(StandardCharsets.UTF_8));
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(pipeline(
                "file.csv.source",
                "    config:\n"
                        + "      path: " + csv + "\n"
                        + "      header: false\n"
                        + "      columns: [id, name]\n"
                        + "      skipRows: 2\n"
                        + "      limit: 2\n",
                "collect.sink", ""), captured);
        assertEquals(2L, result.getRecordsRead(), "skipRows 跳过前导行，limit 限量 2 行");
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals("甲", rows.get(0).get("name"));
        assertEquals("乙", rows.get(1).get("name"));
    }

    // ——— 输入：HTTP 分页 ———

    @Test
    @DisplayName("输入 http.source：pageParam 翻页拉全量（空页停）")
    void httpSourcePagesThroughResults() throws Exception {
        final List<String> requests = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/orders", new com.sun.net.httpserver.HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws java.io.IOException {
                requests.add(exchange.getRequestURI().toString());
                int page = 1;
                String query = exchange.getRequestURI().getQuery();
                if (query != null && query.contains("page=")) {
                    page = Integer.parseInt(query.replaceAll(".*page=(\\d+).*", "$1"));
                }
                StringBuilder items = new StringBuilder();
                if (page <= 2) {
                    for (int i = 0; i < 2; i++) {
                        if (items.length() > 0) {
                            items.append(',');
                        }
                        int id = (page - 1) * 2 + i + 1;
                        items.append("{\"id\":").append(id).append("}");
                    }
                }
                byte[] body = ("{\"items\":[" + items + "]}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                OutputStream out = exchange.getResponseBody();
                out.write(body);
                out.close();
            }
        });
        server.start();
        try {
            Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
            PipelineRunResult result = run(pipeline(
                    "http.source",
                    "    config:\n"
                            + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/orders\n"
                            + "      itemPath: items\n"
                            + "      pageParam: page\n"
                            + "      pageSize: 2\n"
                            + "      maxPages: 5\n"
                            + "      allowedHosts: [127.0.0.1]\n",
                    "collect.sink", ""), captured);
            assertEquals(4L, result.getRecordsWritten(), "两页各 2 条，第 3 页空 ⇒ 停");
            assertEquals(3, requests.size(), "应请求 3 次（第 3 次拿到空数组）");
            assertEquals("page=1&pageSize=2", requests.get(0).split("\\?")[1], "页码参数应拼在 URL 上");
        } finally {
            server.stop(0);
        }
    }

    // ——— 多源：全局水位 = 各活跃源最小值 ———

    @Test
    @DisplayName("多源水位：慢源拖住全局水位，窗口不提前结算 ⇒ 不丢迟到行")
    void multiSourceGlobalWatermarkIsMinOfActive() throws Exception {
        Path a = temp.resolve("a.csv");
        Files.write(a, ("id,src,ts\n"
                + "1,A,2026-01-01 00:00:00\n"
                + "2,A,2026-01-01 00:01:00\n"
                + "3,A,2026-01-01 00:02:00\n").getBytes(StandardCharsets.UTF_8));
        Path b = temp.resolve("b.csv");
        Files.write(b, ("id,src,ts\n"
                + "11,B,2026-01-01 00:00:00\n"
                + "12,B,2026-01-01 00:01:00\n"
                + "13,B,2026-01-01 00:02:00\n").getBytes(StandardCharsets.UTF_8));

        String yaml = ""
                + "name: multi-source-watermark\n"
                + "nodes:\n"
                + "  - id: a\n"
                + "    type: file.csv.source\n"
                + "    config:\n"
                + "      path: " + a + "\n"
                + "      columnTypes: {ts: TIMESTAMP}\n"
                + "      eventTimeColumn: ts\n"
                + "      eventTimeType: TIMESTAMP\n"
                + "      maxOutOfOrderness: 0s\n"
                + "  - id: b\n"
                + "    type: file.csv.source\n"
                + "    config:\n"
                + "      path: " + b + "\n"
                + "      columnTypes: {ts: TIMESTAMP}\n"
                + "      eventTimeColumn: ts\n"
                + "      eventTimeType: TIMESTAMP\n"
                + "      maxOutOfOrderness: 0s\n"
                + "  - id: wagg\n"
                + "    type: window.aggregate\n"
                + "    config:\n"
                + "      keyBy: [src]\n"
                + "      window: {type: tumbling, size: 1m}\n"
                + "      aggregates:\n"
                + "        - {fn: COUNT, column: \"*\", alias: cnt}\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: a, to: wagg}\n"
                + "  - {from: b, to: wagg}\n"
                + "  - {from: wagg, to: sink}\n";

        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        assertEquals(6L, result.getRecordsRead());

        // 两个源各 3 个 1 分钟窗口 ⇒ 6 个窗口行；若水位只按单源算，
        // A 先进数据会提前结算 [00:00,00:01) 窗口，B 的 00:00 行就被当迟到丢掉（只剩 5 行）
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(6, rows.size(), "A/B 各 3 个窗口，一行不丢: " + rows);
        Map<String, Long> byKeyAndWindow = new LinkedHashMap<String, Long>();
        for (Map<String, Object> row : rows) {
            byKeyAndWindow.put(row.get("src") + "@" + String.valueOf(row.get("windowStart")),
                    ((Number) row.get("cnt")).longValue());
        }
        for (String key : new String[]{"A", "B"}) {
            assertEquals(3, countWindows(byKeyAndWindow, key), key + " 应有 3 个窗口");
            for (Map.Entry<String, Long> entry : byKeyAndWindow.entrySet()) {
                if (entry.getKey().startsWith(key + "@")) {
                    assertEquals(Long.valueOf(1L), entry.getValue(),
                            "每窗口 1 行: " + entry.getKey() + " = " + entry.getValue());
                }
            }
        }
    }

    private int countWindows(Map<String, Long> byKeyAndWindow, String key) {
        int count = 0;
        for (String windowKey : byKeyAndWindow.keySet()) {
            if (windowKey.startsWith(key + "@")) {
                count++;
            }
        }
        return count;
    }

    // ——— 类型别名 ———

    @Test
    @DisplayName("类型别名：csv.source / kafka.sink / webhook.sink 都能解析")
    void typeAliasesResolve() {
        assertEquals(OperatorType.FILE_CSV_SOURCE, OperatorType.fromId("csv.source"));
        assertEquals(OperatorType.FILE_JSON_SOURCE, OperatorType.fromId("ndjson.source"));
        assertEquals(OperatorType.FILE_XLSX_SOURCE, OperatorType.fromId("excel.source"));
        assertEquals(OperatorType.HTTP_SOURCE, OperatorType.fromId("rest.source"));
        assertEquals(OperatorType.MESSAGE_SINK, OperatorType.fromId("kafka.sink"));
        assertEquals(OperatorType.HTTP_SINK, OperatorType.fromId("webhook.sink"));
        assertEquals(OperatorType.FILE_CSV_SINK, OperatorType.fromId("csv.sink"));
    }

    // ——— 内部工具 ———

    /** 拼一条「源 → 汇」的流水线声明。 */
    private String pipeline(String sourceType, String sourceConfig, String sinkType, String sinkConfig) {
        return "name: connector-demo\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: " + sourceType + "\n"
                + sourceConfig
                + "  - id: sink\n"
                + "    type: " + sinkType + "\n"
                + sinkConfig
                + "edges:\n"
                + "  - {from: src, to: sink}\n";
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured) throws Exception {
        return run(yaml, captured, new InMemoryCheckpointStore());
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured,
                                  CheckpointStore checkpoints) throws Exception {
        return run(yaml, captured, checkpoints, null);
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured,
                                  CheckpointStore checkpoints, MessageBus bus) throws Exception {
        PipelineSpec spec = PipelineParser.parse(yaml);
        PipelineExecutor executor = new PipelineExecutor(new MapDataSourceProvider(),
                new HeapStateBackend(), new com.zifang.z.etl.core.transform.ScriptSandbox(),
                checkpoints, null, bus);
        return executor.execute(spec, captured);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured) {
        Operator operator = captured.get("sink");
        assertNotNull(operator, "应捕获到汇算子");
        return ((SinkOperators.CollectSink) operator).rows();
    }

    private static DataSource h2(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    /** 造一个最小可读的 xlsx（zip + worksheet XML，内联字符串）。 */
    private void writeXlsx(Path target, String[][] rows) throws Exception {
        StringBuilder sheet = new StringBuilder();
        sheet.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
                .append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">")
                .append("<sheetData>");
        for (int r = 0; r < rows.length; r++) {
            sheet.append("<row r=\"").append(r + 1).append("\">");
            for (int c = 0; c < rows[r].length; c++) {
                String reference = columnRef(c) + (r + 1);
                String value = rows[r][c];
                if (isNumeric(value)) {
                    sheet.append("<c r=\"").append(reference).append("\"><v>").append(value).append("</v></c>");
                } else {
                    sheet.append("<c r=\"").append(reference).append("\" t=\"inlineStr\"><is><t>")
                            .append(value).append("</t></is></c>");
                }
            }
            sheet.append("</row>");
        }
        sheet.append("</sheetData></worksheet>");
        ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(target));
        try {
            zos.putNextEntry(new ZipEntry("xl/worksheets/sheet1.xml"));
            zos.write(sheet.toString().getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        } finally {
            zos.close();
        }
    }

    private String columnRef(int index) {
        StringBuilder sb = new StringBuilder();
        int value = index + 1;
        while (value > 0) {
            int rest = (value - 1) % 26;
            sb.insert(0, (char) ('A' + rest));
            value = (value - 1) / 26;
        }
        return sb.toString();
    }

    private boolean isNumeric(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (!Character.isDigit(ch) && ch != '.' && ch != '-') {
                return false;
            }
        }
        return true;
    }

    /** 造一个带日期样式的最小 xlsx：styles.xml 里 cellXfs[1] 挂内置日期格式 numFmtId=14。 */
    private void writeXlsxWithStyles(Path target, String[][] rows) throws Exception {
        StringBuilder sheet = new StringBuilder();
        sheet.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
                .append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">")
                .append("<sheetData>");
        for (int r = 0; r < rows.length; r++) {
            sheet.append("<row r=\"").append(r + 1).append("\">");
            for (int c = 0; c < rows[r].length; c++) {
                String reference = columnRef(c) + (r + 1);
                String value = rows[r][c];
                if (r == 0) {
                    sheet.append("<c r=\"").append(reference).append("\" t=\"inlineStr\"><is><t>")
                            .append(value).append("</t></is></c>");
                } else if (c == 1) {
                    // 日期列：挂样式 1（numFmtId=14 内置日期格式）
                    sheet.append("<c r=\"").append(reference).append("\" s=\"1\"><v>")
                            .append(value).append("</v></c>");
                } else {
                    sheet.append("<c r=\"").append(reference).append("\" s=\"0\"><v>")
                            .append(value).append("</v></c>");
                }
            }
            sheet.append("</row>");
        }
        sheet.append("</sheetData></worksheet>");
        String styles = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
                + "<cellXfs count=\"2\">"
                + "<xf numFmtId=\"0\"/>"
                + "<xf numFmtId=\"14\" applyNumberFormat=\"1\"/>"
                + "</cellXfs></styleSheet>";
        ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(target));
        try {
            zos.putNextEntry(new ZipEntry("xl/worksheets/sheet1.xml"));
            zos.write(sheet.toString().getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("xl/styles.xml"));
            zos.write(styles.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        } finally {
            zos.close();
        }
    }
}
