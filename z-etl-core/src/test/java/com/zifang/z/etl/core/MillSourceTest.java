package com.zifang.z.etl.core;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 资料磨坊源验收：z-mill-web 打通（文件 multipart 上传 / 网页抓取），chunk / doc 两种产出模式。
 *
 * <p>桩服务内嵌 {@link HttpServer}，按 z-mill-web 真实契约回包：
 * {@code {"results":[{name,status,format,stats,outputs,chunks}]}}。
 *
 * @author zifang
 */
class MillSourceTest {

    private static HttpServer mill;
    private static int port;
    /** 桩收到的 multipart 请求体（验证文件确实上传了）。 */
    private static final List<String> GRIND_BODIES = new CopyOnWriteArrayList<String>();

    @BeforeAll
    static void startStub() throws IOException {
        mill = HttpServer.create(new InetSocketAddress(0), 0);
        port = mill.getAddress().getPort();
        mill.createContext("/api/grind", exchange -> {
            GRIND_BODIES.add(read(exchange));
            respond(exchange, "{\"results\":[{\"name\":\"报告.docx\",\"status\":\"ok\",\"format\":\"DOCX\","
                    + "\"stats\":{\"blocks\":12,\"chars\":3456},"
                    + "\"outputs\":{\"md\":\"# 报告\",\"json\":\"{}\",\"txt\":\"报告\",\"html\":\"<h1>报告</h1>\"},"
                    + "\"chunks\":[{\"heading\":\"第一章\",\"path\":[\"报告\",\"第一章\"],\"text\":\"第一章正文\"},"
                    + "{\"heading\":\"第二章\",\"path\":[\"报告\",\"第二章\"],\"text\":\"第二章正文\"}]}]}");
        });
        mill.createContext("/api/grind-url", exchange -> {
            String body = read(exchange);
            if (!body.contains("https://ok.example.com")) {
                respond(exchange, "{\"error\":\"只支持 http/https 网页地址\"}");
                return;
            }
            respond(exchange, "{\"results\":[{\"name\":\"https://ok.example.com/a\",\"status\":\"ok\",\"format\":\"HTML\","
                    + "\"stats\":{\"blocks\":3,\"chars\":500},"
                    + "\"outputs\":{\"md\":\"# 页面\",\"json\":\"{}\",\"txt\":\"页面\",\"html\":\"<h1>页面</h1>\"},"
                    + "\"chunks\":[{\"heading\":\"\",\"path\":[],\"text\":\"页面全文\"}]}]}");
        });
        mill.start();
    }

    @AfterAll
    static void stopStub() {
        mill.stop(0);
    }

    @Test
    @DisplayName("文件上传 + chunk 模式：一块一行，path 拼接，metrics 计数")
    void fileChunkMode() throws Exception {
        Path doc = Files.createTempFile("zetl-mill-", ".docx");
        Files.write(doc, "fake-docx-bytes".getBytes(StandardCharsets.UTF_8));
        PipelineRunResult result = run(""
                + "name: mill-file-chunk\n"
                + "nodes:\n"
                + "  - id: docs\n"
                + "    type: mill.source\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "\n"
                + "      file: " + doc + "\n"
                + "      mode: chunk\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: docs, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(2, rows.size(), "两个 chunk 两行");
        assertEquals("报告.docx", rows.get(0).get("source"));
        assertEquals(Integer.valueOf(0), rows.get(0).get("index"));
        assertEquals("第一章", rows.get(0).get("heading"));
        assertEquals("报告 / 第一章", rows.get(0).get("path"), "标题路径用 ' / ' 拼接");
        assertEquals("第一章正文", rows.get(0).get("text"));
        assertEquals("报告 / 第二章", rows.get(1).get("path"));
        assertEquals(Long.valueOf(2L), metrics(result).get("records-out"));
        assertTrue(!GRIND_BODIES.isEmpty() && GRIND_BODIES.get(0).contains("fake-docx-bytes"),
                "文件内容确实经 multipart 上传给了 z-mill");
        Files.deleteIfExists(doc);
    }

    @Test
    @DisplayName("网页抓取 + doc 模式：一文档一行，四形态成品齐")
    void urlDocMode() throws Exception {
        PipelineRunResult result = run(""
                + "name: mill-url-doc\n"
                + "nodes:\n"
                + "  - id: docs\n"
                + "    type: mill.source\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "\n"
                + "      url: https://ok.example.com/a\n"
                + "      mode: doc\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: docs, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "doc 模式一文档一行");
        assertEquals("https://ok.example.com/a", rows.get(0).get("source"));
        assertEquals("HTML", rows.get(0).get("format"));
        assertEquals(Integer.valueOf(3), rows.get(0).get("blocks"));
        assertEquals("# 页面", rows.get(0).get("markdown"));
        assertEquals("<h1>页面</h1>", rows.get(0).get("html"));
        assertEquals(Long.valueOf(1L), metrics(result).get("records-out"));
    }

    @Test
    @DisplayName("研磨失败（unsupported）⇒ 跑之前报错不静默吞")
    void failureSurfaces() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> run(""
                + "name: mill-broken\n"
                + "nodes:\n"
                + "  - id: docs\n"
                + "    type: mill.source\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "\n"
                + "      url: https://bad.example.com/x\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: docs, to: out}\n"));
        assertTrue(error.getMessage().contains("z-mill"), error.getMessage());
    }

    @Test
    @DisplayName("file 与 url 必须二选一；缺 baseUrl / mode 拼错 ⇒ 声明期就报错")
    void validation() {
        IllegalArgumentException both = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: mill-both\n"
                + "nodes:\n"
                + "  - id: docs\n"
                + "    type: mill.source\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "\n"
                + "      file: /tmp/a.docx\n"
                + "      url: https://example.com\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: docs, to: out}\n"));
        assertTrue(both.getMessage().contains("二选一"), both.getMessage());

        IllegalArgumentException noBase = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: mill-nobase\n"
                + "nodes:\n"
                + "  - id: docs\n"
                + "    type: mill.source\n"
                + "    config:\n"
                + "      file: /tmp/a.docx\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: docs, to: out}\n"));
        assertTrue(noBase.getMessage().contains("baseUrl"), noBase.getMessage());

        IllegalArgumentException badMode = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: mill-badmode\n"
                + "nodes:\n"
                + "  - id: docs\n"
                + "    type: mill.source\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "\n"
                + "      file: /tmp/a.docx\n"
                + "      mode: rows\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: docs, to: out}\n"));
        assertTrue(badMode.getMessage().contains("chunk|doc"), badMode.getMessage());
    }

    private static String read(HttpExchange exchange) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        try (java.io.InputStream in = exchange.getRequestBody()) {
            int n;
            while ((n = in.read(chunk)) > 0) {
                buffer.write(chunk, 0, n);
            }
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
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

    private Map<String, Long> metrics(PipelineRunResult result) {
        return result.getNodeMetrics().get("docs");
    }
}
