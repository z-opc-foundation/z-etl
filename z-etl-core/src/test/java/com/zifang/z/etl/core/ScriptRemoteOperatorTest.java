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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 平台脚本算子验收：z-script 平台打通（app + AK，整行当参数，column / row 两种产出）。
 *
 * <p>桩服务内嵌 {@link HttpServer}，按 z-script 平台真实契约回包：
 * {@code {success, data, errorMessage, durationMs}}；鉴权 {@code X-Api-Key}。
 *
 * @author zifang
 */
class ScriptRemoteOperatorTest {

    private static HttpServer platform;
    private static int port;
    /** 桩收到的调用（验请求形状：路径、AK 头、body=整行）。 */
    private static final List<String[]> CALLS = new CopyOnWriteArrayList<String[]>();

    @BeforeAll
    static void startStub() throws IOException {
        platform = HttpServer.create(new InetSocketAddress(0), 0);
        port = platform.getAddress().getPort();
        platform.createContext("/script/api/script/run", exchange -> {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            try (java.io.InputStream in = exchange.getRequestBody()) {
                int n;
                while ((n = in.read(chunk)) > 0) {
                    buffer.write(chunk, 0, n);
                }
            }
            String body = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
            CALLS.add(new String[]{String.valueOf(exchange.getRequestURI()), 
                    String.valueOf(exchange.getRequestHeaders().getFirst("X-Api-Key")), body});
            if ("sk-good".equals(exchange.getRequestHeaders().getFirst("X-Api-Key"))) {
                if (body.contains("\"row_b\"")) {
                    // row 模式脚本：回一个对象
                    respond(exchange, 200, "{\"success\":true,\"data\":{\"id\":1,\"pay_channel\":\"ALIPAY\"},\"errorMessage\":null,\"durationMs\":3}");
                } else {
                    respond(exchange, 200, "{\"success\":true,\"data\":\"ALIPAY\",\"errorMessage\":null,\"durationMs\":3}");
                }
            } else {
                respond(exchange, 200, "{\"success\":false,\"data\":null,\"errorMessage\":\"AK 无效或配额超限\",\"durationMs\":1}");
            }
        });
        platform.start();
    }

    @AfterAll
    static void stopStub() {
        platform.stop(0);
    }

    @Test
    @DisplayName("column 模式：整行当 params，data 落声明列；请求带 X-Api-Key")
    void columnMode() throws Exception {
        PipelineRunResult result = run(""
                + "name: remote-column\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, raw: ' alipay '}\n"
                + "  - id: enrich\n"
                + "    type: script.remote\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "/script\n"
                + "      scriptCode: clean-order\n"
                + "      apiKey: sk-good\n"
                + "      name: pay_channel\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size());
        assertEquals("ALIPAY", rows.get(0).get("pay_channel"), "平台脚本返回值落声明列");
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"), "原行其余列保留");
        assertEquals(Long.valueOf(1L), metrics(result).get("records-out"));
        String[] call = CALLS.get(CALLS.size() - 1);
        assertTrue(call[0].contains("/api/script/run?scriptCode=clean-order"), call[0]);
        assertEquals("sk-good", call[1], "AK 走 X-Api-Key 头");
        assertTrue(call[2].contains("\"raw\"") && call[2].contains("alipay"), "body = 整行 JSON 当 params");
    }

    @Test
    @DisplayName("row 模式：脚本返回对象整行替换")
    void rowMode() throws Exception {
        PipelineRunResult result = run(""
                + "name: remote-row\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, row_b: true}\n"
                + "  - id: enrich\n"
                + "    type: script.remote\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "/script\n"
                + "      scriptCode: reshape-order\n"
                + "      apiKey: sk-good\n"
                + "      mode: row\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("ALIPAY", rows.get(0).get("pay_channel"));
        assertEquals(Integer.valueOf(1), rows.get(0).get("id"));
        assertNull(rows.get(0).get("row_b"), "整行替换后原列消失");
    }

    @Test
    @DisplayName("success=false ⇒ 抛错不静默吞（带平台给的 errorMessage）")
    void failureSurfaces() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> run(""
                + "name: remote-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 1}]\n"
                + "  - id: enrich\n"
                + "    type: script.remote\n"
                + "    config:\n"
                + "      baseUrl: http://127.0.0.1:" + port + "/script\n"
                + "      scriptCode: clean-order\n"
                + "      apiKey: sk-bad\n"
                + "      name: pay_channel\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n"));
        assertTrue(error.getMessage().contains("AK 无效"), error.getMessage());
    }

    @Test
    @DisplayName("校验：缺 baseUrl / scriptCode、mode=row 带 name、mode=column 缺 name ⇒ 声明期报错")
    void validation() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> run(pipeline("scriptCode: x", "      name: c")))
                .getMessage().contains("baseUrl"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> run(pipeline("baseUrl: http://x", "      name: c")))
                .getMessage().contains("scriptCode"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> run(pipeline("baseUrl: http://x\n      scriptCode: x\n      mode: row", "      name: c")))
                .getMessage().contains("mode=row"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> run(pipeline("baseUrl: http://x\n      scriptCode: x", null)))
                .getMessage().contains("mode=column"));
    }

    private String pipeline(String configLines, String nameLine) {
        return ""
                + "name: remote-validate\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 1}]\n"
                + "  - id: enrich\n"
                + "    type: script.remote\n"
                + "    config:\n"
                + "      " + configLines + "\n"
                + (nameLine == null ? "" : nameLine + "\n")
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n";
    }

    private static void respond(HttpExchange exchange, int code, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
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
        return result.getNodeMetrics().get("enrich");
    }
}
