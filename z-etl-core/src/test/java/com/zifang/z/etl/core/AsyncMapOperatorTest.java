package com.zifang.z.etl.core;

import com.sun.net.httpserver.HttpServer;
import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异步映射（{@code async.map}）验收：每行异步外呼 HTTP 补列。
 *
 * <p>口径：容量背压（在途满阻塞等一个）、ordered 按到达序 / unordered 完成即发、
 * 流结束 flush 把在途全部发出、onError fail/skip、allowedHosts 信任边界。
 *
 * @author zifang
 */
class AsyncMapOperatorTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 64);
        server.setExecutor(Executors.newCachedThreadPool());
        // /profile/{id}?delayMs=NN → {"data": {"balance": id * 100}}
        server.createContext("/profile", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();
            int id = Integer.parseInt(path.substring(path.lastIndexOf('/') + 1));
            // slow=1 ⇒ 首个 id 故意慢（ordered 排序验证用）；all=NN ⇒ 每个 id 都慢（并发验证用）
            try {
                if (query != null && query.contains("slow=1")) {
                    Thread.sleep(id == 1 ? 300L : 20L);
                } else if (query != null && query.contains("all=")) {
                    Thread.sleep(Long.parseLong(query.replaceAll(".*all=", "")));
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            byte[] body = ("{\"data\": {\"balance\": " + (id * 100) + "}}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("content-type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        // /broken/{id} → HTTP 500
        server.createContext("/broken", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        port = server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rows() {
        return ((SinkOperators.CollectSink) captured.get("out")).rows();
    }

    private Map<String, Long> metrics(PipelineRunResult result) {
        return result.getNodeMetrics().get("enrich");
    }

    private String rowsYaml(int... ids) {
        StringBuilder sb = new StringBuilder();
        for (int id : ids) {
            sb.append("        - {user_id: ").append(id).append(", name: u").append(id).append("}\n");
        }
        return sb.toString();
    }

    private String pipeline(String path, String extraConfig, int... ids) {
        return ""
                + "name: async-map\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + rowsYaml(ids)
                + "  - id: enrich\n"
                + "    type: async.map\n"
                + "    config:\n"
                + "      url: http://127.0.0.1:" + port + path + "\n"
                + (extraConfig.isEmpty() ? "" : "      " + extraConfig + "\n")
                + "      fields: [{target: balance, source: data.balance}]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n";
    }

    @Test
    @DisplayName("基本补列 + 流结束 flush：单行经 flush 产出，balance 从响应点路径提取")
    void enrichSingleRowViaFlush() throws Exception {
        PipelineRunResult result = run(pipeline("/profile/{user_id}", "timeoutMs: 3000", 7));
        List<Map<String, Object>> rows = rows();
        assertEquals(1, rows.size());
        assertEquals(Integer.valueOf(700), rows.get(0).get("balance"));
        assertEquals("u7", rows.get(0).get("name"), "原行字段保留");
        assertEquals(Long.valueOf(1L), metrics(result).get("enriched"));
    }

    @Test
    @DisplayName("ordered：服务器对早到的行故意慢，产出仍严格按行到达序")
    void orderedKeepsArrivalOrder() throws Exception {
        // user_id 1 慢 300ms，user_id 2 快 —— ordered 口径下 1 必须先出
        PipelineRunResult result = run(pipeline("/profile/{user_id}?slow=1", "", 1, 2));
        List<Map<String, Object>> rows = rows();
        assertEquals(2, rows.size());
        assertEquals(Integer.valueOf(1), rows.get(0).get("user_id"));
        assertEquals(Integer.valueOf(2), rows.get(1).get("user_id"));
        assertEquals(Integer.valueOf(100), rows.get(0).get("balance"));
        assertEquals(Integer.valueOf(200), rows.get(1).get("balance"));
        assertEquals(Long.valueOf(2L), metrics(result).get("enriched"));
    }

    @Test
    @DisplayName("并发在途：3 行各慢 300ms + capacity 3 ⇒ 墙钟远小于串行 900ms")
    void inFlightParallelism() throws Exception {
        long start = System.currentTimeMillis();
        run(pipeline("/profile/{user_id}?all=300", "capacity: 3\n      outputMode: unordered", 1, 2, 3));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 800L, "3 个请求应并发在途（串行要 ~900ms），实际 " + elapsed + "ms");
        assertEquals(3, rows().size());
    }

    @Test
    @DisplayName("容量背压：capacity 1 也能全部产出（只是没有并发重叠）")
    void capacityBackpressure() throws Exception {
        run(pipeline("/profile/{user_id}", "capacity: 1", 1, 2, 3));
        assertEquals(3, rows().size());
        assertEquals(Integer.valueOf(300), rows().get(2).get("balance"));
    }

    @Test
    @DisplayName("onError: skip —— 500 的行丢掉并计数，其余行照常产出")
    void errorSkipped() throws Exception {
        String yaml = ""
                + "name: async-skip\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user_id: 1}\n"
                + "        - {user_id: 2}\n"
                + "  - id: enrich\n"
                + "    type: async.map\n"
                + "    config:\n"
                + "      url: http://127.0.0.1:" + port + "/broken/{user_id}\n"
                + "      onError: skip\n"
                + "      fields: [{target: balance, source: data.balance}]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n";
        PipelineRunResult result = run(yaml);
        assertEquals(0, rows().size(), "全 500 ⇒ 全丢");
        assertEquals(Long.valueOf(2L), metrics(result).get("failed-skipped"));
    }

    @Test
    @DisplayName("onError 缺省 fail：HTTP 500 抛出点名")
    void errorFailsByDefault() {
        String yaml = ""
                + "name: async-fail\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user_id: 1}\n"
                + "  - id: enrich\n"
                + "    type: async.map\n"
                + "    config:\n"
                + "      url: http://127.0.0.1:" + port + "/broken/{user_id}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n";
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> run(yaml));
        assertTrue(error.getMessage().contains("外呼失败"), "应点名外呼失败: " + error.getMessage());
    }

    @Test
    @DisplayName("缺 url 校验期报错；allowedHosts 不在白名单 ⇒ 拒绝访问")
    void rejectsBrokenConfigs() {
        String noUrl = ""
                + "name: async-no-url\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user_id: 1}\n"
                + "  - id: enrich\n"
                + "    type: async.map\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: enrich}\n"
                + "  - {from: enrich, to: out}\n";
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> run(noUrl));
        assertTrue(missing.getMessage().contains("url"), "缺 url 应报错: " + missing.getMessage());

        String blocked = pipeline("/profile/{user_id}", "allowedHosts: [other-host]", 1);
        // 执行器会把派发期异常包成 IllegalStateException，根因是白名单拒绝
        IllegalStateException denied = assertThrows(IllegalStateException.class, () -> run(blocked));
        assertTrue(denied.getMessage().contains("拒绝访问"), "白名单外应拒绝: " + denied.getMessage());
    }
}
