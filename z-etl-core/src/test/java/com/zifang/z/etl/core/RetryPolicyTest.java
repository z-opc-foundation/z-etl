package com.zifang.z.etl.core;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.InMemoryPipelineRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试验收：作业级与流水线级都支持「失败自动重试 + 指数退避」。
 *
 * <p>用本地 HTTP 接口做「前 N 次失败」的可控故障源（不连外网）。
 *
 * @author zifang
 */
class RetryPolicyTest {

    @Test
    @DisplayName("线性作业：前两次失败、第三次成功 ⇒ 自动重试后跑通")
    void linearJobRetriesUntilSuccess() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startFailingServer(hits, 2);
        try {
            String yaml = ""
                    + "job:\n"
                    + "  name: retry-job\n"
                    + "  mode: auto\n"
                    + "  source:\n"
                    + "    type: inline\n"
                    + "    options:\n"
                    + "      rows:\n"
                    + "        - {id: 1}\n"
                    + "  transform:\n"
                    + "    fieldMappings:\n"
                    + "      - {source: id, target: id, type: LONG}\n"
                    + "  target:\n"
                    + "    type: http\n"
                    + "    options:\n"
                    + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/ingest\n"
                    + "  idempotencyKey: [id]\n"
                    + "  retry:\n"
                    + "    maxAttempts: 3\n"
                    + "    backoffMs: 10\n";

            JobDescriptor descriptor = JobDescriptorParser.parse(yaml);
            EtlEngine engine = new EtlEngine(new MapDataSourceProvider(), null, new InMemoryCheckpointStore());
            EtlRunResult result = engine.run(descriptor);
            assertEquals(1L, result.getRowsWritten());
            assertEquals(3, hits.get(), "应请求 3 次（前两次失败）");
            assertTrue(result.getNotes().toString().contains("第 3 次尝试成功"),
                    "履历应注明第几次尝试成功: " + result.getNotes());
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("流水线：前两次失败、第三次成功 ⇒ 自动重试后跑通")
    void pipelineRetriesUntilSuccess() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startFailingServer(hits, 2);
        try {
            String yaml = ""
                    + "name: retry-pipeline\n"
                    + "retry:\n"
                    + "  maxAttempts: 3\n"
                    + "  backoffMs: 10\n"
                    + "nodes:\n"
                    + "  - id: src\n"
                    + "    type: inline.source\n"
                    + "    config:\n"
                    + "      rows:\n"
                    + "        - {id: 1}\n"
                    + "  - id: sink\n"
                    + "    type: http.sink\n"
                    + "    config:\n"
                    + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/ingest\n"
                    + "edges:\n"
                    + "  - {from: src, to: sink}\n";

            PipelineEngine engine = new PipelineEngine(new MapDataSourceProvider(),
                    new InMemoryPipelineRepository(), new InMemoryCheckpointStore(),
                    new InMemoryPipelineStateStore());
            engine.register(yaml);
            PipelineRunResult result = engine.run("retry-pipeline");
            assertEquals(1L, result.getRecordsWritten());
            assertEquals(3, hits.get(), "应请求 3 次（前两次失败）");
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("重试用尽 ⇒ 报错说清「连续 N 次失败」")
    void exhaustedRetriesReportAttempts() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = startFailingServer(hits, Integer.MAX_VALUE);
        try {
            String yaml = ""
                    + "job:\n"
                    + "  name: retry-exhausted\n"
                    + "  mode: auto\n"
                    + "  source:\n"
                    + "    type: inline\n"
                    + "    options:\n"
                    + "      rows:\n"
                    + "        - {id: 1}\n"
                    + "  transform:\n"
                    + "    fieldMappings:\n"
                    + "      - {source: id, target: id, type: LONG}\n"
                    + "  target:\n"
                    + "    type: http\n"
                    + "    options:\n"
                    + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/ingest\n"
                    + "  idempotencyKey: [id]\n"
                    + "  retry:\n"
                    + "    maxAttempts: 2\n"
                    + "    backoffMs: 10\n";

            EtlEngine engine = new EtlEngine(new MapDataSourceProvider(), null, new InMemoryCheckpointStore());
            Exception error = assertThrows(IllegalStateException.class,
                    new org.junit.jupiter.api.function.Executable() {
                        @Override
                        public void execute() {
                            engine.run(JobDescriptorParser.parse(yaml));
                        }
                    });
            assertTrue(error.getMessage().contains("连续 2 次失败"), "实际: " + error.getMessage());
            assertEquals(2, hits.get(), "应只尝试 2 次");
        } finally {
            server.stop(0);
        }
    }

    /** 前 {@code failures} 次返回 500，之后返回 200。 */
    private HttpServer startFailingServer(final AtomicInteger hits, final int failures) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ingest", new com.sun.net.httpserver.HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws java.io.IOException {
                int hit = hits.incrementAndGet();
                byte[] body = "{\"id\":1}".getBytes("UTF-8");
                int status = hit <= failures ? 500 : 200;
                exchange.sendResponseHeaders(status, body.length);
                OutputStream out = exchange.getResponseBody();
                out.write(body);
                out.close();
            }
        });
        server.start();
        return server;
    }
}
