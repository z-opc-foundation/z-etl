package com.zifang.z.etl.core;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.connector.JdbcMessageBus;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import com.zifang.z.etl.core.job.JobDescriptorValidator;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 线性作业层的连接器验收：**job 描述里的 {@code source.type} / {@code target.type}**
 * 与 DAG 层共用同一批连接器实现（文件 / HTTP / 消息 / 内联 …）。
 *
 * <p>⚠️ 全部跑在本地 H2 + 临时目录 + 本地 HTTP 服务上，不连生产库、不发外网请求。
 *
 * @author zifang
 */
class LinearConnectorJobTest {

    @TempDir
    Path temp;

    @Test
    @DisplayName("线性作业：file.csv 源 → http 汇，并按水位自动切增量")
    void csvSourceToHttpSinkWithIncrementalResume() throws Exception {
        Path csv = temp.resolve("orders.csv");
        Files.write(csv, ("id,amount,updated_at\n"
                + "1,10.50,2026-01-01 00:00:00\n"
                + "2,20.50,2026-01-01 00:01:00\n"
                + "3,30.50,2026-01-01 00:02:00\n").getBytes(StandardCharsets.UTF_8));

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
            String yaml = ""
                    + "job:\n"
                    + "  name: csv-to-http\n"
                    + "  mode: auto\n"
                    + "  source:\n"
                    + "    type: file.csv\n"
                    + "    pageSize: 2\n"
                    + "    watermarkColumn: updated_at\n"
                    + "    options:\n"
                    + "      path: " + csv + "\n"
                    + "      columnTypes: {amount: BIGDECIMAL, updated_at: TIMESTAMP}\n"
                    + "  transform:\n"
                    + "    fieldMappings:\n"
                    + "      - {source: id, target: id, type: LONG}\n"
                    + "      - {source: amount, target: amount, type: BIGDECIMAL}\n"
                    + "      - {source: updated_at, target: updated_at, type: TIMESTAMP}\n"
                    + "  target:\n"
                    + "    type: http\n"
                    + "    options:\n"
                    + "      url: http://127.0.0.1:" + server.getAddress().getPort() + "/ingest\n"
                    + "  idempotencyKey: [id]\n"
                    + "  watermark:\n"
                    + "    column: updated_at\n"
                    + "    type: TIMESTAMP\n";

            JobDescriptor descriptor = JobDescriptorParser.parse(yaml);
            JobDescriptorValidator.validate(descriptor);

            CheckpointStore checkpoints = new InMemoryCheckpointStore();
            EtlEngine engine = new EtlEngine(new MapDataSourceProvider(), null, checkpoints);

            EtlRunResult first = engine.run(descriptor);
            assertEquals(3L, first.getRowsRead());
            assertEquals(3L, first.getRowsWritten());
            assertEquals(3, payloads.size(), "应发出 3 次 HTTP");
            assertTrue(payloads.get(0).contains("\"id\":1"), "实际: " + payloads.get(0));
            assertTrue(first.isReconciled(), "行内口径对账应一致");
            assertNotNull(checkpoints.load("csv-to-http"), "水位应落库");
            assertNotNull(first.getWatermarkAfter());

            // 追加一行 ⇒ 第二轮自动切增量，只搬新增行
            Files.write(csv, ("id,amount,updated_at\n"
                    + "1,10.50,2026-01-01 00:00:00\n"
                    + "2,20.50,2026-01-01 00:01:00\n"
                    + "3,30.50,2026-01-01 00:02:00\n"
                    + "4,40.50,2026-01-01 00:03:00\n").getBytes(StandardCharsets.UTF_8));
            EtlRunResult second = engine.run(descriptor);
            assertEquals(1L, second.getRowsRead(), "第二轮只应读到新增行");
            assertEquals(1L, second.getRowsWritten());
            assertEquals(4, payloads.size(), "只多发 1 次 HTTP");
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("线性作业：inline 源 → file.json 汇（零 IO 输入 + 文件输出）")
    void inlineSourceToJsonFileSink() throws Exception {
        Path out = temp.resolve("out.json");
        String yaml = ""
                + "job:\n"
                + "  name: inline-to-json\n"
                + "  mode: auto\n"
                + "  source:\n"
                + "    type: inline\n"
                + "    options:\n"
                + "      rows:\n"
                + "        - {id: 1, name: 甲}\n"
                + "        - {id: 2, name: 乙}\n"
                + "  transform:\n"
                + "    fieldMappings:\n"
                + "      - {source: id, target: id, type: LONG}\n"
                + "      - {source: name, target: name, type: STRING}\n"
                + "  target:\n"
                + "    type: file.json\n"
                + "    options:\n"
                + "      path: " + out + "\n"
                + "      format: ndjson\n"
                + "  idempotencyKey: [id]\n";

        EtlEngine engine = new EtlEngine(new MapDataSourceProvider(), null, new InMemoryCheckpointStore());
        EtlRunResult result = engine.run(JobDescriptorParser.parse(yaml));
        assertEquals(2L, result.getRowsRead());
        assertEquals(2L, result.getRowsWritten());
        String content = new String(Files.readAllBytes(out), StandardCharsets.UTF_8);
        assertTrue(content.contains("\"name\":\"甲\""), "实际: " + content);
    }

    @Test
    @DisplayName("线性作业：file.json 源 → message 汇（消息总线落库队列）")
    void jsonSourceToMessageSink() throws Exception {
        Path ndjson = temp.resolve("events.ndjson");
        Files.write(ndjson, ("{\"id\":1,\"name\":\"甲\"}\n"
                + "{\"id\":2,\"name\":\"乙\"}\n").getBytes(StandardCharsets.UTF_8));

        DataSource meta = h2("linear_msg_" + System.nanoTime());
        JdbcMessageBus.ensure(meta);
        MessageBus bus = new JdbcMessageBus(meta);

        String yaml = ""
                + "job:\n"
                + "  name: json-to-mq\n"
                + "  mode: auto\n"
                + "  source:\n"
                + "    type: file.json\n"
                + "    options:\n"
                + "      path: " + ndjson + "\n"
                + "      format: ndjson\n"
                + "  transform:\n"
                + "    fieldMappings:\n"
                + "      - {source: id, target: id, type: LONG}\n"
                + "      - {source: name, target: name, type: STRING}\n"
                + "  target:\n"
                + "    type: message\n"
                + "    options:\n"
                + "      topic: order-events\n"
                + "  idempotencyKey: [id]\n";

        EtlEngine engine = new EtlEngine(new MapDataSourceProvider(), null,
                new InMemoryCheckpointStore(), bus);
        EtlRunResult result = engine.run(JobDescriptorParser.parse(yaml));
        assertEquals(2L, result.getRowsWritten());

        Connection connection = meta.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT COUNT(*) FROM etl_message_outbox WHERE topic = ?");
            statement.setString(1, "order-events");
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "两条消息应落库");
            rs.close();
            statement.close();
        } finally {
            connection.close();
        }
    }

    @Test
    @DisplayName("线性作业：类型不支持 / 必填项缺失 ⇒ 登记前就报错")
    void rejectsBadConnectorTypes() {
        Exception unknownType = assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                JobDescriptor descriptor = JobDescriptorParser.parse(""
                        + "job:\n"
                        + "  name: bad-type\n"
                        + "  source:\n"
                        + "    type: nosuch\n"
                        + "  target:\n"
                        + "    type: http\n"
                        + "  idempotencyKey: [id]\n");
                JobDescriptorValidator.validate(descriptor);
            }
        });
        assertTrue(unknownType.getMessage().contains("source.type"), "实际: " + unknownType.getMessage());

        Exception missingOption = assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                JobDescriptor descriptor = JobDescriptorParser.parse(""
                        + "job:\n"
                        + "  name: missing-path\n"
                        + "  source:\n"
                        + "    type: file.csv\n"
                        + "  target:\n"
                        + "    type: http\n"
                        + "  idempotencyKey: [id]\n");
                JobDescriptorValidator.validate(descriptor);
            }
        });
        assertTrue(missingOption.getMessage().contains("options.path"), "实际: " + missingOption.getMessage());
    }

    private static DataSource h2(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    /** 建一张普通表（取证用）。 */
    private static void exec(DataSource dataSource, String ddl) throws Exception {
        Connection connection = dataSource.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                statement.execute(ddl);
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }
}
