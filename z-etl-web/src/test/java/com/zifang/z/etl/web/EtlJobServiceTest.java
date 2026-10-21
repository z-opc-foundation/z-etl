package com.zifang.z.etl.web;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.InMemoryJobRepository;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.etl.web.api.EtlJobService;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 管控面（web 层）服务链路：登记 → 列出 → 取描述 → 触发 → 看水位 → 看履历。
 *
 * <p>跑在本地 H2 上，不连生产库。
 *
 * @author zifang
 */
class EtlJobServiceTest {

    private DataSource source;
    private DataSource target;
    private InMemoryJobRepository repository;
    private InMemoryCheckpointStore checkpointStore;
    private EtlJobService service;

    private static final String DESCRIPTOR =
            "job:\n"
                    + "  name: web-demo\n"
                    + "  mode: auto\n"
                    + "  source:\n"
                    + "    dataSourceRef: ds-src\n"
                    + "    table: demo_src\n"
                    + "    columns: [id, name, amount, updated_at]\n"
                    + "    watermarkColumn: updated_at\n"
                    + "    pageSize: 10\n"
                    + "  transform:\n"
                    + "    fieldMappings:\n"
                    + "      - {source: id, target: row_id, type: LONG}\n"
                    + "      - {source: name, target: name, type: STRING}\n"
                    + "      - {source: amount, target: amount, type: BIGDECIMAL}\n"
                    + "      - {source: updated_at, target: updated_at, type: TIMESTAMP}\n"
                    + "  target:\n"
                    + "    dataSourceRef: ds-dst\n"
                    + "    table: demo_dst\n"
                    + "    batchSize: 10\n"
                    + "  idempotencyKey: [row_id]\n"
                    + "  watermark:\n"
                    + "    column: updated_at\n"
                    + "    type: TIMESTAMP\n";

    @BeforeEach
    void setUp() throws Exception {
        source = h2("web_src_" + System.nanoTime());
        target = h2("web_dst_" + System.nanoTime());
        exec(source, "CREATE TABLE demo_src (id BIGINT PRIMARY KEY, name VARCHAR(64),"
                + " amount DECIMAL(18,2), updated_at TIMESTAMP)");
        exec(target, "CREATE TABLE demo_dst (row_id BIGINT PRIMARY KEY, name VARCHAR(64),"
                + " amount DECIMAL(18,2), updated_at TIMESTAMP)");
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO demo_src (id, name, amount, updated_at) VALUES (?, ?, ?, ?)");
            try {
                for (int i = 1; i <= 5; i++) {
                    statement.setLong(1, i);
                    statement.setString(2, "行" + i);
                    statement.setBigDecimal(3, new BigDecimal(i + ".50"));
                    statement.setTimestamp(4, new java.sql.Timestamp(1767225600000L + i * 60000L));
                    statement.addBatch();
                }
                statement.executeBatch();
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }

        repository = new InMemoryJobRepository();
        checkpointStore = new InMemoryCheckpointStore();
        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register("ds-src", source)
                .register("ds-dst", target);
        EtlEngine engine = new EtlEngine(provider, repository, checkpointStore);
        service = new EtlJobService(repository, checkpointStore, engine);
    }

    @Test
    @DisplayName("登记 → 触发 → 水位可见 → 履历有记录")
    void registerRunWatermarkAndHistory() throws Exception {
        assertEquals("web-demo", service.register(DESCRIPTOR));
        assertTrue(service.list().contains("web-demo"));

        JobDescriptor descriptor = service.get("web-demo");
        assertNotNull(descriptor);
        assertEquals("demo_src", descriptor.getSource().getTable());
        assertEquals("demo_dst", descriptor.getTarget().getTable());

        EtlRunResult result = service.run("web-demo");
        assertEquals("init", result.getMode());
        assertEquals(5L, result.getRowsRead());
        assertEquals(5L, result.getRowsWritten());
        assertTrue(result.isReconciled());

        // 水位可见
        assertNotNull(service.watermark("web-demo"));
        assertNotNull(service.watermark("web-demo").getWatermarkValue());
        assertEquals("init", service.watermark("web-demo").getLastMode());

        // 履历有记录
        assertEquals(1, service.history("web-demo", 10).size());
        assertEquals("web-demo", service.history("web-demo", 10).get(0).getJobName());
    }

    private static DataSource h2(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

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
