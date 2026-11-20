package com.zifang.z.etl.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.checkpoint.Savepoint;
import com.zifang.z.etl.core.checkpoint.Savepoints;
import com.zifang.z.etl.core.engine.InMemoryPipelineRepository;
import com.zifang.z.etl.core.engine.InMemoryPipelineScheduleRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.etl.web.api.EtlPipelineService;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流水线管控面（web 层）savepoint 与 dry-run 链路：导出 → 换名导入 → 照常跑；
 * dry-run 按采样行数预演。
 *
 * <p>跑在本地 H2 上，不连生产库。
 *
 * @author zifang
 */
class EtlPipelineSavepointTest {

    private EtlPipelineService service;

    private static final String SPEC =
            "name: sp-pipeline\n"
                    + "nodes:\n"
                    + "  - id: src\n"
                    + "    type: jdbc.source\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-src\n"
                    + "      table: sp_src\n"
                    + "      columns: [id, updated_at]\n"
                    + "      pageSize: 2\n"
                    + "      eventTimeColumn: updated_at\n"
                    + "      eventTimeType: TIMESTAMP\n"
                    + "      resume: {column: updated_at, type: TIMESTAMP, tiebreakColumn: id}\n"
                    + "  - id: sink\n"
                    + "    type: jdbc.sink\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-dst\n"
                    + "      table: sp_dst\n"
                    + "      writeMode: upsert\n"
                    + "      idempotencyKey: [id]\n"
                    + "edges:\n"
                    + "  - {from: src, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        DataSource source = h2("sp_src_" + System.nanoTime());
        DataSource target = h2("sp_dst_" + System.nanoTime());
        exec(source, "CREATE TABLE sp_src (id BIGINT PRIMARY KEY, updated_at TIMESTAMP)");
        exec(target, "CREATE TABLE sp_dst (id BIGINT PRIMARY KEY, updated_at TIMESTAMP)");
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO sp_src (id, updated_at) VALUES (?, ?)");
            try {
                for (int i = 1; i <= 5; i++) {
                    statement.setLong(1, i);
                    statement.setTimestamp(2, new java.sql.Timestamp(1767225600000L + i * 60000L));
                    statement.addBatch();
                }
                statement.executeBatch();
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
        PipelineEngine engine = new PipelineEngine(
                new MapDataSourceProvider()
                        .register("ds-src", source)
                        .register("ds-dst", target),
                new InMemoryPipelineRepository(),
                new InMemoryCheckpointStore(),
                new InMemoryPipelineStateStore(), null, new InMemoryPipelineScheduleRepository());
        service = new EtlPipelineService(engine);
        service.register(SPEC);
    }

    @Test
    @DisplayName("savepoint 往返（迁移口径）：副本先登记同构管道 → 跑原名 → 导出 → 换名灌回 → 副本增量续跑")
    void savepointRoundtripWithRename() throws Exception {
        service.register(SPEC.replace("name: sp-pipeline\n", "name: sp-pipeline-copy\n"));
        assertEquals(5L, service.run("sp-pipeline").getRecordsWritten(), "原名先跑全量");

        Savepoint savepoint = service.exportSavepoint("sp-pipeline");
        assertNotNull(savepoint);
        assertEquals("sp-pipeline", savepoint.getPipeline());

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> body = new HashMap<String, Object>();
        body.put("savepoint", mapper.readValue(Savepoints.toJson(savepoint), Map.class));
        body.put("targetName", "sp-pipeline-copy");
        Map<String, Object> imported = service.importSavepoint("sp-pipeline", body);
        assertNotNull(imported);

        PipelineRunResult copyRun = service.run("sp-pipeline-copy");
        assertEquals(0L, copyRun.getRecordsWritten(),
                "副本灌回水位后增量续跑：源头没有新行 ⇒ 0 行");
        assertTrue(service.list().contains("sp-pipeline-copy"));
        assertTrue(service.list().contains("sp-pipeline"), "原名不受影响");
    }

    @Test
    @DisplayName("dry-run：按采样行数预演，不落库到履历之外的痕迹")
    void dryRunSamplesRows() throws Exception {
        PipelineRunResult result = service.dryRun("sp-pipeline", null, 3L);
        assertEquals(3L, result.getRecordsWritten(), "采样 3 行就写 3 行");
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
