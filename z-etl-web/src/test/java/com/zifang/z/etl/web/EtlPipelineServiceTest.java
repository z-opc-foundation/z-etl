package com.zifang.z.etl.web;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.InMemoryPipelineRepository;
import com.zifang.z.etl.core.engine.InMemoryPipelineScheduleRepository;
import com.zifang.z.etl.core.engine.PipelineSchedule;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流水线管控面（web 层）服务链路：登记 → 列出 → 取定义 → 计划解释 → 触发 → 看水位 → 看履历。
 *
 * <p>跑在本地 H2 上，不连生产库。
 *
 * @author zifang
 */
class EtlPipelineServiceTest {

    private EtlPipelineService service;

    private static final String SPEC =
            "name: web-pipeline\n"
                    + "description: web 层验收\n"
                    + "nodes:\n"
                    + "  - id: src\n"
                    + "    type: jdbc.source\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-src\n"
                    + "      table: pipe_src\n"
                    + "      columns: [id, name, amount, updated_at]\n"
                    + "      pageSize: 2\n"
                    + "      eventTimeColumn: updated_at\n"
                    + "      eventTimeType: TIMESTAMP\n"
                    + "      resume: {column: updated_at, type: TIMESTAMP, tiebreakColumn: id}\n"
                    + "  - id: fm\n"
                    + "    type: field.mapping\n"
                    + "    config:\n"
                    + "      mappings:\n"
                    + "        - {source: id, target: row_id, type: LONG}\n"
                    + "        - {source: name, target: name, type: STRING}\n"
                    + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                    + "        - {source: updated_at, target: updated_at, type: TIMESTAMP}\n"
                    + "  - id: sink\n"
                    + "    type: jdbc.sink\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-dst\n"
                    + "      table: pipe_dst\n"
                    + "      writeMode: upsert\n"
                    + "      idempotencyKey: [row_id]\n"
                    + "edges:\n"
                    + "  - {from: src, to: fm}\n"
                    + "  - {from: fm, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        DataSource source = h2("pipe_src_" + System.nanoTime());
        DataSource target = h2("pipe_dst_" + System.nanoTime());
        exec(source, "CREATE TABLE pipe_src (id BIGINT PRIMARY KEY, name VARCHAR(64),"
                + " amount DECIMAL(18,2), updated_at TIMESTAMP)");
        exec(target, "CREATE TABLE pipe_dst (row_id BIGINT PRIMARY KEY, name VARCHAR(64),"
                + " amount DECIMAL(18,2), updated_at TIMESTAMP)");
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO pipe_src (id, name, amount, updated_at) VALUES (?, ?, ?, ?)");
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

        PipelineEngine engine = new PipelineEngine(
                new MapDataSourceProvider()
                        .register("ds-src", source)
                        .register("ds-dst", target),
                new InMemoryPipelineRepository(),
                new InMemoryCheckpointStore(),
                new InMemoryPipelineStateStore(), null, new InMemoryPipelineScheduleRepository());
        service = new EtlPipelineService(engine);
    }

    @Test
    @DisplayName("登记 → 触发 → 计划解释 → 水位可见 → 履历有记录")
    void registerExplainRunWatermarkAndHistory() throws Exception {
        assertEquals("web-pipeline", service.register(SPEC));
        assertTrue(service.list().contains("web-pipeline"));

        PipelineSpec spec = service.get("web-pipeline");
        assertNotNull(spec);
        assertEquals("web-pipeline", spec.getName());

        String plan = service.explain("web-pipeline");
        assertTrue(plan.contains("jdbc.source"), "计划里有源算子");
        assertTrue(plan.contains("jdbc.sink"), "计划里有汇算子");

        PipelineRunResult result = service.run("web-pipeline");
        assertEquals(5L, result.getRecordsWritten());

        assertNotNull(service.watermark("web-pipeline"));

        assertEquals(1, service.history("web-pipeline", 10).size());
        assertEquals("web-pipeline", service.history("web-pipeline", 10).get(0).getPipelineName());
    }

    @Test
    @DisplayName("调度登记：登记 cron ⇒ 看得见下一次触发时间 ⇒ 可取消")
    void scheduleIsVisibleAndCancelable() {
        assertEquals("web-pipeline", service.register(SPEC));
        PipelineSchedule schedule = service.schedule("web-pipeline", "0 * * * *", true);
        assertNotNull(schedule);
        assertEquals("0 * * * *", schedule.getCronExpression());
        assertNotNull(schedule.getNextFireTime());
        assertEquals(1, service.schedules().size());
        assertNotNull(service.schedule("web-pipeline"));
        assertTrue(service.unschedule("web-pipeline"));
        assertEquals(0, service.schedules().size());
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
