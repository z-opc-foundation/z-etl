package com.zifang.z.etl.web;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.InMemoryPipelineRepository;
import com.zifang.z.etl.core.engine.InMemoryPipelineScheduleRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.etl.web.api.EtlPipelineService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行总览（web 层）验收：{@code overview} 的窗口计数（24h/7d 成功失败）、
 * 每条流水线最近一次运行、调度登记、最近运行明细。
 *
 * <p>跑在内存仓储 + 本地 H2 上，不连生产库。
 *
 * @author zifang
 */
class EtlPipelineOverviewTest {

    private EtlPipelineService service;

    private static final String OK_SPEC =
            "name: overview-ok\n"
                    + "nodes:\n"
                    + "  - id: src\n"
                    + "    type: jdbc.source\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-src\n"
                    + "      table: o_src\n"
                    + "  - id: sink\n"
                    + "    type: jdbc.sink\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-dst\n"
                    + "      table: o_dst\n"
                    + "      writeMode: upsert\n"
                    + "      idempotencyKey: [id]\n"
                    + "edges:\n"
                    + "  - {from: src, to: sink}\n";

    private static final String BROKEN_SPEC =
            "name: overview-broken\n"
                    + "nodes:\n"
                    + "  - id: src\n"
                    + "    type: jdbc.source\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-missing\n"
                    + "      table: o_src\n"
                    + "  - id: sink\n"
                    + "    type: jdbc.sink\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-dst\n"
                    + "      table: o_dst\n"
                    + "      writeMode: append\n"
                    + "edges:\n"
                    + "  - {from: src, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        DataSource source = h2("o_src_" + System.nanoTime());
        DataSource target = h2("o_dst_" + System.nanoTime());
        exec(source, "CREATE TABLE o_src (id BIGINT PRIMARY KEY, name VARCHAR(64))");
        exec(target, "CREATE TABLE o_dst (id BIGINT PRIMARY KEY, name VARCHAR(64))");
        exec(source, "INSERT INTO o_src (id, name) VALUES (1, '行1')");
        PipelineEngine engine = new PipelineEngine(
                new MapDataSourceProvider()
                        .register("ds-src", source)
                        .register("ds-dst", target),
                new InMemoryPipelineRepository(),
                new InMemoryCheckpointStore(),
                new InMemoryPipelineStateStore(), null, new InMemoryPipelineScheduleRepository());
        service = new EtlPipelineService(engine);
        service.register(OK_SPEC);
        service.register(BROKEN_SPEC);
    }

    @Test
    @DisplayName("overview：窗口计数、每流水线最近一次运行、失败也计入、最近明细")
    void overviewAggregates() throws Exception {
        service.run("overview-ok");
        try {
            service.run("overview-broken");
            throw new AssertionError("坏引用流水线应当失败");
        } catch (IllegalArgumentException expected) {
            // 失败即目标
        }
        service.schedule("overview-ok", "0 0 * * *", true);

        Map<String, Object> overview = service.overview(10);

        Map<?, ?> h24 = (Map<?, ?>) overview.get("h24");
        assertEquals(Integer.valueOf(1), h24.get("success"), "24h 窗口成功 1 次");
        assertEquals(Integer.valueOf(1), h24.get("failed"), "24h 窗口失败 1 次（失败运行也落履历）");
        assertEquals(Integer.valueOf(2), h24.get("total"));

        List<Map<String, Object>> pipelines = (List<Map<String, Object>>) overview.get("pipelines");
        assertEquals(2, pipelines.size());
        Map<String, Object> okRow = pipelines.stream()
                .filter((row) -> "overview-ok".equals(row.get("name"))).findFirst().orElse(null);
        assertNotNull(okRow);
        assertEquals(PipelineRunResult.STATUS_SUCCESS, okRow.get("lastStatus"));
        assertNotNull(okRow.get("lastDurationMs"));
        assertNotNull(okRow.get("schedule"), "调度登记在总览里");
        Map<String, Object> brokenRow = pipelines.stream()
                .filter((row) -> "overview-broken".equals(row.get("name"))).findFirst().orElse(null);
        assertNotNull(brokenRow);
        assertEquals(PipelineRunResult.STATUS_FAILED, brokenRow.get("lastStatus"));

        List<Map<String, Object>> recent = (List<Map<String, Object>>) overview.get("recentRuns");
        assertEquals(2, recent.size(), "最近明细含成功与失败两条");
        assertTrue(String.valueOf(recent.get(0).get("status")).length() > 0);
    }

    @Test
    @DisplayName("从未运行过的流水线：lastStatus 为空但不缺行")
    void neverRunPipelineStillListed() throws Exception {
        Map<String, Object> overview = service.overview(10);
        List<Map<String, Object>> pipelines = (List<Map<String, Object>>) overview.get("pipelines");
        Map<String, Object> okRow = pipelines.stream()
                .filter((row) -> "overview-ok".equals(row.get("name"))).findFirst().orElse(null);
        assertNotNull(okRow, "未运行也列出");
        assertNull(okRow.get("lastStatus"));
        assertNull(okRow.get("schedule"), "未登记调度时 schedule 为空");
    }

    private static DataSource h2(String name) {
        org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    private static void exec(DataSource dataSource, String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
