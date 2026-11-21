package com.zifang.z.etl.web;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.InMemoryPipelineRepository;
import com.zifang.z.etl.core.engine.InMemoryPipelineScheduleRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流水线管控面（web 层）运行指标暴露验收：{@code history} 里每次运行带
 * 观测三件套（读了多少 / 写了多少 / 每节点计数快照）+ 逐节点耗时 + 总耗时。
 *
 * <p>跑在本地 H2 上，不连生产库。
 *
 * @author zifang
 */
class EtlPipelineMetricsTest {

    private EtlPipelineService service;

    private static final String SPEC =
            "name: metrics-pipeline\n"
                    + "nodes:\n"
                    + "  - id: src\n"
                    + "    type: jdbc.source\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-src\n"
                    + "      table: m_src\n"
                    + "      columns: [id, name, amount]\n"
                    + "      pageSize: 2\n"
                    + "  - id: fm\n"
                    + "    type: field.mapping\n"
                    + "    config:\n"
                    + "      mappings:\n"
                    + "        - {source: id, target: row_id, type: LONG}\n"
                    + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                    + "  - id: sink\n"
                    + "    type: jdbc.sink\n"
                    + "    config:\n"
                    + "      dataSourceRef: ds-dst\n"
                    + "      table: m_dst\n"
                    + "      writeMode: upsert\n"
                    + "      idempotencyKey: [row_id]\n"
                    + "edges:\n"
                    + "  - {from: src, to: fm}\n"
                    + "  - {from: fm, to: sink}\n";

    @BeforeEach
    void setUp() throws Exception {
        DataSource source = h2("m_src_" + System.nanoTime());
        DataSource target = h2("m_dst_" + System.nanoTime());
        exec(source, "CREATE TABLE m_src (id BIGINT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(18,2))");
        exec(target, "CREATE TABLE m_dst (row_id BIGINT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(18,2))");
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO m_src (id, name, amount) VALUES (?, ?, ?)");
            try {
                for (int i = 1; i <= 5; i++) {
                    statement.setLong(1, i);
                    statement.setString(2, "行" + i);
                    statement.setBigDecimal(3, new BigDecimal(i + ".50"));
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
    @DisplayName("history 带观测面：读 / 写总量、逐节点 records-in 与 elapsed-nanos、总耗时")
    void historyExposesMetrics() throws Exception {
        service.run("metrics-pipeline");
        PipelineRunResult last = service.history("metrics-pipeline", 1).get(0);

        assertEquals(5L, last.getRecordsRead());
        assertEquals(5L, last.getRecordsWritten());
        assertTrue(last.getStartedAt() > 0L, "有开始时间");

        Map<String, Map<String, Long>> metrics = last.getNodeMetrics();
        assertNotNull(metrics.get("src"), "源节点指标在");
        assertNotNull(metrics.get("fm"), "变换节点指标在");
        assertNotNull(metrics.get("sink"), "汇节点指标在");

        assertEquals(Long.valueOf(5L), metrics.get("fm").get("records-in"), "变换收到 5 行");
        assertEquals(Long.valueOf(5L), metrics.get("fm").get("records-out"), "变换发出 5 行（与 records-in 对称）");
        assertEquals(Long.valueOf(5L), metrics.get("sink").get("records-in"), "汇收到 5 行");
        assertNotNull(metrics.get("src").get("elapsed-nanos"), "源有耗时指标");
        for (String node : new String[] {"src", "fm", "sink"}) {
            assertTrue(metrics.get(node).containsKey("elapsed-nanos"),
                    "节点 " + node + " 有耗时: " + metrics.get(node));
            assertTrue(metrics.get(node).get("elapsed-nanos") >= 0L, "耗时非负");
        }
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
