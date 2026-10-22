package com.zifang.z.etl.core;

import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本卡（TASK-20261012-014 水位轮询 + 断点续传）专属夹具。
 *
 * <p>⚠️ 与 {@link EtlTestFixtures} 分开：那张表由另一个 worker 共用，避免踩它的改动。
 * 口径完全一致 —— **两个独立 H2 内存库**（源库 + 目标库）+ 引擎元数据库，
 * 一个字节都不碰真实库 / 生产库。
 *
 * @author zifang
 */
final class WatermarkTestFixtures {

    static final String SOURCE_REF = "ds-oc-main";
    static final String TARGET_REF = "ds-etl-landing";
    static final String SOURCE_TABLE = "z_trade_order";
    static final String TARGET_TABLE = "etl_trade_order_snapshot";

    /** 源表 DDL（形状照生产订单表）。 */
    static final String SOURCE_DDL =
            "CREATE TABLE IF NOT EXISTS " + SOURCE_TABLE + " ("
                    + "id BIGINT PRIMARY KEY, "
                    + "order_no VARCHAR(64) NOT NULL, "
                    + "user_id BIGINT NOT NULL, "
                    + "user_name VARCHAR(128), "
                    + "product_code VARCHAR(64), "
                    + "amount DECIMAL(18,2) NOT NULL, "
                    + "quantity INT DEFAULT 1, "
                    + "status INT DEFAULT 0, "
                    + "payment_method VARCHAR(32), "
                    + "created_at TIMESTAMP, "
                    + "updated_at TIMESTAMP)";

    /** 目标表 DDL（幂等键 order_id 上有主键约束 ⇒ upsert 的前提）。 */
    static final String TARGET_DDL =
            "CREATE TABLE IF NOT EXISTS " + TARGET_TABLE + " ("
                    + "order_id BIGINT PRIMARY KEY, "
                    + "order_no VARCHAR(64) NOT NULL, "
                    + "user_id BIGINT, "
                    + "amount DECIMAL(18,2), "
                    + "status INT, "
                    + "updated_at TIMESTAMP)";

    private static final AtomicInteger UNIQUE = new AtomicInteger();

    private WatermarkTestFixtures() {
    }

    /** 独立命名的 H2 内存库（同 JVM 内跨用例存活 ⇒ 库名必须唯一）。 */
    static DataSource h2Mem(String prefix) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + prefix + "_" + UNIQUE.incrementAndGet() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    /** H2 **文件**库（跨进程存活 ⇒ 用来做「kill 进程后重启续传」的实测）。
     *
     * <p>{@code WRITE_DELAY=0}：MVStore 默认 1 秒才把改动刷到磁盘 ⇒ 进程被 SIGKILL 时
     * 最后一段提交可能丢。验收项 2 要的就是「被 kill 之后水位与数据仍在」⇒ 必须即时落盘。
     */
    static DataSource h2File(String path) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:file:" + path + ";WRITE_DELAY=0");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    /** 建表：源表 + 目标表。 */
    static void createSchema(DataSource source, DataSource target) throws SQLException {
        exec(source, SOURCE_DDL);
        exec(target, TARGET_DDL);
    }

    /** 灌一批行（时间戳等距递增）。 */
    static void insertOrders(DataSource source, long startId, int rows, Timestamp base, long stepMs)
            throws SQLException {
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO " + SOURCE_TABLE + " (id, order_no, user_id, user_name, product_code,"
                            + " amount, quantity, status, payment_method, created_at, updated_at)"
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            try {
                for (int i = 0; i < rows; i++) {
                    long id = startId + i;
                    statement.setLong(1, id);
                    statement.setString(2, String.format("SO%08d", id));
                    statement.setLong(3, 1000L + (id % 97));
                    statement.setString(4, "用户" + (id % 97));
                    statement.setString(5, "P" + (id % 31));
                    statement.setBigDecimal(6, new BigDecimal("100.00").add(new BigDecimal(id % 50)));
                    statement.setInt(7, 1 + (int) (id % 3));
                    statement.setInt(8, (int) (id % 3));
                    statement.setString(9, "alipay");
                    Timestamp ts = new Timestamp(base.getTime() + i * stepMs);
                    statement.setTimestamp(10, ts);
                    statement.setTimestamp(11, ts);
                    statement.addBatch();
                }
                statement.executeBatch();
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 灌一批**时间戳完全相同**的行（同秒多多的造数）。 */
    static void insertSameTimestamp(DataSource source, long startId, int rows, Timestamp ts) throws SQLException {
        insertOrders(source, startId, rows, ts, 0L);
    }

    /** 灌单行（order_no 可为 null ⇒ 用来制造目标侧写入失败）。 */
    static void insertOrder(DataSource source, long id, String orderNo, Timestamp updatedAt) throws SQLException {
        insertOrder(source, id, orderNo, 0, updatedAt);
    }

    /** 灌单行（可指定 status ⇒ 用来触发目标侧的 CHECK 约束失败）。 */
    static void insertOrder(DataSource source, long id, String orderNo, int status, Timestamp updatedAt)
            throws SQLException {
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO " + SOURCE_TABLE + " (id, order_no, user_id, user_name, product_code,"
                            + " amount, quantity, status, payment_method, created_at, updated_at)"
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            try {
                statement.setLong(1, id);
                statement.setString(2, orderNo);
                statement.setLong(3, 1000L + (id % 97));
                statement.setString(4, "用户" + (id % 97));
                statement.setString(5, "P" + (id % 31));
                statement.setBigDecimal(6, new BigDecimal("100.00"));
                statement.setInt(7, 1);
                statement.setInt(8, status);
                statement.setString(9, "alipay");
                statement.setTimestamp(10, updatedAt);
                statement.setTimestamp(11, updatedAt);
                statement.executeUpdate();
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 改一行（修好坏数据 ⇒ 复跑能过）。 */
    static void updateStatus(DataSource source, long id, int status) throws SQLException {
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "UPDATE " + SOURCE_TABLE + " SET status = ? WHERE id = ?");
            try {
                statement.setInt(1, status);
                statement.setLong(2, id);
                statement.executeUpdate();
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 跑一条 DDL / DML（造约束等）。 */
    static void execSql(DataSource dataSource, String sql) throws SQLException {
        exec(dataSource, sql);
    }

    /** 清空源表（造「游标越界」场景）。 */
    static void truncateSource(DataSource source) throws SQLException {
        exec(source, "DELETE FROM " + SOURCE_TABLE);
    }

    /** 表行数。 */
    static long count(DataSource dataSource, String table) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + table);
                try {
                    return rs.next() ? rs.getLong(1) : 0L;
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 单值查询。 */
    static Object scalar(DataSource dataSource, String sql) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet rs = statement.executeQuery(sql);
                try {
                    return rs.next() ? rs.getObject(1) : null;
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 直接查水位表（验收项 3：游标持久化可查）。 */
    static String checkpointValue(DataSource meta, String jobName) throws SQLException {
        Object value = scalar(meta, "SELECT watermark_value FROM etl_job_checkpoint WHERE job_name = '"
                + jobName + "'");
        return value == null ? null : String.valueOf(value);
    }

    /** 直接查水位的上次推进时间。 */
    static Long checkpointLastRunAt(DataSource meta, String jobName) throws SQLException {
        Object value = scalar(meta, "SELECT last_run_at FROM etl_job_checkpoint WHERE job_name = '"
                + jobName + "'");
        return value == null ? null : ((Number) value).longValue();
    }

    /**
     * 造作业描述（yml 文本 → 描述对象）。
     *
     * @param jobName        作业名
     * @param tiebreakColumn 平局列（null = 单键游标）
     * @param overlap        回溯窗口（秒；0 = 关闭）
     * @param onOutOfRange   越界策略（FAIL / RESET）
     * @param pageSize       源侧分页大小
     * @return 作业描述
     */
    static JobDescriptor job(String jobName, String tiebreakColumn, long overlap,
                             String onOutOfRange, int pageSize) {
        StringBuilder yml = new StringBuilder();
        yml.append("job:\n")
                .append("  name: ").append(jobName).append("\n")
                .append("  mode: auto\n")
                .append("  source:\n")
                .append("    dataSourceRef: ").append(SOURCE_REF).append("\n")
                .append("    table: ").append(SOURCE_TABLE).append("\n")
                .append("    columns: [id, order_no, user_id, amount, status, updated_at]\n")
                .append("    watermarkColumn: updated_at\n")
                .append("    pageSize: ").append(pageSize).append("\n")
                .append("  transform:\n")
                .append("    fieldMappings:\n")
                .append("      - {source: id,         target: order_id,   type: LONG}\n")
                .append("      - {source: order_no,   target: order_no,   type: STRING}\n")
                .append("      - {source: user_id,    target: user_id,    type: LONG}\n")
                .append("      - {source: amount,     target: amount,     type: BIGDECIMAL}\n")
                .append("      - {source: status,     target: status,     type: INT}\n")
                .append("      - {source: updated_at, target: updated_at, type: TIMESTAMP}\n")
                .append("  target:\n")
                .append("    dataSourceRef: ").append(TARGET_REF).append("\n")
                .append("    table: ").append(TARGET_TABLE).append("\n")
                .append("    batchSize: 100\n")
                .append("    writeMode: upsert\n")
                .append("  idempotencyKey: [order_id]\n")
                .append("  watermark:\n")
                .append("    column: updated_at\n")
                .append("    type: TIMESTAMP\n");
        if (tiebreakColumn != null) {
            yml.append("    tiebreakColumn: ").append(tiebreakColumn).append("\n");
        }
        if (overlap > 0L) {
            yml.append("    overlap: ").append(overlap).append("\n");
        }
        if (onOutOfRange != null) {
            yml.append("    onOutOfRange: ").append(onOutOfRange).append("\n");
        }
        return JobDescriptorParser.parse(yml.toString());
    }

    /** 造作业描述（默认口径：复合游标 + FAIL 策略 + 分页 100）。 */
    static JobDescriptor job(String jobName) {
        return job(jobName, "id", 0L, "FAIL", 100);
    }

    private static void exec(DataSource dataSource, String ddl) throws SQLException {
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
