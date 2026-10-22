package com.zifang.z.etl.core;

import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.nio.charset.Charset;

/**
 * 本地夹具：两个 H2 内存库（源库 + 目标库）+ 一个引擎元数据库。
 *
 * <p>⚠️ 硬边界（卡面）：验收项 1 的「搬运跑通 + 行数对账」必须在**本地/测试环境**完成，
 * 不得连生产 RDS、不得在 {@code oc} 库建表。因此：
 * <ul>
 *   <li>源表 {@code z_trade_order} 的**表结构照生产真实表形状**（z-opc/_doc/004_sql/z-trade.sql
 *       里那张表的字段集），但**数据是本地生成的**，一个字节都没碰生产库；</li>
 *   <li>源库与目标库是两个**独立的** H2 内存库 ⇒ 真的是「一张表搬到另一张表、跨数据源」。</li>
 * </ul>
 *
 * @author zifang
 */
public final class EtlTestFixtures {

    /** 源数据源引用名（对应 z-dms 的 DataSourceDef ref）。 */
    public static final String SOURCE_REF = "ds-oc-main";
    /** 目标数据源引用名。 */
    public static final String TARGET_REF = "ds-etl-landing";

    /** 源表 DDL：照生产 z_trade_order 的形状（字段集与类型对齐）。 */
    public static final String SOURCE_DDL =
            "CREATE TABLE IF NOT EXISTS z_trade_order ("
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

    /** 目标表 DDL：落地快照表，幂等键 order_id 上有主键约束（upsert 的前提）。 */
    public static final String TARGET_DDL =
            "CREATE TABLE IF NOT EXISTS etl_trade_order_snapshot ("
                    + "order_id BIGINT PRIMARY KEY, "
                    + "order_no VARCHAR(64) NOT NULL, "
                    + "user_id BIGINT, "
                    + "amount DECIMAL(18,2), "
                    + "status INT, "
                    + "updated_at TIMESTAMP)";

    /** 转换链目标表 DDL：字段映射 + 类型归一 + 表达式 + 脚本 四类算子的产物落点。 */
    public static final String TRANSFORM_TARGET_DDL =
            "CREATE TABLE IF NOT EXISTS etl_trade_order_transformed ("
                    + "order_id BIGINT PRIMARY KEY, "
                    + "order_no VARCHAR(64) NOT NULL, "
                    + "user_id BIGINT, "
                    + "amount DECIMAL(18,2), "
                    + "status BIGINT, "
                    + "status_text VARCHAR(32), "
                    + "gross_amount DECIMAL(18,4), "
                    + "tax_amount DECIMAL(18,4), "
                    + "payment_method VARCHAR(32), "
                    + "pay_channel VARCHAR(64), "
                    + "updated_at TIMESTAMP)";

    /** 聚合下推目标表 DDL：每行 = 一个分组。 */
    public static final String SUMMARY_TARGET_DDL =
            "CREATE TABLE IF NOT EXISTS etl_status_summary ("
                    + "status INT PRIMARY KEY, "
                    + "order_cnt BIGINT, "
                    + "total_amount DECIMAL(18,2), "
                    + "avg_amount DECIMAL(18,4))";

    private static final java.util.concurrent.atomic.AtomicInteger UNIQUE =
            new java.util.concurrent.atomic.AtomicInteger();

    private final DataSource source;
    private final DataSource target;
    private final DataSource meta;

    public EtlTestFixtures(String id) {
        // H2 内存库在同一 JVM 内跨用例存活（DB_CLOSE_DELAY=-1）⇒ 库名必须唯一，否则第二个用例撞主键
        String unique = id + "_" + UNIQUE.incrementAndGet();
        this.source = h2("etl_src_" + unique);
        this.target = h2("etl_dst_" + unique);
        this.meta = h2("etl_meta_" + unique);
    }

    /** 造一个 H2 内存库数据源（DB_CLOSE_DELAY=-1 ⇒ 同 JVM 内连接不回收）。 */
    public static DataSource h2(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    public DataSource source() {
        return source;
    }

    public DataSource target() {
        return target;
    }

    /** 引擎自身库（作业定义 / 水位 / 履历）。 */
    public DataSource meta() {
        return meta;
    }

    /** 建表：源表 + 目标表 + 引擎元表。 */
    public void createSchema() throws SQLException {
        execute(source, SOURCE_DDL);
        execute(target, TARGET_DDL);
    }

    /** 建表：源表 + 转换链目标表 + 聚合下推目标表（+ 引擎元表）。 */
    public void createTransformSchema() throws SQLException {
        execute(source, SOURCE_DDL);
        execute(target, TRANSFORM_TARGET_DDL);
        execute(target, SUMMARY_TARGET_DDL);
    }

    /**
     * 灌存量数据（本地生成，形状照生产订单表）。
     *
     * @param rows  行数
     * @param startId 起始主键
     * @param baseTime 起始时间戳（增量接力时往后推）
     */
    public void seedSource(int rows, long startId, Timestamp baseTime) throws SQLException {
        Connection connection = source.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO z_trade_order (id, order_no, user_id, user_name, product_code, amount,"
                            + " quantity, status, payment_method, created_at, updated_at)"
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
                    Timestamp ts = new Timestamp(baseTime.getTime() + i * 60000L);
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

    /** 从作业描述资源文件读描述（yml 形态）。 */
    public static JobDescriptor loadYamlDescriptor() {
        return JobDescriptorParser.parse(readResource("/jobs/trade-order-to-snapshot.yml"));
    }

    /** 从作业描述资源文件读描述（JSON 形态）。 */
    public static JobDescriptor loadJsonDescriptor() {
        return JobDescriptorParser.parse(readResource("/jobs/trade-order-to-snapshot.json"));
    }

    /** 转换链作业描述（yml 形态）。 */
    public static JobDescriptor loadTransformDescriptor() {
        return JobDescriptorParser.parse(readResource("/jobs/trade-order-transform.yml"));
    }

    /** 转换链作业描述（JSON 形态）。 */
    public static JobDescriptor loadTransformJsonDescriptor() {
        return JobDescriptorParser.parse(readResource("/jobs/trade-order-transform.json"));
    }

    /** 聚合下推作业描述。 */
    public static JobDescriptor loadSummaryDescriptor() {
        return JobDescriptorParser.parse(readResource("/jobs/trade-order-status-summary.yml"));
    }

    /**
     * 整表规范化导出（逐字段一致性比对用）。
     *
     * <p>口径：按 {@code orderBy} 列排序，把每行的所有列按「列名=值」拼成一行文本，
     * 全部行拼成一个字符串 —— 两次跑出来的字符串完全相同 ⟺ 逐字段一致。
     *
     * @param dataSource 数据源
     * @param table      表名
     * @param orderBy    排序列
     * @return 规范化文本
     */
    public static String dumpTable(DataSource dataSource, String table, String orderBy) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet rs = statement.executeQuery("SELECT * FROM " + table + " ORDER BY " + orderBy);
                try {
                    ResultSetMetaData meta = rs.getMetaData();
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        for (int i = 1; i <= meta.getColumnCount(); i++) {
                            Object value = rs.getObject(i);
                            sb.append(meta.getColumnLabel(i).toLowerCase()).append('=')
                                    .append(value == null ? "<null>" : value.toString()).append(';');
                        }
                        sb.append('\n');
                    }
                    return sb.toString();
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

    /** 某列的去重值个数（幂等键去重口径）。 */
    public static long countDistinct(DataSource dataSource, String table, String column) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet rs = statement.executeQuery(
                        "SELECT COUNT(DISTINCT " + column + ") FROM " + table);
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

    /** 读 classpath 资源为字符串。 */
    public static String readResource(String path) {
        InputStream in = EtlTestFixtures.class.getResourceAsStream(path);
        if (in == null) {
            throw new IllegalStateException("测试资源不存在: " + path);
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) > 0) {
                out.write(buffer, 0, read);
            }
            return new String(out.toByteArray(), Charset.forName("UTF-8"));
        } catch (IOException e) {
            throw new IllegalStateException("读取测试资源失败: " + path, e);
        } finally {
            try {
                in.close();
            } catch (IOException ignore) {
                // 忽略
            }
        }
    }

    /** 表行数。 */
    public static long count(DataSource dataSource, String table) throws SQLException {
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

    /** 取目标表里某一行的关键列值（验证字段映射与类型归一）。 */
    public static Object valueOf(DataSource dataSource, String table, String keyColumn, Object key,
                                 String column) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT " + column + " FROM " + table + " WHERE " + keyColumn + " = ?");
            try {
                statement.setObject(1, key);
                ResultSet rs = statement.executeQuery();
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

    private static void execute(DataSource dataSource, String ddl) throws SQLException {
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
