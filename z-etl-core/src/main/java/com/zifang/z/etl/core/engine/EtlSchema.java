package com.zifang.z.etl.core.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 引擎**自身**的库表定义（作业定义 / 水位 / 运行履历）。
 *
 * <p>注意与业务表的分工：本仓的「作业元数据库」与「被搬运的业务表」是两回事——
 * 前者由本仓自己建、自己管；后者是宿主/被管库的表，本仓**只读不建**（需求 008「不包含」里
 * 「不与 z-boot-datasource-starter 合并」是同一条边界）。
 *
 * <p>DDL 全部写成 {@code CREATE TABLE IF NOT EXISTS}，可重复执行。
 *
 * @author zifang
 */
public final class EtlSchema {

    /** 作业定义表（job 描述落库处）。 */
    public static final String TABLE_JOB_DEF = "etl_job_def";
    /** 水位表。 */
    public static final String TABLE_JOB_CHECKPOINT = "etl_job_checkpoint";
    /** 运行履历表。 */
    public static final String TABLE_JOB_RUN = "etl_job_run";

    private static final String[] DDL = new String[]{
            "CREATE TABLE IF NOT EXISTS " + TABLE_JOB_DEF + " ("
                    + "job_name VARCHAR(128) PRIMARY KEY, "
                    + "description VARCHAR(512), "
                    + "mode VARCHAR(32), "
                    + "descriptor_json TEXT, "
                    + "descriptor_yml TEXT, "
                    + "created_at BIGINT, "
                    + "updated_at BIGINT)",
            "CREATE TABLE IF NOT EXISTS " + TABLE_JOB_CHECKPOINT + " ("
                    + "job_name VARCHAR(128) PRIMARY KEY, "
                    + "watermark_column VARCHAR(128), "
                    + "watermark_type VARCHAR(32), "
                    + "watermark_value VARCHAR(256), "
                    + "tiebreak_column VARCHAR(128), "
                    + "tiebreak_value VARCHAR(256), "
                    + "last_mode VARCHAR(32), "
                    + "last_run_at BIGINT)",
            "CREATE TABLE IF NOT EXISTS " + TABLE_JOB_RUN + " ("
                    + "run_id VARCHAR(64) PRIMARY KEY, "
                    + "job_name VARCHAR(128), "
                    + "mode VARCHAR(32), "
                    + "rows_read BIGINT, "
                    + "rows_written BIGINT, "
                    + "source_count BIGINT, "
                    + "target_count BIGINT, "
                    + "source_checksum VARCHAR(64), "
                    + "target_checksum VARCHAR(64), "
                    + "watermark_before VARCHAR(256), "
                    + "watermark_after VARCHAR(256), "
                    + "reconciled INT, "
                    + "started_at BIGINT, "
                    + "finished_at BIGINT)"
    };

    /**
     * 已存在的引擎库补列（复合游标的两列）。
     *
     * <p>{@code CREATE TABLE IF NOT EXISTS} 对**已建好的表**不会补列 ⇒ 老库升级时这两列会缺。
     * 这里做一次「补列」：{@code ALTER TABLE ... ADD COLUMN}，列已存在时忽略。
     * 方言差异：MySQL 报 1060「Duplicate column name」，H2 / PG 报「... already exists」——
     * 两种措辞都归到「已存在」这一类，不当成失败。
     */
    private static final String[] COLUMN_ADDITIONS = new String[]{
            "ALTER TABLE " + TABLE_JOB_CHECKPOINT + " ADD COLUMN tiebreak_column VARCHAR(128)",
            "ALTER TABLE " + TABLE_JOB_CHECKPOINT + " ADD COLUMN tiebreak_value VARCHAR(256)"
    };

    private EtlSchema() {
    }

    /**
     * 建表（幂等，可重复调用）。
     *
     * @param dataSource 引擎自身库数据源
     * @throws SQLException 建表失败
     */
    public static void ensure(javax.sql.DataSource dataSource) throws SQLException {
        java.sql.Connection connection = dataSource.getConnection();
        try {
            ensure(connection);
        } finally {
            connection.close();
        }
    }

    /**
     * 建表（幂等，可重复调用）。
     *
     * @param connection 引擎自身库连接
     * @throws SQLException 建表失败
     */
    public static void ensure(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            for (String ddl : DDL) {
                statement.execute(ddl);
            }
        } finally {
            statement.close();
        }
        for (String alter : COLUMN_ADDITIONS) {
            try {
                Statement addColumn = connection.createStatement();
                try {
                    addColumn.execute(alter);
                } finally {
                    addColumn.close();
                }
            } catch (SQLException e) {
                if (!isColumnAlreadyExists(e)) {
                    throw e;
                }
            }
        }
    }

    /** 「列已存在」类错误不算失败（老库补列口径）。 */
    private static boolean isColumnAlreadyExists(SQLException e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
        return message.contains("already exist") || message.contains("duplicate column")
                || message.contains("duplicate column name");
    }
}
