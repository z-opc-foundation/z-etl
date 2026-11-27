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
    /** 流水线定义表（DAG 声明落库处）。 */
    public static final String TABLE_PIPELINE_DEF = "etl_pipeline_def";
    /** 流水线状态快照表（键控状态序列化，跨重启恢复）。 */
    public static final String TABLE_PIPELINE_STATE = "etl_pipeline_state";
    /** 流水线运行履历表。 */
    public static final String TABLE_PIPELINE_RUN = "etl_pipeline_run";
    /** 消息队列表（默认消息总线实现：落库当队列）。 */
    public static final String TABLE_MESSAGE_OUTBOX = "etl_message_outbox";
    /** 流水线调度登记表（cron 表达式 + 上次触发时间）。 */
    public static final String TABLE_PIPELINE_SCHEDULE = "etl_pipeline_schedule";
    /** 数据源登记表（数据源管理页；凭据属运行时数据，凭据管控最终归 z-dms/z-mist）。 */
    public static final String TABLE_DATASOURCE = "etl_datasource";

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
                    + "finished_at BIGINT)",
            "CREATE TABLE IF NOT EXISTS " + TABLE_PIPELINE_DEF + " ("
                    + "pipeline_name VARCHAR(128) PRIMARY KEY, "
                    + "description VARCHAR(512), "
                    + "mode VARCHAR(32), "
                    + "spec_json TEXT, "
                    + "spec_yml TEXT, "
                    + "created_at BIGINT, "
                    + "updated_at BIGINT)",
            // 状态快照存 base64 文本而不是 BLOB：PG 没有 BLOB 类型名，TEXT 三方言通吃
            "CREATE TABLE IF NOT EXISTS " + TABLE_PIPELINE_STATE + " ("
                    + "pipeline_name VARCHAR(128) PRIMARY KEY, "
                    + "state TEXT, "
                    + "updated_at BIGINT)",
            "CREATE TABLE IF NOT EXISTS " + TABLE_PIPELINE_RUN + " ("
                    + "run_id VARCHAR(64) PRIMARY KEY, "
                    + "pipeline_name VARCHAR(128), "
                    + "mode VARCHAR(32), "
                    + "status VARCHAR(16), "
                    + "records_read BIGINT, "
                    + "records_written BIGINT, "
                    + "watermark_before VARCHAR(256), "
                    + "watermark_after VARCHAR(256), "
                    + "restored INT, "
                    + "notes TEXT, "
                    + "started_at BIGINT, "
                    + "finished_at BIGINT)",
            // 消息队列（默认消息总线）：发出即落行，消费标记 consumed=1
            "CREATE TABLE IF NOT EXISTS " + TABLE_MESSAGE_OUTBOX + " ("
                    + "message_id VARCHAR(64) PRIMARY KEY, "
                    + "topic VARCHAR(128), "
                    + "msg_key VARCHAR(256), "
                    + "payload TEXT, "
                    + "created_at BIGINT, "
                    + "consumed INT)",
            // 调度登记：z-etl 只登记 cron，定时器由宿主驱动
            "CREATE TABLE IF NOT EXISTS " + TABLE_PIPELINE_SCHEDULE + " ("
                    + "pipeline_name VARCHAR(128) PRIMARY KEY, "
                    + "cron_expression VARCHAR(128), "
                    + "enabled INT, "
                    + "last_triggered_at BIGINT, "
                    + "created_at BIGINT, "
                    + "updated_at BIGINT)",
            // 数据源登记：数据源管理页的落库处；password 只落库不出网（列表/详情一律掩码）
            "CREATE TABLE IF NOT EXISTS " + TABLE_DATASOURCE + " ("
                    + "ds_name VARCHAR(128) PRIMARY KEY, "
                    + "ds_type VARCHAR(32), "
                    + "url VARCHAR(512), "
                    + "username VARCHAR(128), "
                    + "password VARCHAR(256), "
                    + "description VARCHAR(512), "
                    + "created_at BIGINT, "
                    + "updated_at BIGINT)"
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
            "ALTER TABLE " + TABLE_JOB_CHECKPOINT + " ADD COLUMN tiebreak_value VARCHAR(256)",
            // 运行总览（046）：失败运行也落履历，老库补 status 列（旧行缺省当 SUCCESS）
            "ALTER TABLE " + TABLE_PIPELINE_RUN + " ADD COLUMN status VARCHAR(16)"
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
