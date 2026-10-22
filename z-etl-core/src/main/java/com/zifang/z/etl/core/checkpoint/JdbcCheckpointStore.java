package com.zifang.z.etl.core.checkpoint;

import com.zifang.z.etl.core.engine.EtlSchema;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC 版水位存储（落库口径：{@code etl_job_checkpoint}）。
 *
 * <p>水位必须**持久化**才能断点续传（需求 008 验收：首跑全量 → 自动切增量，水位持久化 ⇒ 中断后断点续传）。
 *
 * @author zifang
 */
public class JdbcCheckpointStore implements CheckpointStore {

    private final DataSource dataSource;

    public JdbcCheckpointStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Checkpoint load(String jobName) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT job_name, watermark_column, watermark_type, "
                    + "watermark_value, tiebreak_column, tiebreak_value, last_mode, last_run_at FROM "
                    + EtlSchema.TABLE_JOB_CHECKPOINT + " WHERE job_name = ?");
            statement.setString(1, jobName);
            rs = statement.executeQuery();
            if (rs.next()) {
                return new Checkpoint(rs.getString("job_name"), rs.getString("watermark_column"),
                        rs.getString("watermark_type"), rs.getString("watermark_value"),
                        rs.getString("tiebreak_column"), rs.getString("tiebreak_value"),
                        rs.getString("last_mode"), rs.getLong("last_run_at"));
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException("读取水位失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public void save(Checkpoint checkpoint) {
        Connection connection = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        try {
            connection = dataSource.getConnection();
            // 可移植的 upsert：先 UPDATE，命中 0 行再 INSERT
            // （不用 MERGE —— MERGE INTO ... KEY() 只有 H2 有，MySQL/PG 都不认）
            update = connection.prepareStatement("UPDATE " + EtlSchema.TABLE_JOB_CHECKPOINT
                    + " SET watermark_column = ?, watermark_type = ?, watermark_value = ?, "
                    + "tiebreak_column = ?, tiebreak_value = ?, "
                    + "last_mode = ?, last_run_at = ? WHERE job_name = ?");
            update.setString(1, checkpoint.getWatermarkColumn());
            update.setString(2, checkpoint.getWatermarkType());
            update.setString(3, checkpoint.getWatermarkValue());
            update.setString(4, checkpoint.getTiebreakColumn());
            update.setString(5, checkpoint.getTiebreakValue());
            update.setString(6, checkpoint.getLastMode());
            update.setLong(7, checkpoint.getLastRunAt());
            update.setString(8, checkpoint.getJobName());
            if (update.executeUpdate() == 0) {
                insert = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_JOB_CHECKPOINT
                        + " (job_name, watermark_column, watermark_type, watermark_value,"
                        + " tiebreak_column, tiebreak_value, last_mode, last_run_at)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
                insert.setString(1, checkpoint.getJobName());
                insert.setString(2, checkpoint.getWatermarkColumn());
                insert.setString(3, checkpoint.getWatermarkType());
                insert.setString(4, checkpoint.getWatermarkValue());
                insert.setString(5, checkpoint.getTiebreakColumn());
                insert.setString(6, checkpoint.getTiebreakValue());
                insert.setString(7, checkpoint.getLastMode());
                insert.setLong(8, checkpoint.getLastRunAt());
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("保存水位失败: " + e.getMessage(), e);
        } finally {
            close(insert);
            close(update);
            close(connection);
        }
    }

    @Override
    public void clear(String jobName) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("DELETE FROM " + EtlSchema.TABLE_JOB_CHECKPOINT
                    + " WHERE job_name = ?");
            statement.setString(1, jobName);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("清除水位失败: " + e.getMessage(), e);
        } finally {
            close(statement);
            close(connection);
        }
    }

    private void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignore) {
                // 忽略关闭异常
            }
        }
    }
}
