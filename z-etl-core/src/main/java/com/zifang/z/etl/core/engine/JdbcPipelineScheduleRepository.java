package com.zifang.z.etl.core.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 调度登记仓储（落 {@code etl_pipeline_schedule}）。
 *
 * @author zifang
 */
public class JdbcPipelineScheduleRepository implements PipelineScheduleRepository {

    private final DataSource dataSource;

    public JdbcPipelineScheduleRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(PipelineSchedule schedule) {
        Connection connection = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        try {
            connection = dataSource.getConnection();
            long now = System.currentTimeMillis();
            update = connection.prepareStatement("UPDATE " + EtlSchema.TABLE_PIPELINE_SCHEDULE
                    + " SET cron_expression = ?, enabled = ?, updated_at = ? WHERE pipeline_name = ?");
            update.setString(1, schedule.getCronExpression());
            update.setInt(2, schedule.isEnabled() ? 1 : 0);
            update.setLong(3, now);
            update.setString(4, schedule.getPipelineName());
            if (update.executeUpdate() == 0) {
                insert = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_PIPELINE_SCHEDULE
                        + " (pipeline_name, cron_expression, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?)");
                insert.setString(1, schedule.getPipelineName());
                insert.setString(2, schedule.getCronExpression());
                insert.setInt(3, schedule.isEnabled() ? 1 : 0);
                insert.setLong(4, now);
                insert.setLong(5, now);
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("调度登记落库失败: " + e.getMessage(), e);
        } finally {
            close(insert);
            close(update);
            close(connection);
        }
    }

    @Override
    public PipelineSchedule find(String pipelineName) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT * FROM " + EtlSchema.TABLE_PIPELINE_SCHEDULE
                    + " WHERE pipeline_name = ?");
            statement.setString(1, pipelineName);
            rs = statement.executeQuery();
            return rs.next() ? read(rs) : null;
        } catch (SQLException e) {
            throw new IllegalStateException("读取调度登记失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public List<PipelineSchedule> list() {
        List<PipelineSchedule> schedules = new ArrayList<PipelineSchedule>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT * FROM " + EtlSchema.TABLE_PIPELINE_SCHEDULE
                    + " ORDER BY pipeline_name");
            rs = statement.executeQuery();
            while (rs.next()) {
                schedules.add(read(rs));
            }
            return schedules;
        } catch (SQLException e) {
            throw new IllegalStateException("列出调度登记失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public boolean remove(String pipelineName) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("DELETE FROM " + EtlSchema.TABLE_PIPELINE_SCHEDULE
                    + " WHERE pipeline_name = ?");
            statement.setString(1, pipelineName);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("取消调度失败: " + e.getMessage(), e);
        } finally {
            close(statement);
            close(connection);
        }
    }

    /** 结果集 → 登记对象。 */
    private PipelineSchedule read(ResultSet rs) throws SQLException {
        PipelineSchedule schedule = new PipelineSchedule();
        schedule.setPipelineName(rs.getString("pipeline_name"));
        schedule.setCronExpression(rs.getString("cron_expression"));
        schedule.setEnabled(rs.getInt("enabled") == 1);
        long lastTriggered = rs.getLong("last_triggered_at");
        schedule.setLastTriggeredAt(rs.wasNull() ? null : Long.valueOf(lastTriggered));
        long created = rs.getLong("created_at");
        schedule.setCreatedAt(rs.wasNull() ? null : Long.valueOf(created));
        long updated = rs.getLong("updated_at");
        schedule.setUpdatedAt(rs.wasNull() ? null : Long.valueOf(updated));
        return schedule;
    }

    /** 记录一次触发（宿主调触发口时更新「上次触发时间」）。 */
    @Override
    public void touch(String pipelineName) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("UPDATE " + EtlSchema.TABLE_PIPELINE_SCHEDULE
                    + " SET last_triggered_at = ?, updated_at = ? WHERE pipeline_name = ?");
            long now = System.currentTimeMillis();
            statement.setLong(1, now);
            statement.setLong(2, now);
            statement.setString(3, pipelineName);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("更新触发时间失败: " + e.getMessage(), e);
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
