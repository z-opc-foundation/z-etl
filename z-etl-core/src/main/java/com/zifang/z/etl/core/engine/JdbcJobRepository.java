package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import com.zifang.z.etl.core.reconcile.ReconciliationReport;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 版作业仓储（落库口径：{@code etl_job_def} + {@code etl_job_run}）。
 *
 * <p>「job 描述 JSON/yml 可落库」：同一份描述存两份文本 —— canonical JSON（机器读，解析用）
 * 与 yml（人读/可粘贴回工单）。两者内容同源，落库后按作业名取回即可重跑。
 *
 * @author zifang
 */
public class JdbcJobRepository implements JobRepository {

    private final DataSource dataSource;

    public JdbcJobRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(JobDescriptor descriptor) {
        Connection connection = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        try {
            connection = dataSource.getConnection();
            long now = System.currentTimeMillis();
            // 可移植 upsert：先 UPDATE，命中 0 行再 INSERT
            update = connection.prepareStatement("UPDATE " + EtlSchema.TABLE_JOB_DEF
                    + " SET description = ?, mode = ?, descriptor_json = ?, descriptor_yml = ?, updated_at = ?"
                    + " WHERE job_name = ?");
            update.setString(1, descriptor.getDescription());
            update.setString(2, descriptor.getMode());
            update.setString(3, JobDescriptorParser.toJson(descriptor));
            update.setString(4, JobDescriptorParser.toYaml(descriptor));
            update.setLong(5, now);
            update.setString(6, descriptor.getName());
            if (update.executeUpdate() == 0) {
                insert = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_JOB_DEF
                        + " (job_name, description, mode, descriptor_json, descriptor_yml, created_at, updated_at)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?)");
                insert.setString(1, descriptor.getName());
                insert.setString(2, descriptor.getDescription());
                insert.setString(3, descriptor.getMode());
                insert.setString(4, JobDescriptorParser.toJson(descriptor));
                insert.setString(5, JobDescriptorParser.toYaml(descriptor));
                insert.setLong(6, now);
                insert.setLong(7, now);
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("作业定义落库失败: " + e.getMessage(), e);
        } finally {
            close(insert);
            close(update);
            close(connection);
        }
    }

    @Override
    public JobDescriptor find(String name) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT descriptor_json FROM " + EtlSchema.TABLE_JOB_DEF
                    + " WHERE job_name = ?");
            statement.setString(1, name);
            rs = statement.executeQuery();
            if (rs.next()) {
                String json = rs.getString(1);
                return json == null ? null : JobDescriptorParser.parseJson(json);
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException("读取作业定义失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public List<String> listNames() {
        List<String> names = new ArrayList<String>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT job_name FROM " + EtlSchema.TABLE_JOB_DEF
                    + " ORDER BY job_name");
            rs = statement.executeQuery();
            while (rs.next()) {
                names.add(rs.getString(1));
            }
            return names;
        } catch (SQLException e) {
            throw new IllegalStateException("列出作业失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public void recordRun(EtlRunResult result) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_JOB_RUN
                    + " (run_id, job_name, mode, rows_read, rows_written, source_count, target_count,"
                    + " source_checksum, target_checksum, watermark_before, watermark_after, reconciled,"
                    + " started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            statement.setString(1, result.getJobName() + "-" + result.getStartedAt());
            statement.setString(2, result.getJobName());
            statement.setString(3, result.getMode());
            statement.setLong(4, result.getRowsRead());
            statement.setLong(5, result.getRowsWritten());
            ReconciliationReport report = result.getReport();
            statement.setLong(6, report == null ? -1L : report.getSourceCount());
            statement.setLong(7, report == null ? -1L : report.getTargetCount());
            statement.setString(8, report == null ? null : report.getSourceChecksum());
            statement.setString(9, report == null ? null : report.getTargetChecksum());
            statement.setString(10, result.getWatermarkBefore());
            statement.setString(11, result.getWatermarkAfter());
            statement.setInt(12, result.isReconciled() ? 1 : 0);
            statement.setLong(13, result.getStartedAt());
            statement.setLong(14, result.getFinishedAt());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("记录运行履历失败: " + e.getMessage(), e);
        } finally {
            close(statement);
            close(connection);
        }
    }

    @Override
    public List<EtlRunResult> runHistory(String jobName, int limit) {
        List<EtlRunResult> history = new ArrayList<EtlRunResult>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            String sql = "SELECT * FROM " + EtlSchema.TABLE_JOB_RUN + " WHERE job_name = ? ORDER BY started_at DESC";
            if (limit > 0) {
                sql = sql + " LIMIT " + limit;
            }
            statement = connection.prepareStatement(sql);
            statement.setString(1, jobName);
            rs = statement.executeQuery();
            while (rs.next()) {
                EtlRunResult result = new EtlRunResult();
                result.setJobName(rs.getString("job_name"));
                result.setMode(rs.getString("mode"));
                result.setRowsRead(rs.getLong("rows_read"));
                result.setRowsWritten(rs.getLong("rows_written"));
                result.setWatermarkBefore(rs.getString("watermark_before"));
                result.setWatermarkAfter(rs.getString("watermark_after"));
                result.setStartedAt(rs.getLong("started_at"));
                result.setFinishedAt(rs.getLong("finished_at"));
                result.setReconciled(rs.getInt("reconciled") == 1);
                history.add(result);
            }
            return history;
        } catch (SQLException e) {
            throw new IllegalStateException("读取运行履历失败: " + e.getMessage(), e);
        } finally {
            close(rs);
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
