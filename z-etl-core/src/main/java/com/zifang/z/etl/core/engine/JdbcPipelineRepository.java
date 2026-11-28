package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.PipelineRunResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * JDBC 版流水线仓储（落库口径：{@code etl_pipeline_def} + {@code etl_pipeline_run}）。
 *
 * <p>与 {@link JdbcJobRepository} 同形状：同一份声明存 JSON（canonical，解析用）+ yml（人读）两份文本；
 * 可移植 upsert（先 UPDATE 命中 0 行再 INSERT）；履历按开始时间倒序。
 *
 * @author zifang
 */
public class JdbcPipelineRepository implements PipelineRepository {

    private final DataSource dataSource;

    public JdbcPipelineRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(PipelineSpec spec) {
        String json = PipelineParser.toJson(spec);
        String yml = PipelineParser.toYaml(spec);
        Connection connection = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        try {
            connection = dataSource.getConnection();
            long now = System.currentTimeMillis();
            update = connection.prepareStatement("UPDATE " + EtlSchema.TABLE_PIPELINE_DEF
                    + " SET description = ?, mode = ?, spec_json = ?, spec_yml = ?, updated_at = ?"
                    + " WHERE pipeline_name = ?");
            update.setString(1, spec.getDescription());
            update.setString(2, spec.getMode());
            update.setString(3, json);
            update.setString(4, yml);
            update.setLong(5, now);
            update.setString(6, spec.getName());
            if (update.executeUpdate() == 0) {
                insert = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_PIPELINE_DEF
                        + " (pipeline_name, description, mode, spec_json, spec_yml, created_at, updated_at)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?)");
                insert.setString(1, spec.getName());
                insert.setString(2, spec.getDescription());
                insert.setString(3, spec.getMode());
                insert.setString(4, json);
                insert.setString(5, yml);
                insert.setLong(6, now);
                insert.setLong(7, now);
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("流水线定义落库失败: " + e.getMessage(), e);
        } finally {
            close(insert);
            close(update);
            close(connection);
        }
    }

    @Override
    public PipelineSpec find(String name) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT spec_json FROM " + EtlSchema.TABLE_PIPELINE_DEF
                    + " WHERE pipeline_name = ?");
            statement.setString(1, name);
            rs = statement.executeQuery();
            if (rs.next()) {
                String json = rs.getString(1);
                return json == null ? null : PipelineParser.parseJson(json);
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException("读取流水线定义失败: " + e.getMessage(), e);
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
            statement = connection.prepareStatement("SELECT pipeline_name FROM "
                    + EtlSchema.TABLE_PIPELINE_DEF + " ORDER BY pipeline_name");
            rs = statement.executeQuery();
            while (rs.next()) {
                names.add(rs.getString(1));
            }
            return names;
        } catch (SQLException e) {
            throw new IllegalStateException("列出流水线失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public void saveRun(PipelineRunResult result) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_PIPELINE_RUN
                    + " (run_id, pipeline_name, mode, status, records_read, records_written,"
                    + " watermark_before, watermark_after, restored, notes, started_at, finished_at)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            String runId = result.getRunId() == null || result.getRunId().isEmpty()
                    ? UUID.randomUUID().toString() : result.getRunId();
            result.setRunId(runId);
            statement.setString(1, runId);
            statement.setString(2, result.getPipelineName());
            statement.setString(3, result.getMode());
            statement.setString(4, result.getStatus() == null ? PipelineRunResult.STATUS_SUCCESS : result.getStatus());
            statement.setLong(5, result.getRecordsRead());
            statement.setLong(6, result.getRecordsWritten());
            statement.setString(7, result.getWatermarkBefore());
            statement.setString(8, result.getWatermarkAfter());
            statement.setInt(9, result.isRestored() ? 1 : 0);
            statement.setString(10, String.join("；", result.getNotes()));
            statement.setLong(11, result.getStartedAt());
            statement.setLong(12, result.getFinishedAt());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("流水线运行履历落库失败: " + e.getMessage(), e);
        } finally {
            close(statement);
            close(connection);
        }
    }

    @Override
    public List<PipelineRunResult> runHistory(String name, int limit) {
        List<PipelineRunResult> history = new ArrayList<PipelineRunResult>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            String sql = "SELECT run_id, pipeline_name, mode, status, records_read, records_written,"
                    + " watermark_before, watermark_after, restored, notes, started_at, finished_at"
                    + " FROM " + EtlSchema.TABLE_PIPELINE_RUN + " WHERE pipeline_name = ?"
                    + " ORDER BY started_at DESC";
            if (limit > 0) {
                // 不可移植的 LIMIT 子句由方言差异兜底：履历行数有限，取回后截断
                sql += " LIMIT " + Math.abs(limit);
            }
            statement = connection.prepareStatement(sql);
            statement.setString(1, name);
            rs = statement.executeQuery();
            while (rs.next()) {
                history.add(mapRunRow(rs));
            }
            return history;
        } catch (SQLException e) {
            throw new IllegalStateException("读取流水线运行履历失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
    }

    @Override
    public List<PipelineRunResult> recentRuns(int limit) {
        List<PipelineRunResult> runs = new ArrayList<PipelineRunResult>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            String sql = "SELECT run_id, pipeline_name, mode, status, records_read, records_written,"
                    + " watermark_before, watermark_after, restored, notes, started_at, finished_at"
                    + " FROM " + EtlSchema.TABLE_PIPELINE_RUN + " ORDER BY started_at DESC";
            if (limit > 0) {
                sql += " LIMIT " + Math.abs(limit);
            }
            statement = connection.prepareStatement(sql);
            rs = statement.executeQuery();
            while (rs.next()) {
                runs.add(mapRunRow(rs));
            }
            return runs;
        } catch (SQLException e) {
            throw new IllegalStateException("读取跨流水线最近运行失败: " + e.getMessage(), e);
        } finally {
            close(rs, statement, connection);
        }
    }

    @Override
    public boolean delete(String name) {
        if (name == null || name.trim().isEmpty()) {
            return false;
        }
        Connection connection = null;
        PreparedStatement deleteRun = null;
        PreparedStatement deleteDef = null;
        try {
            connection = dataSource.getConnection();
            deleteRun = connection.prepareStatement("DELETE FROM " + EtlSchema.TABLE_PIPELINE_RUN
                    + " WHERE pipeline_name = ?");
            deleteRun.setString(1, name);
            deleteRun.executeUpdate();
            deleteDef = connection.prepareStatement("DELETE FROM " + EtlSchema.TABLE_PIPELINE_DEF
                    + " WHERE pipeline_name = ?");
            deleteDef.setString(1, name);
            return deleteDef.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("删除流水线失败: " + e.getMessage(), e);
        } finally {
            close(deleteDef);
            close(deleteRun);
            close(connection);
        }
    }

    /** 履历行 → 运行结果（runHistory / recentRuns 共用）。 */
    private static PipelineRunResult mapRunRow(ResultSet rs) throws SQLException {
        PipelineRunResult result = new PipelineRunResult();
        result.setRunId(rs.getString(1));
        result.setPipelineName(rs.getString(2));
        result.setMode(rs.getString(3));
        String status = rs.getString(4);
        result.setStatus(status == null || status.isEmpty()
                ? PipelineRunResult.STATUS_SUCCESS : status);
        result.setRecordsRead(rs.getLong(5));
        result.setRecordsWritten(rs.getLong(6));
        result.setWatermarkBefore(rs.getString(7));
        result.setWatermarkAfter(rs.getString(8));
        result.setRestored(rs.getInt(9) == 1);
        String notes = rs.getString(10);
        if (notes != null && !notes.isEmpty()) {
            for (String note : notes.split("；")) {
                result.addNote(note);
            }
        }
        result.setStartedAt(rs.getLong(11));
        result.setFinishedAt(rs.getLong(12));
        return result;
    }

    /** 三件套关闭（recentRuns 用）。 */
    private static void close(ResultSet rs, Statement statement, Connection connection) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ignore) {
                // 关闭失败不影响主流程
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ignore) {
                // 关闭失败不影响主流程
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignore) {
                // 关闭失败不影响主流程
            }
        }
    }

    private void close(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignore) {
            // 关闭失败不影响主流程
        }
    }
}
