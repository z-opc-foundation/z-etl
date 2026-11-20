package com.zifang.z.etl.core.runtime;

import com.zifang.z.etl.core.engine.EtlSchema;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;

/**
 * JDBC 版状态快照存储（落 {@code etl_pipeline_state}，跨重启恢复）。
 *
 * <p>快照字节存 **base64 文本** 而不是 BLOB：PG 没有 BLOB 类型名，TEXT 三方言（mysql/pg/h2）通吃。
 * 可移植 upsert（先 UPDATE 命中 0 行再 INSERT），与 {@code JdbcJobRepository} 同形状。
 *
 * @author zifang
 */
public class JdbcPipelineStateStore implements PipelineStateStore {

    private final DataSource dataSource;

    public JdbcPipelineStateStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public byte[] load(String id) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(
                    "SELECT state FROM " + EtlSchema.TABLE_PIPELINE_STATE + " WHERE pipeline_name = ?");
            statement.setString(1, id);
            rs = statement.executeQuery();
            if (rs.next()) {
                String text = rs.getString(1);
                return text == null ? null : Base64.getDecoder().decode(text);
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException("状态快照读取失败: " + e.getMessage(), e);
        } finally {
            close(rs, statement, connection);
        }
    }

    @Override
    public void save(String id, byte[] state) {
        String text = Base64.getEncoder().encodeToString(state);
        Connection connection = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        try {
            connection = dataSource.getConnection();
            update = connection.prepareStatement("UPDATE etl_pipeline_state"
                    + " SET state = ?, updated_at = ? WHERE pipeline_name = ?");
            update.setString(1, text);
            update.setLong(2, System.currentTimeMillis());
            update.setString(3, id);
            if (update.executeUpdate() == 0) {
                insert = connection.prepareStatement("INSERT INTO etl_pipeline_state"
                        + " (pipeline_name, state, updated_at) VALUES (?, ?, ?)");
                insert.setString(1, id);
                insert.setString(2, text);
                insert.setLong(3, System.currentTimeMillis());
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("状态快照落库失败: " + e.getMessage(), e);
        } finally {
            close(null, insert, null);
            close(null, update, connection);
        }
    }

    @Override
    public void clear(String id) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(
                    "DELETE FROM etl_pipeline_state WHERE pipeline_name = ?");
            statement.setString(1, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("状态快照清除失败: " + e.getMessage(), e);
        } finally {
            close(null, statement, connection);
        }
    }

    /** 快照是否存在（可观测性用）。 */
    public boolean contains(String id) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(
                    "SELECT 1 FROM etl_pipeline_state WHERE pipeline_name = ?");
            statement.setString(1, id);
            rs = statement.executeQuery();
            return rs.next();
        } finally {
            close(rs, statement, connection);
        }
    }

    private static void close(ResultSet rs, PreparedStatement statement, Connection connection) {
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
}
