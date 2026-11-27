package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.connector.DriverUrlDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据源登记仓储（落库口径：{@code etl_datasource}，见 {@link EtlSchema}）。
 *
 * <p>数据源管理页的读写面：登记 / 更新 / 删除 / 列表 + 「测试连接」。
 * 本仓储返回**全量行**（含密码，内部层）；密码掩码口径在 web 层统一做，不出网。
 * 带密码的 {@link #find(String)} 只供引擎侧 {@code DataSourceProvider} 解析用。
 *
 * @author zifang
 */
public class JdbcDataSourceRegistry {

    private final DataSource dataSource;

    public JdbcDataSourceRegistry(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 列表（不含密码）。 */
    public List<DataSourceDef> list() {
        List<DataSourceDef> defs = new ArrayList<DataSourceDef>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT ds_name, ds_type, url, username, password,"
                    + " description, created_at, updated_at FROM " + EtlSchema.TABLE_DATASOURCE + " ORDER BY ds_name");
            rs = statement.executeQuery();
            while (rs.next()) {
                DataSourceDef def = new DataSourceDef();
                def.setName(rs.getString(1));
                def.setType(rs.getString(2));
                def.setUrl(rs.getString(3));
                def.setUsername(rs.getString(4));
                def.setPassword(rs.getString(5));
                def.setDescription(rs.getString(6));
                def.setCreatedAt(rs.getLong(7));
                def.setUpdatedAt(rs.getLong(8));
                defs.add(def);
            }
            return defs;
        } catch (SQLException e) {
            throw new IllegalStateException("列出数据源失败: " + e.getMessage(), e);
        } finally {
            close(rs, statement, connection);
        }
    }

    /** 单条（含密码；只供引擎侧解析，不直接出网）。 */
    public DataSourceDef find(String name) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT ds_name, ds_type, url, username, password,"
                    + " description, created_at, updated_at FROM " + EtlSchema.TABLE_DATASOURCE
                    + " WHERE ds_name = ?");
            statement.setString(1, name);
            rs = statement.executeQuery();
            if (!rs.next()) {
                return null;
            }
            DataSourceDef def = new DataSourceDef();
            def.setName(rs.getString(1));
            def.setType(rs.getString(2));
            def.setUrl(rs.getString(3));
            def.setUsername(rs.getString(4));
            def.setPassword(rs.getString(5));
            def.setDescription(rs.getString(6));
            def.setCreatedAt(rs.getLong(7));
            def.setUpdatedAt(rs.getLong(8));
            return def;
        } catch (SQLException e) {
            throw new IllegalStateException("读取数据源失败: " + e.getMessage(), e);
        } finally {
            close(rs, statement, connection);
        }
    }

    /**
     * 登记 / 更新（upsert）。
     *
     * @param keepPasswordOnBlank true 且新密码为空时保留旧密码（编辑页不重输密码的口径）
     */
    public void save(DataSourceDef def, boolean keepPasswordOnBlank) {
        DataSourceDef existing = find(def.getName());
        if (existing != null && keepPasswordOnBlank
                && (def.getPassword() == null || def.getPassword().isEmpty())) {
            def.setPassword(existing.getPassword());
        }
        Connection connection = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        try {
            connection = dataSource.getConnection();
            long now = System.currentTimeMillis();
            if (existing == null) {
                def.setCreatedAt(now);
                def.setUpdatedAt(now);
                insert = connection.prepareStatement("INSERT INTO " + EtlSchema.TABLE_DATASOURCE
                        + " (ds_name, ds_type, url, username, password, description, created_at, updated_at)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
                insert.setString(1, def.getName());
                insert.setString(2, def.getType());
                insert.setString(3, def.getUrl());
                insert.setString(4, def.getUsername());
                insert.setString(5, def.getPassword());
                insert.setString(6, def.getDescription());
                insert.setLong(7, def.getCreatedAt());
                insert.setLong(8, def.getUpdatedAt());
                insert.executeUpdate();
                return;
            }
            def.setCreatedAt(existing.getCreatedAt());
            def.setUpdatedAt(now);
            update = connection.prepareStatement("UPDATE " + EtlSchema.TABLE_DATASOURCE
                    + " SET ds_type = ?, url = ?, username = ?, password = ?, description = ?, updated_at = ?"
                    + " WHERE ds_name = ?");
            update.setString(1, def.getType());
            update.setString(2, def.getUrl());
            update.setString(3, def.getUsername());
            update.setString(4, def.getPassword());
            update.setString(5, def.getDescription());
            update.setLong(6, def.getUpdatedAt());
            update.setString(7, def.getName());
            update.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("数据源登记失败: " + e.getMessage(), e);
        } finally {
            close(null, update, null);
            close(null, insert, null);
            close(null, null, connection);
        }
    }

    /** 删除（被流水线引用时引擎侧自然报「数据源无法解析」，这里不做引用检查）。 */
    public boolean delete(String name) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("DELETE FROM " + EtlSchema.TABLE_DATASOURCE
                    + " WHERE ds_name = ?");
            statement.setString(1, name);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("删除数据源失败: " + e.getMessage(), e);
        } finally {
            close(null, statement, connection);
        }
    }

    /**
     * 测试连接：实际握手一次。
     *
     * @return null = 连接成功；否则错误摘要
     */
    public static String testConnection(DataSourceDef def, int timeoutSeconds) {
        DriverUrlDataSource probe = new DriverUrlDataSource(def.getUrl(), def.getUsername(), def.getPassword());
        try {
            Connection connection = probe.getConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    statement.execute("SELECT 1");
                } finally {
                    statement.close();
                }
            } finally {
                connection.close();
            }
            return null;
        } catch (SQLException e) {
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            return message.length() > 300 ? message.substring(0, 300) + "…" : message;
        } catch (Exception e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

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
}
