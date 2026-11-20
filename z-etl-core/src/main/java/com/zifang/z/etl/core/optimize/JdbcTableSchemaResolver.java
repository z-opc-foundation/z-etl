package com.zifang.z.etl.core.optimize;

import com.zifang.util.db.support.Identifiers;
import com.zifang.z.etl.core.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 基于 JDBC 元数据的表结构解析。
 *
 * <p>口径：查不到（表不存在 / 权限不足 / 驱动不支持）⇒ 返回 {@code null}（= 不知道），
 * 由优化器按保守口径处理；本解析器**不抛异常**（元数据探测失败不该把作业带崩）。
 *
 * @author zifang
 */
public class JdbcTableSchemaResolver implements TableSchemaResolver {

    private final DataSourceProvider provider;

    /**
     * @param provider 数据源解析
     */
    public JdbcTableSchemaResolver(DataSourceProvider provider) {
        this.provider = provider;
    }

    @Override
    public List<String> columnsOf(String dataSourceRef, String table) {
        if (provider == null || dataSourceRef == null || table == null || table.trim().isEmpty()) {
            return null;
        }
        DataSource dataSource;
        try {
            dataSource = provider.resolve(dataSourceRef);
        } catch (RuntimeException e) {
            return null;
        }
        if (dataSource == null) {
            return null;
        }
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            String[] parts = Identifiers.parts(table);
            String schema = parts.length == 2 ? parts[0] : null;
            String name = parts.length == 2 ? parts[1] : table;
            List<String> columns = readColumns(metaData, schema, name);
            if (columns == null) {
                // H2 之类把标识符折成大写 ⇒ 原名查不到时再按大写试一次
                columns = readColumns(metaData, schema, name.toUpperCase());
            }
            if (columns == null) {
                columns = readColumns(metaData, schema, name.toLowerCase());
            }
            return columns;
        } catch (Exception e) {
            return null;
        } finally {
            closeQuietly(connection);
        }
    }

    /** 读一张表的列；查不到返回 {@code null}。 */
    private List<String> readColumns(DatabaseMetaData metaData, String schema, String table) {
        ResultSet rs = null;
        try {
            rs = metaData.getColumns(null, schema, table, "%");
            Set<String> columns = new LinkedHashSet<String>();
            while (rs.next()) {
                String column = rs.getString("COLUMN_NAME");
                if (column != null && !column.trim().isEmpty()) {
                    columns.add(column.trim().toLowerCase());
                }
            }
            return columns.isEmpty() ? null : new ArrayList<String>(columns);
        } catch (Exception e) {
            return null;
        } finally {
            closeQuietly(rs);
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignore) {
            // 忽略
        }
    }
}
