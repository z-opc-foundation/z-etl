package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.connector.DriverUrlDataSource;
import com.zifang.util.db.support.Identifiers;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据源探活之外的**结构探查**：表清单与样例行。
 *
 * <p>门户 ETL 台账要「选表 → 看列 → 预览首行」，这两件事以前只有未来的参照实现有。
 * 本仓把这能力放回我们自己的 z-etl：凭据留在 z-etl 侧（数据源登记表），
 * 门户与 z-kb 都不接触明文密码，只拿结构与样例数据。</p>
 *
 * @author zifang
 */
public class DataSourceIntrospector {

    /** 表清单上限（防止大库一次拉爆）。 */
    private static final int MAX_TABLES = 200;

    /**
     * 表清单（表名 + 列名/列类型）。
     *
     * @param def 数据源定义（含凭据）
     * @return 表清单；查不到返回空列表
     */
    public List<Map<String, Object>> tables(DataSourceDef def) {
        List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
        if (def == null || def.getUrl() == null || def.getUrl().trim().isEmpty()) {
            return tables;
        }
        Connection connection = null;
        try {
            connection = new DriverUrlDataSource(def.getUrl(), def.getUsername(), def.getPassword())
                    .getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tablesRs = null;
            try {
                tablesRs = metaData.getTables(null, null, "%", new String[] {"TABLE", "VIEW"});
                while (tablesRs.next() && tables.size() < MAX_TABLES) {
                    String schema = tablesRs.getString("TABLE_SCHEM");
                    String table = tablesRs.getString("TABLE_NAME");
                    if (table == null || table.trim().isEmpty()) {
                        continue;
                    }
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("schema", schema);
                    item.put("name", table);
                    item.put("columns", columns(metaData, schema, table));
                    tables.add(item);
                }
            } finally {
                close(tablesRs);
            }
        } catch (Exception e) {
            // 探查失败＝拿不到结构，返回空清单（调用方据此提示「读不到结构」），不抛给作业流程
            return new ArrayList<Map<String, Object>>();
        } finally {
            close(connection);
        }
        return tables;
    }

    /**
     * 样例行。
     *
     * @param def 数据源定义（含凭据）
     * @param table 表名（可带 schema 前缀）
     * @param limit 行数上限
     * @return 样例行（每行一个 map，键为列名）
     */
    public List<Map<String, Object>> preview(DataSourceDef def, String table, int limit) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        if (def == null || def.getUrl() == null || def.getUrl().trim().isEmpty()
                || table == null || table.trim().isEmpty()) {
            return rows;
        }
        String[] parts = Identifiers.parts(table.trim());
        String quoted = null;
        for (String part : parts) {
            if (!Identifiers.isSafe(part)) {
                throw new IllegalArgumentException("表名不合法：" + table);
            }
        }
        if (parts.length == 2) {
            quoted = parts[0] + "." + parts[1];
        } else {
            quoted = parts[0];
        }
        int max = limit <= 0 ? 20 : Math.min(limit, 500);
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            connection = new DriverUrlDataSource(def.getUrl(), def.getUsername(), def.getPassword())
                    .getConnection();
            statement = connection.createStatement();
            statement.setMaxRows(max);
            rs = statement.executeQuery("SELECT * FROM " + quoted);
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                rows.add(row);
            }
        } catch (Exception e) {
            throw new IllegalStateException("读取样例行失败：" + e.getMessage(), e);
        } finally {
            close(rs);
            close(statement);
            close(connection);
        }
        return rows;
    }

    /** 一张表的列（列名 + 列类型）。 */
    private List<Map<String, Object>> columns(DatabaseMetaData metaData, String schema, String table) {
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        ResultSet rs = null;
        try {
            rs = metaData.getColumns(null, schema, table, "%");
            while (rs.next()) {
                Map<String, Object> column = new LinkedHashMap<String, Object>();
                column.put("name", rs.getString("COLUMN_NAME"));
                column.put("type", rs.getString("TYPE_NAME"));
                columns.add(column);
            }
        } catch (Exception ignore) {
            // 列读不到就当没有列信息
        } finally {
            close(rs);
        }
        return columns;
    }

    private void close(AutoCloseable closeable) {
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
