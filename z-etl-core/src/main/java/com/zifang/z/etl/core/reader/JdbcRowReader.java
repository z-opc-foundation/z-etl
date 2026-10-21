package com.zifang.z.etl.core.reader;

import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.util.db.support.Identifiers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC 取数器：分页游标 + 水位过滤。
 *
 * <p>分页用 {@code LIMIT ? OFFSET ?} —— 三方言（mysql / postgres / h2）同写法，不需要逐方言分叉
 * （需求 008 notes.md §9.8）。全量模式下逐页物化，**内存上限 = pageSize**，不随表长大。
 *
 * <p>增量模式在 where 上叠加水位条件 {@code 水位列 > ?}（D3 起步口径：水位轮询 + 断点续传）。
 *
 * @author zifang
 */
public class JdbcRowReader implements RowReader {

    private final DataSource dataSource;
    private final SourceSpec spec;
    private final String watermarkColumn;
    private final Object watermarkValue;
    private final String watermarkType;

    /**
     * @param dataSource       源数据源
     * @param spec             源定义
     * @param watermarkColumn  水位列（增量模式用；全量模式传 null）
     * @param watermarkValue  水位值（增量模式用；全量模式传 null）
     * @param watermarkType   水位类型（LONG / TIMESTAMP）
     */
    public JdbcRowReader(DataSource dataSource, SourceSpec spec, String watermarkColumn,
                         Object watermarkValue, String watermarkType) {
        this.dataSource = dataSource;
        this.spec = spec;
        this.watermarkColumn = watermarkColumn;
        this.watermarkValue = watermarkValue;
        this.watermarkType = watermarkType;
    }

    @Override
    public long read(RowConsumer consumer) throws Exception {
        long total = 0L;
        Connection connection = dataSource.getConnection();
        try {
            int pageSize = Math.max(1, spec.getPageSize());
            int pageNo = 0;
            while (true) {
                List<Map<String, Object>> page = fetchPage(connection, pageNo, pageSize);
                if (page.isEmpty()) {
                    break;
                }
                for (Map<String, Object> row : page) {
                    consumer.accept(row);
                    total++;
                }
                if (page.size() < pageSize) {
                    break;
                }
                pageNo++;
            }
        } finally {
            close(connection);
        }
        return total;
    }

    @Override
    public long count() throws Exception {
        Connection connection = dataSource.getConnection();
        try {
            StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(tableRef());
            List<Object> params = new ArrayList<Object>();
            appendWhere(sql, params);
            PreparedStatement statement = connection.prepareStatement(sql.toString());
            try {
                bind(statement, params);
                ResultSet rs = statement.executeQuery();
                try {
                    return rs.next() ? rs.getLong(1) : 0L;
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            close(connection);
        }
    }

    /** 取一页（物化，内存上限 = pageSize）。 */
    private List<Map<String, Object>> fetchPage(Connection connection, int pageNo, int pageSize) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT ").append(columnList()).append(" FROM ").append(tableRef());
        List<Object> params = new ArrayList<Object>();
        appendWhere(sql, params);
        String order = effectiveOrderBy();
        if (order != null) {
            sql.append(" ORDER BY ").append(order);
        }
        sql.append(" LIMIT ? OFFSET ?");
        params.add(Integer.valueOf(pageSize));
        params.add(Integer.valueOf(pageNo * pageSize));

        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        PreparedStatement statement = connection.prepareStatement(sql.toString());
        try {
            bind(statement, params);
            statement.setFetchSize(pageSize);
            ResultSet rs = statement.executeQuery();
            try {
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<String, Object>();
                    for (int i = 1; i <= columnCount; i++) {
                        String label = meta.getColumnLabel(i);
                        if (label == null || label.isEmpty()) {
                            label = meta.getColumnName(i);
                        }
                        // 列标签统一小写收口：驱动之间大小写不一致，见 RowValues 的说明
                        row.put(label.toLowerCase(), rs.getObject(i));
                    }
                    rows.add(row);
                }
            } finally {
                rs.close();
            }
        } finally {
            statement.close();
        }
        return rows;
    }

    /** where 拼装：静态条件 + 水位条件。 */
    private void appendWhere(StringBuilder sql, List<Object> params) {
        List<String> conditions = new ArrayList<String>();
        if (spec.getWhere() != null && !spec.getWhere().trim().isEmpty()) {
            conditions.add("(" + spec.getWhere().trim() + ")");
        }
        if (watermarkColumn != null && !watermarkColumn.trim().isEmpty() && watermarkValue != null) {
            conditions.add(Identifiers.require(watermarkColumn) + " > ?");
            params.add(normalizeWatermark(watermarkValue));
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
    }

    /** 水位值按声明类型归一后再绑定（LONG → Long；TIMESTAMP → Timestamp）。 */
    private Object normalizeWatermark(Object value) {
        if ("TIMESTAMP".equalsIgnoreCase(watermarkType)) {
            if (value instanceof Timestamp) {
                return value;
            }
            return Timestamp.valueOf(String.valueOf(value));
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.valueOf(String.valueOf(value));
    }

    private void bind(PreparedStatement statement, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            statement.setObject(i + 1, params.get(i));
        }
    }

    /** 读取列清单；未声明则取全列（{@code *}）。 */
    private String columnList() {
        List<String> columns = spec.getColumns();
        if (columns == null || columns.isEmpty()) {
            return "*";
        }
        List<String> safe = new ArrayList<String>();
        for (String column : columns) {
            safe.add(Identifiers.require(column));
        }
        return String.join(", ", safe);
    }

    /** 表引用（schema.table 两段，段内白名单）。 */
    private String tableRef() {
        String[] parts = Identifiers.parts(spec.getTable());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(parts[i]);
        }
        return sb.toString();
    }

    /**
     * 有效排序列：显式 orderBy → 水位列 → 首列。
     *
     * <p>分页必须配稳定排序，否则翻页期间源表变动会漏行/重行。
     */
    private String effectiveOrderBy() {
        if (spec.getOrderBy() != null && !spec.getOrderBy().trim().isEmpty()) {
            return Identifiers.require(spec.getOrderBy().trim());
        }
        if (watermarkColumn != null && !watermarkColumn.trim().isEmpty()) {
            return Identifiers.require(watermarkColumn.trim());
        }
        List<String> columns = spec.getColumns();
        if (columns != null && !columns.isEmpty()) {
            return Identifiers.require(columns.get(0));
        }
        return null;
    }

    private void close(Connection connection) {
        try {
            connection.close();
        } catch (SQLException ignore) {
            // 关闭失败不影响主流程
        }
    }
}
