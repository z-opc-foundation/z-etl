package com.zifang.z.etl.core.writer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * upsert 方言。
 *
 * <p>零件面没有 upsert、没有幂等键（需求 008 notes.md §9.6.1 E7），所以批量 upsert + 幂等键必须 z-etl 自己实现。
 * 三种落地写法都靠**目标表上的唯一约束**保证「不重」：
 * <ul>
 *   <li>MySQL：{@code INSERT ... ON DUPLICATE KEY UPDATE}（要求唯一索引）</li>
 *   <li>PostgreSQL：{@code INSERT ... ON CONFLICT (keys) DO UPDATE}</li>
 *   <li>H2：{@code MERGE INTO ... KEY (keys) VALUES ...}</li>
 *   <li>兜底（其它库）：先 UPDATE 后 INSERT（两段式，语义等价、代价高一档）</li>
 * </ul>
 *
 * @author zifang
 */
public enum UpsertDialect {

    MYSQL,
    POSTGRES,
    H2,
    GENERIC;

    /**
     * 按连接元数据判别方言；判不出来走 {@link #GENERIC} 兜底（不猜、不静默按 MySQL 写）。
     */
    public static UpsertDialect resolve(Connection connection) {
        try {
            String product = connection.getMetaData().getDatabaseProductName();
            if (product == null) {
                return GENERIC;
            }
            String key = product.toLowerCase();
            if (key.contains("mysql") || key.contains("mariadb")) {
                return MYSQL;
            }
            if (key.contains("postgres")) {
                return POSTGRES;
            }
            if (key.contains("h2")) {
                return H2;
            }
            return GENERIC;
        } catch (SQLException e) {
            return GENERIC;
        }
    }

    /**
     * 拼 upsert 语句。
     *
     * @param table      目标表（已过标识符白名单）
     * @param columns    全部列
     * @param keyColumns 幂等键列（子集）
     * @return upsert 语句；{@link #GENERIC} 返回 null（走两段式兜底）
     */
    public String upsertSql(String table, List<String> columns, List<String> keyColumns) {
        StringBuilder sb = new StringBuilder();
        String columnList = String.join(", ", columns);
        String placeholders = placeholders(columns.size());
        switch (this) {
            case MYSQL: {
                sb.append("INSERT INTO ").append(table).append(" (").append(columnList).append(") VALUES (")
                        .append(placeholders).append(") ON DUPLICATE KEY UPDATE ");
                sb.append(updateAssignments(columns, keyColumns, "VALUES(%s)"));
                return sb.toString();
            }
            case POSTGRES: {
                sb.append("INSERT INTO ").append(table).append(" (").append(columnList).append(") VALUES (")
                        .append(placeholders).append(") ON CONFLICT (").append(String.join(", ", keyColumns))
                        .append(") DO UPDATE SET ");
                sb.append(updateAssignments(columns, keyColumns, "EXCLUDED.%s"));
                return sb.toString();
            }
            case H2: {
                sb.append("MERGE INTO ").append(table).append(" (").append(columnList).append(") KEY (")
                        .append(String.join(", ", keyColumns)).append(") VALUES (").append(placeholders).append(")");
                return sb.toString();
            }
            default:
                return null;
        }
    }

    /** 拼普通 insert 语句（writeMode=insert 时用）。 */
    public String insertSql(String table, List<String> columns) {
        return "INSERT INTO " + table + " (" + String.join(", ", columns) + ") VALUES ("
                + placeholders(columns.size()) + ")";
    }

    /** 拼 update 语句（GENERIC 兜底路径用）。 */
    public String updateSql(String table, List<String> columns, List<String> keyColumns) {
        StringBuilder sb = new StringBuilder("UPDATE ").append(table).append(" SET ");
        sb.append(updateAssignments(columns, keyColumns, "%s"));
        sb.append(" WHERE ");
        List<String> conditions = new java.util.ArrayList<String>();
        for (String key : keyColumns) {
            conditions.add(key + " = ?");
        }
        sb.append(String.join(" AND ", conditions));
        return sb.toString();
    }

    /** SET 子句：幂等键列不参与更新（它是匹配条件，不是被更新内容）。 */
    private static String updateAssignments(List<String> columns, List<String> keyColumns, String valueTemplate) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String column : columns) {
            if (keyColumns != null && keyColumns.contains(column)) {
                continue;
            }
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(column).append(" = ").append(String.format(valueTemplate, column));
        }
        if (first) {
            // 没有可更新的列（只有幂等键）——给个恒假条件避免语法错误
            sb.append(column0(columns)).append(" = ").append(column0(columns));
        }
        return sb.toString();
    }

    private static String column0(List<String> columns) {
        return columns.isEmpty() ? "id" : columns.get(0);
    }

    private static String placeholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('?');
        }
        return sb.toString();
    }
}
