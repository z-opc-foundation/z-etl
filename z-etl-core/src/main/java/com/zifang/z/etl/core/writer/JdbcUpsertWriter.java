package com.zifang.z.etl.core.writer;

import com.zifang.z.etl.core.job.TargetSpec;
import com.zifang.util.db.support.Identifiers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC 写出器：批量 upsert（幂等键保证「不重不漏」）。
 *
 * <p>攒批阈值 = {@code target.batchSize}（背压第一道闸）。写模式：
 * <ul>
 *   <li>{@code upsert}（默认）：按方言拼 upsert 语句，幂等键列必须在目标表上有唯一约束。</li>
 *   <li>{@code insert}：只插不更新（不幂等，仅供一次性补数用）。</li>
 * </ul>
 *
 * <p>GENERIC 方言（判不出来的库）走 UPDATE-then-INSERT 两段式兜底，不猜方言。
 *
 * @author zifang
 */
public class JdbcUpsertWriter implements RowWriter {

    private final DataSource dataSource;
    private final TargetSpec spec;
    private final List<String> keyColumns;
    private final List<String> declaredColumns;

    private final List<Map<String, Object>> batchRows = new ArrayList<Map<String, Object>>();

    private Connection connection;
    private PreparedStatement statement;
    private UpsertDialect dialect;
    private List<String> columns;
    private long written;

    /**
     * @param dataSource       目标数据源
     * @param spec             目标定义
     * @param keyColumns       幂等键列（目标列名）
     * @param declaredColumns  目标列（为空则由首行推导）
     */
    public JdbcUpsertWriter(DataSource dataSource, TargetSpec spec, List<String> keyColumns,
                            List<String> declaredColumns) {
        this.dataSource = dataSource;
        this.spec = spec;
        this.keyColumns = new ArrayList<String>(keyColumns);
        this.declaredColumns = declaredColumns == null ? null : new ArrayList<String>(declaredColumns);
    }

    @Override
    public void write(Map<String, Object> row) throws Exception {
        if (row == null) {
            return;
        }
        if (columns == null) {
            columns = resolveColumns(row);
        }
        batchRows.add(new LinkedHashMap<String, Object>(row));
        written++;
        if (batchRows.size() >= Math.max(1, spec.getBatchSize())) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (batchRows.isEmpty()) {
            return;
        }
        if (connection == null) {
            connection = dataSource.getConnection();
            dialect = UpsertDialect.resolve(connection);
        }
        boolean insertOnly = "insert".equalsIgnoreCase(spec.getWriteMode());
        if (!insertOnly && dialect == UpsertDialect.GENERIC) {
            executeGenericBatch();
        } else {
            executeStatementBatch(insertOnly);
        }
        batchRows.clear();
    }

    @Override
    public long written() {
        return written;
    }

    /** 关闭（先冲刷）。 */
    public void close() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ignore) {
            // 忽略关闭异常
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignore) {
            // 忽略关闭异常
        }
        statement = null;
        connection = null;
    }

    /** 目标列：声明优先，否则首行推导。 */
    private List<String> resolveColumns(Map<String, Object> firstRow) {
        if (declaredColumns != null && !declaredColumns.isEmpty()) {
            for (String column : declaredColumns) {
                Identifiers.require(column);
            }
            return declaredColumns;
        }
        return new ArrayList<String>(firstRow.keySet());
    }

    /** 标准批处理：一条语句 executeBatch。 */
    private void executeStatementBatch(boolean insertOnly) throws SQLException {
        String table = tableRef();
        if (insertOnly) {
            statement = connection.prepareStatement(dialect.insertSql(table, columns));
        } else {
            statement = connection.prepareStatement(dialect.upsertSql(table, columns, keyColumns));
        }
        try {
            for (Map<String, Object> row : batchRows) {
                bind(statement, row);
                statement.addBatch();
            }
            statement.executeBatch();
        } finally {
            statement.close();
            statement = null;
        }
    }

    /** GENERIC 兜底：UPDATE 命中 0 行再 INSERT（语义与 upsert 等价，代价高一档）。 */
    private void executeGenericBatch() throws SQLException {
        String table = tableRef();
        PreparedStatement update = connection.prepareStatement(dialect.updateSql(table, columns, keyColumns));
        PreparedStatement insert = connection.prepareStatement(dialect.insertSql(table, columns));
        try {
            for (Map<String, Object> row : batchRows) {
                int index = 1;
                for (String column : columns) {
                    if (keyColumns.contains(column)) {
                        continue;
                    }
                    update.setObject(index++, row.get(column));
                }
                for (String key : keyColumns) {
                    update.setObject(index++, row.get(key));
                }
                if (update.executeUpdate() == 0) {
                    index = 1;
                    for (String column : columns) {
                        insert.setObject(index++, row.get(column));
                    }
                    insert.executeUpdate();
                }
            }
        } finally {
            update.close();
            insert.close();
        }
    }

    private void bind(PreparedStatement target, Map<String, Object> row) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            target.setObject(i + 1, row.get(columns.get(i)));
        }
    }

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
}
