package com.zifang.z.etl.core.reconcile;

import com.zifang.z.etl.core.job.AggregateSpec;
import com.zifang.z.etl.core.job.PushdownSpec;
import com.zifang.z.etl.core.util.Checksums;
import com.zifang.util.db.support.Identifiers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 对账器：搬运完自动核对源/目标两侧（行数 + 校验和双口径）。
 *
 * <p>两侧都做**整表扫描**算键多重集校验和，所以「首跑全量」与「增量接力」两种模式下口径一致可比。
 * 代价：每次跑完多两次整表扫描（M1 代价，已在 README 标注；增量省扫描的做法留作后续）。
 *
 * @author zifang
 */
public class Reconciler {

    /**
     * 对账。
     *
     * @param sourceDs        源数据源
     * @param sourceTable     源表
     * @param sourceKeyCols   源侧幂等键列
     * @param targetDs        目标数据源
     * @param targetTable     目标表
     * @param targetKeyCols   目标侧幂等键列
     * @return 对账报告
     * @throws SQLException 统计失败
     */
    public ReconciliationReport reconcile(DataSource sourceDs, String sourceTable, List<String> sourceKeyCols,
                                          DataSource targetDs, String targetTable, List<String> targetKeyCols)
            throws SQLException {
        return reconcile(sourceDs, sourceTable, sourceKeyCols, null, targetDs, targetTable, targetKeyCols);
    }

    /**
     * 对账（带源侧过滤条件）。
     *
     * <p>源侧必须带上**同样的**过滤条件：作业里做了过滤/聚合下推时，源表总行数 ≠ 实际搬走的行数，
     * 不带过滤的对账会误报不一致。
     *
     * @param sourceDs        源数据源
     * @param sourceTable     源表
     * @param sourceKeyCols   源侧幂等键列
     * @param sourceWhere     源侧过滤片段（可空）
     * @param targetDs        目标数据源
     * @param targetTable     目标表
     * @param targetKeyCols   目标侧幂等键列
     * @return 对账报告
     * @throws SQLException 统计失败
     */
    public ReconciliationReport reconcile(DataSource sourceDs, String sourceTable, List<String> sourceKeyCols,
                                          String sourceWhere,
                                          DataSource targetDs, String targetTable, List<String> targetKeyCols)
            throws SQLException {
        ReconciliationReport report = new ReconciliationReport();
        report.setSourceCount(count(sourceDs, sourceTable, sourceWhere));
        report.setTargetCount(count(targetDs, targetTable, null));
        report.setSourceChecksum(checksum(sourceDs, sourceTable, sourceKeyCols, sourceWhere));
        report.setTargetChecksum(checksum(targetDs, targetTable, targetKeyCols, null));
        report.setCountMatched(report.getSourceCount() == report.getTargetCount());
        report.setChecksumMatched(report.getSourceChecksum().equals(report.getTargetChecksum()));
        return report;
    }

    /**
     * 对账（源侧带下推口径：过滤下推 + 聚合下推都算进去）。
     *
     * <p>源侧必须复现**作业实际取数的口径**，否则会误报：
     * <ul>
     *   <li>过滤下推：源表总行数 ≠ 实际搬走的行数 ⇒ 源侧要带同一个过滤条件。</li>
     *   <li>聚合下推：源侧「行」是聚合结果行（每组一行），键列 = 分组列。</li>
     * </ul>
     *
     * @param sourceDs      源数据源
     * @param sourceSpec    源定义（表 / 过滤 / 下推）
     * @param sourceKeyCols 源侧幂等键列（聚合下推时 = 分组列）
     * @param targetDs      目标数据源
     * @param targetTable   目标表
     * @param targetKeyCols 目标侧幂等键列
     * @return 对账报告
     * @throws SQLException 统计失败
     */
    public ReconciliationReport reconcile(DataSource sourceDs, com.zifang.z.etl.core.job.SourceSpec sourceSpec,
                                          List<String> sourceKeyCols,
                                          DataSource targetDs, String targetTable, List<String> targetKeyCols)
            throws SQLException {
        ReconciliationReport report = new ReconciliationReport();
        PushdownSpec pushdown = sourceSpec.getPushdown();
        boolean aggregate = pushdown != null && pushdown.isAggregatePushdown();
        String where = combineWhere(sourceSpec.getWhere(), pushdown);

        long[] sourceCount = new long[1];
        String[] sourceChecksum = new String[1];
        computeSourceSide(sourceDs, sourceSpec, sourceKeyCols, where, aggregate, sourceCount, sourceChecksum);

        report.setSourceCount(sourceCount[0]);
        report.setTargetCount(count(targetDs, targetTable, null));
        report.setSourceChecksum(sourceChecksum[0]);
        report.setTargetChecksum(checksum(targetDs, targetTable, targetKeyCols, null));
        report.setCountMatched(report.getSourceCount() == report.getTargetCount());
        report.setChecksumMatched(report.getSourceChecksum().equals(report.getTargetChecksum()));
        return report;
    }

    /** 源侧口径合并：作业静态条件 + 下推过滤。 */
    private static String combineWhere(String where, PushdownSpec pushdown) {
        List<String> parts = new ArrayList<String>();
        if (where != null && !where.trim().isEmpty()) {
            parts.add("(" + where.trim() + ")");
        }
        if (pushdown != null && pushdown.hasFilter()) {
            parts.add("(" + pushdown.getFilter().trim() + ")");
        }
        if (parts.isEmpty()) {
            return null;
        }
        return String.join(" AND ", parts);
    }

    /** 源侧一次查询同时算「行数 + 键多重集校验和」（口径与目标侧一致）。 */
    private void computeSourceSide(DataSource dataSource, com.zifang.z.etl.core.job.SourceSpec sourceSpec,
                                    List<String> keyColumns, String where, boolean aggregate,
                                    long[] countHolder, String[] checksumHolder) throws SQLException {
        List<String> safeKeys = new ArrayList<String>();
        for (String column : keyColumns) {
            safeKeys.add(Identifiers.require(column));
        }
        List<String> selectParts = new ArrayList<String>();
        if (aggregate) {
            PushdownSpec pushdown = sourceSpec.getPushdown();
            for (String column : pushdown.getGroupBy()) {
                selectParts.add(Identifiers.require(column));
            }
            for (AggregateSpec aggregateSpec : pushdown.getAggregates()) {
                selectParts.add(aggregateExpression(aggregateSpec) + " AS "
                        + Identifiers.require(aggregateSpec.effectiveAlias()));
            }
        } else {
            selectParts.addAll(safeKeys);
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", selectParts))
                .append(" FROM ").append(tableRef(sourceSpec.getTable()));
        appendWhere(sql, where);
        if (aggregate) {
            PushdownSpec pushdown = sourceSpec.getPushdown();
            if (pushdown.getGroupBy() != null && !pushdown.getGroupBy().isEmpty()) {
                List<String> groupParts = new ArrayList<String>();
                for (String column : pushdown.getGroupBy()) {
                    groupParts.add(Identifiers.require(column));
                }
                sql.append(" GROUP BY ").append(String.join(", ", groupParts));
            }
        }
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(sql.toString());
            try {
                ResultSet rs = statement.executeQuery();
                try {
                    long count = 0L;
                    long sum = 0L;
                    while (rs.next()) {
                        Object[] values = new Object[safeKeys.size()];
                        for (int i = 0; i < safeKeys.size(); i++) {
                            values[i] = rs.getObject(i + 1);
                        }
                        sum += Checksums.rowHash(values);
                        count++;
                    }
                    countHolder[0] = count;
                    checksumHolder[0] = Long.toHexString(sum);
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 聚合表达式（函数白名单 + 标识符白名单）。 */
    private static String aggregateExpression(AggregateSpec aggregate) {
        String function = aggregate.effectiveFunction();
        String column = aggregate.getColumn() == null ? "" : aggregate.getColumn().trim();
        if ("*".equals(column)) {
            return function + "(*)";
        }
        return function + "(" + Identifiers.require(column) + ")";
    }

    /** where 片段追加。 */
    private static void appendWhere(StringBuilder sql, String where) {
        if (where != null && !where.trim().isEmpty()) {
            sql.append(" WHERE ").append(where.trim());
        }
    }

    /** 表行数。 */
    public long count(DataSource dataSource, String table) throws SQLException {
        return count(dataSource, table, null);
    }

    /** 表行数（带过滤）。 */
    public long count(DataSource dataSource, String table, String where) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(tableRef(table));
            appendWhere(sql, where);
            PreparedStatement statement = connection.prepareStatement(sql.toString());
            try {
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
            connection.close();
        }
    }

    /** 键多重集校验和（与顺序无关）。 */
    public String checksum(DataSource dataSource, String table, List<String> keyColumns) throws SQLException {
        return checksum(dataSource, table, keyColumns, null);
    }

    /** 键多重集校验和（与顺序无关；带过滤）。 */
    public String checksum(DataSource dataSource, String table, List<String> keyColumns, String where)
            throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            List<String> safe = new ArrayList<String>();
            for (String column : keyColumns) {
                safe.add(Identifiers.require(column));
            }
            StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", safe))
                    .append(" FROM ").append(tableRef(table));
            appendWhere(sql, where);
            PreparedStatement statement = connection.prepareStatement(sql.toString());
            try {
                ResultSet rs = statement.executeQuery();
                try {
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();
                    long sum = 0L;
                    while (rs.next()) {
                        Object[] values = new Object[columnCount];
                        for (int i = 1; i <= columnCount; i++) {
                            values[i - 1] = rs.getObject(i);
                        }
                        sum += Checksums.rowHash(values);
                    }
                    return Long.toHexString(sum);
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    /** 扫全表算校验和（不指定键列时用全列）。 */
    public String checksum(DataSource dataSource, String table) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + tableRef(table));
            try {
                ResultSet rs = statement.executeQuery();
                try {
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();
                    long sum = 0L;
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<String, Object>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
                        }
                        sum += Checksums.rowHash(row, lowercaseAll(meta));
                    }
                    return Long.toHexString(sum);
                } finally {
                    rs.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    private static List<String> lowercaseAll(ResultSetMetaData meta) throws SQLException {
        List<String> columns = new ArrayList<String>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            columns.add(meta.getColumnLabel(i).toLowerCase());
        }
        return columns;
    }

    private static String tableRef(String table) {
        String[] parts = Identifiers.parts(table);
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
