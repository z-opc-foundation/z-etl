package com.zifang.z.etl.core.reconcile;

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
        ReconciliationReport report = new ReconciliationReport();
        report.setSourceCount(count(sourceDs, sourceTable));
        report.setTargetCount(count(targetDs, targetTable));
        report.setSourceChecksum(checksum(sourceDs, sourceTable, sourceKeyCols));
        report.setTargetChecksum(checksum(targetDs, targetTable, targetKeyCols));
        report.setCountMatched(report.getSourceCount() == report.getTargetCount());
        report.setChecksumMatched(report.getSourceChecksum().equals(report.getTargetChecksum()));
        return report;
    }

    /** 表行数。 */
    public long count(DataSource dataSource, String table) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM " + tableRef(table));
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
        Connection connection = dataSource.getConnection();
        try {
            List<String> safe = new ArrayList<String>();
            for (String column : keyColumns) {
                safe.add(Identifiers.require(column));
            }
            PreparedStatement statement = connection.prepareStatement("SELECT " + String.join(", ", safe)
                    + " FROM " + tableRef(table));
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
