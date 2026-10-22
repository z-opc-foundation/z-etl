package com.zifang.z.etl.core.reader;

import com.zifang.z.etl.core.job.AggregateSpec;
import com.zifang.z.etl.core.job.PushdownSpec;
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
    /** 平局列（同秒多多的第二键；可空 ⇒ 单键游标）。 */
    private final String tiebreakColumn;
    /** 平局列的当前值（复合游标的第二键）。 */
    private final Object tiebreakValue;
    /** 回溯窗口（>0 ⇒ 用「水位值 − 窗口」的闭区间下界，兜时钟回拨 / 迟到数据）。 */
    private final long overlap;

    /** 最近一次实际执行的 SQL（取证用：证明下推确实进了源库）。 */
    private volatile String lastExecutedSql;

    /**
     * @param dataSource       源数据源
     * @param spec             源定义
     * @param watermarkColumn  水位列（增量模式用；全量模式传 null）
     * @param watermarkValue  水位值（增量模式用；全量模式传 null）
     * @param watermarkType   水位类型（LONG / TIMESTAMP）
     */
    public JdbcRowReader(DataSource dataSource, SourceSpec spec, String watermarkColumn,
                         Object watermarkValue, String watermarkType) {
        this(dataSource, spec, watermarkColumn, watermarkValue, watermarkType, null, null, 0L);
    }

    /**
     * 复合游标版构造器。
     *
     * @param dataSource      源数据源
     * @param spec            源定义
     * @param watermarkColumn 水位列（增量模式用；全量模式传 null）
     * @param watermarkValue 水位值（增量模式用；全量模式传 null）
     * @param watermarkType  水位类型（LONG / TIMESTAMP）
     * @param tiebreakColumn 平局列（同秒多多的第二键；可空）
     * @param tiebreakValue  平局列当前值（可空）
     * @param overlap        回溯窗口（0 = 关闭）
     */
    public JdbcRowReader(DataSource dataSource, SourceSpec spec, String watermarkColumn,
                         Object watermarkValue, String watermarkType,
                         String tiebreakColumn, Object tiebreakValue, long overlap) {
        this.dataSource = dataSource;
        this.spec = spec;
        this.watermarkColumn = watermarkColumn;
        this.watermarkValue = watermarkValue;
        this.watermarkType = watermarkType;
        this.tiebreakColumn = (tiebreakColumn == null || tiebreakColumn.trim().isEmpty())
                ? null : tiebreakColumn.trim();
        this.tiebreakValue = tiebreakValue;
        this.overlap = Math.max(0L, overlap);
    }

    @Override
    public long read(RowConsumer consumer) throws Exception {
        final long[] total = new long[]{0L};
        readPages(new PageHandler() {
            @Override
            public void handle(List<Map<String, Object>> page) throws Exception {
                for (Map<String, Object> row : page) {
                    consumer.accept(row);
                    total[0]++;
                }
            }
        });
        return total[0];
    }

    /**
     * 按**页**读出（页级断点续传的口子）。
     *
     * <p>一页交给回调一次 ⇒ 调用方可以在「本页写成功」之后推进水位（checkpoint），
     * 进程被 kill / 崩掉时水位停在上一个成功页 ⇒ 重启从断点继续，而不是从头再来。
     *
     * <p>分页口径与 {@link #read(RowConsumer)} 完全一致：同一条 where、同一个排序，
     * 本次读取期间过滤条件固定（不随水位推进而变），所以翻页不会漏行。
     *
     * @param handler 页处理器
     * @return 读出的行数
     * @throws Exception 读取失败
     */
    public long readPages(PageHandler handler) throws Exception {
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
                handler.handle(page);
                total += page.size();
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
        StringBuilder sql = new StringBuilder("SELECT ").append(selectList()).append(" FROM ").append(tableRef());
        List<Object> params = new ArrayList<Object>();
        appendWhere(sql, params);
        String groupBy = groupByClause();
        if (groupBy != null) {
            sql.append(" GROUP BY ").append(groupBy);
        }
        String order = effectiveOrderBy();
        if (order != null) {
            sql.append(" ORDER BY ").append(order);
        }
        sql.append(" LIMIT ? OFFSET ?");
        params.add(Integer.valueOf(pageSize));
        params.add(Integer.valueOf(pageNo * pageSize));
        lastExecutedSql = sql.toString();

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

    /** where 拼装：静态条件 + 下推过滤 + 水位条件。 */
    private void appendWhere(StringBuilder sql, List<Object> params) {
        List<String> conditions = new ArrayList<String>();
        if (spec.getWhere() != null && !spec.getWhere().trim().isEmpty()) {
            conditions.add("(" + spec.getWhere().trim() + ")");
        }
        PushdownSpec pushdown = spec.getPushdown();
        if (pushdown != null && pushdown.hasFilter()) {
            conditions.add("(" + pushdown.getFilter().trim() + ")");
        }
        if (watermarkColumn != null && !watermarkColumn.trim().isEmpty() && watermarkValue != null) {
            String w = Identifiers.require(watermarkColumn);
            if (overlap > 0L) {
                // 回溯窗口口径：下界 = 水位值 − 窗口（闭区间 >=），窗口内的行会被重读（目标侧 upsert 幂等）
                conditions.add(w + " >= ?");
                params.add(overlapThreshold());
            } else if (tiebreakColumn != null && !tiebreakColumn.trim().isEmpty() && tiebreakValue != null) {
                // 复合游标口径：(水位值 > ?) OR (水位值 = ? AND 平局列 > ?)
                // —— 同秒多条不会漏：时间戳等于水位的行靠平局列（自增 id）接力。
                String tb = Identifiers.require(tiebreakColumn);
                Object normalized = normalizeWatermark(watermarkValue);
                conditions.add("(" + w + " > ? OR (" + w + " = ? AND " + tb + " > ?))");
                params.add(normalized);
                params.add(normalized);
                params.add(normalizeTiebreak(tiebreakValue));
            } else {
                conditions.add(w + " > ?");
                params.add(normalizeWatermark(watermarkValue));
            }
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

    /** 平局列值归一（能当数字就当数字，否则按字符串）。 */
    private Object normalizeTiebreak(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.valueOf(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return String.valueOf(value);
        }
    }

    /** 回溯窗口的下界：水位值 − 窗口（TIMESTAMP 单位秒；LONG 单位行数）。 */
    private Object overlapThreshold() {
        Object normalized = normalizeWatermark(watermarkValue);
        if (normalized instanceof Timestamp) {
            return new Timestamp(((Timestamp) normalized).getTime() - overlap * 1000L);
        }
        return Long.valueOf(((Number) normalized).longValue() - overlap);
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

    /**
     * 投影清单：**下推口径**。
     *
     * <ul>
     *   <li>聚合下推（{@code groupBy} / {@code aggregates}）：分组列 + 聚合表达式（带别名），
     *       聚合在源库算完，引擎只拿聚合结果行。</li>
     *   <li>否则用作业声明的列（列裁剪下推）；未声明则 {@code *}。</li>
     * </ul>
     */
    private String selectList() {
        PushdownSpec pushdown = spec.getPushdown();
        if (pushdown == null || !pushdown.isAggregatePushdown()) {
            return columnList();
        }
        List<String> parts = new ArrayList<String>();
        for (String column : pushdown.getGroupBy()) {
            parts.add(Identifiers.require(column));
        }
        for (AggregateSpec aggregate : pushdown.getAggregates()) {
            parts.add(aggregateExpression(aggregate) + " AS " + Identifiers.require(aggregate.effectiveAlias()));
        }
        return String.join(", ", parts);
    }

    /**
     * 聚合表达式：函数名白名单 + 列名标识符白名单（自由 SQL 文本一律不接受）。
     */
    private String aggregateExpression(AggregateSpec aggregate) {
        String function = aggregate.effectiveFunction();
        String column = aggregate.getColumn() == null ? "" : aggregate.getColumn().trim();
        if ("*".equals(column)) {
            return function + "(*)";
        }
        return function + "(" + Identifiers.require(column) + ")";
    }

    /** {@code GROUP BY} 子句；无分组列时返回 {@code null}。 */
    private String groupByClause() {
        PushdownSpec pushdown = spec.getPushdown();
        if (pushdown == null || pushdown.getGroupBy() == null || pushdown.getGroupBy().isEmpty()) {
            return null;
        }
        List<String> safe = new ArrayList<String>();
        for (String column : pushdown.getGroupBy()) {
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
     * 有效排序列：显式 orderBy → 聚合下推的分组列 → 水位列 → 首列。
     *
     * <p>分页必须配稳定排序，否则翻页期间源表变动会漏行/重行。
     */
    private String effectiveOrderBy() {
        if (spec.getOrderBy() != null && !spec.getOrderBy().trim().isEmpty()) {
            return Identifiers.require(spec.getOrderBy().trim());
        }
        PushdownSpec pushdown = spec.getPushdown();
        if (pushdown != null && pushdown.getGroupBy() != null && !pushdown.getGroupBy().isEmpty()) {
            return Identifiers.require(pushdown.getGroupBy().get(0));
        }
        if (watermarkColumn != null && !watermarkColumn.trim().isEmpty()) {
            String order = Identifiers.require(watermarkColumn.trim());
            if (tiebreakColumn != null && !tiebreakColumn.trim().isEmpty() && overlap <= 0L) {
                // 复合游标：水位列 + 平局列 一起排序 ⇒ 排序键唯一 ⇒ 翻页稳定不漏行
                order = order + ", " + Identifiers.require(tiebreakColumn.trim());
            }
            return order;
        }
        List<String> columns = spec.getColumns();
        if (columns != null && !columns.isEmpty()) {
            return Identifiers.require(columns.get(0));
        }
        return null;
    }

    /** 最近一次实际执行的 SQL（取证用）。 */
    public String lastExecutedSql() {
        return lastExecutedSql;
    }

    private void close(Connection connection) {
        try {
            connection.close();
        } catch (SQLException ignore) {
            // 关闭失败不影响主流程
        }
    }
}
