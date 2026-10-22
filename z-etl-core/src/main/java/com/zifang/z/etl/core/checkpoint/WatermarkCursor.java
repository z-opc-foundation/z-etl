package com.zifang.z.etl.core.checkpoint;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 复合游标：水位值（{@code update_time} / 自增 id）+ 平局列值（同秒多条的第二键）。
 *
 * <p>为什么要有「平局列」：单键时间游标的过滤条件是 {@code 水位列 > 已存水位}，
 * 当源表在同一秒（同一时间戳）里写进多行时，**只有时间戳等于水位的那批行会被漏掉**——
 * 它们既不 > 水位，也没有更晚的时间戳可以把自己顶上去。所以水位必须带一个第二键：
 * <ul>
 *   <li>水位值严格更大 ⇒ 行在本轮之后；</li>
 *   <li>水位值相等 ⇒ 比平局列（通常是自增主键），平局列更大的行才算本轮之后。</li>
 * </ul>
 * 过滤条件落成 {@code (w > ?) OR (w = ? AND tb > ?)}，游标推进成「(水位值, 平局值)」字典序比较。
 *
 * <p>平局列没有配（{@code watermark.tiebreakColumn} 为空）时退化成单键游标，
 * 行为与旧口径一致（此时同秒多条会被漏掉，引擎会在读面提示，见 WatermarkSpec 注释）。
 *
 * @author zifang
 */
public class WatermarkCursor implements Comparable<WatermarkCursor> {

    private final String column;
    private final String type;
    private final String value;
    private final String tiebreakColumn;
    private final String tiebreakValue;

    private WatermarkCursor(String column, String type, String value,
                            String tiebreakColumn, String tiebreakValue) {
        this.column = column;
        this.type = type;
        this.value = value;
        this.tiebreakColumn = tiebreakColumn;
        this.tiebreakValue = tiebreakValue;
    }

    /**
     * 单键游标。
     *
     * @param column 水位列
     * @param type   水位类型（LONG / TIMESTAMP）
     * @param value  水位值（任意 JDBC 友好类型）
     * @return 游标
     */
    public static WatermarkCursor of(String column, String type, Object value) {
        String normalizedType = normalizeType(type);
        return new WatermarkCursor(column, normalizedType,
                value == null ? null : format(value, normalizedType), null, null);
    }

    /**
     * 复合游标（带平局列）。
     *
     * @param column         水位列
     * @param type           水位类型（LONG / TIMESTAMP）
     * @param value          水位值
     * @param tiebreakColumn 平局列（可空）
     * @param tiebreakValue  平局列值（可空）
     * @return 游标
     */
    public static WatermarkCursor of(String column, String type, Object value,
                                     String tiebreakColumn, Object tiebreakValue) {
        String normalizedType = normalizeType(type);
        String tbColumn = (tiebreakColumn == null || tiebreakColumn.trim().isEmpty())
                ? null : tiebreakColumn.trim();
        String tbValue = tbColumn == null ? null
                : (tiebreakValue == null ? null : String.valueOf(tiebreakValue));
        return new WatermarkCursor(column, normalizedType,
                value == null ? null : format(value, normalizedType), tbColumn, tbValue);
    }

    public String getColumn() {
        return column;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public String getTiebreakColumn() {
        return tiebreakColumn;
    }

    public String getTiebreakValue() {
        return tiebreakValue;
    }

    /** 是否带平局列（复合游标）。 */
    public boolean hasTiebreak() {
        return tiebreakColumn != null && !tiebreakColumn.trim().isEmpty();
    }

    /** 游标为空（没有水位值）。 */
    public boolean isEmpty() {
        return value == null || value.trim().isEmpty();
    }

    /**
     * 复合比较：先比水位值，水位值相等再比平局值。
     *
     * @param other 对方游标
     * @return &lt;0 / =0 / &gt;0
     */
    @Override
    public int compareTo(WatermarkCursor other) {
        if (other == null) {
            return 1;
        }
        int primary = compareValues(this.value, other.value, this.type);
        if (primary != 0) {
            return primary;
        }
        if (this.hasTiebreak() && other.hasTiebreak()) {
            return compareScalar(this.tiebreakValue, other.tiebreakValue);
        }
        return 0;
    }

    /** 本游标是否严格晚于对方。 */
    public boolean isAfter(WatermarkCursor other) {
        return compareTo(other) > 0;
    }

    /** 水位值按类型归一（LONG → Long；TIMESTAMP → Timestamp），供 JDBC 绑定。 */
    public Object typedValue() {
        return normalize(value, type);
    }

    /** 平局列值按类型归一（能当数字就当数字，否则按字符串）。 */
    public Object typedTiebreakValue() {
        if (tiebreakValue == null) {
            return null;
        }
        try {
            return Long.valueOf(tiebreakValue.trim());
        } catch (NumberFormatException e) {
            return tiebreakValue;
        }
    }

    /** 展示口径：{@code 水位值} 或 {@code 水位值#平局值}。 */
    public String describe() {
        if (!hasTiebreak() || tiebreakValue == null) {
            return value;
        }
        return value + "#" + tiebreakValue;
    }

    @Override
    public String toString() {
        return describe();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof WatermarkCursor)) {
            return false;
        }
        WatermarkCursor other = (WatermarkCursor) obj;
        return compareTo(other) == 0
                && (hasTiebreak() == other.hasTiebreak())
                && (tiebreakValue == null ? other.tiebreakValue == null
                        : tiebreakValue.equals(other.tiebreakValue));
    }

    @Override
    public int hashCode() {
        int result = value == null ? 0 : value.hashCode();
        result = 31 * result + (tiebreakValue == null ? 0 : tiebreakValue.hashCode());
        return result;
    }

    // ——— 归一与比较 ————————————————————————————————————————————————

    /** 类型归一：只认 LONG / TIMESTAMP，其余按 LONG。 */
    public static String normalizeType(String type) {
        if (type == null || type.trim().isEmpty()) {
            return "LONG";
        }
        String upper = type.trim().toUpperCase();
        return "TIMESTAMP".equals(upper) ? "TIMESTAMP" : "LONG";
    }

    /** 值按类型归一（LONG → Long；TIMESTAMP → Timestamp）。 */
    public static Object normalize(Object value, String type) {
        if (value == null) {
            return null;
        }
        if ("TIMESTAMP".equals(normalizeType(type))) {
            return toTimestamp(value);
        }
        return toLong(value);
    }

    /** 值按类型格式化（落库口径：都是字符串）。 */
    public static String format(Object value, String type) {
        if (value == null) {
            return null;
        }
        if ("TIMESTAMP".equals(normalizeType(type))) {
            return toTimestamp(value).toString();
        }
        return String.valueOf(toLong(value));
    }

    /** 按类型比较两个值（空值最小）。 */
    public static int compareValues(String left, String right, String type) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if ("TIMESTAMP".equals(normalizeType(type))) {
            return toTimestamp(left).compareTo(toTimestamp(right));
        }
        return compareScalar(left, right);
    }

    /** 标量比较：能当数字就按数字比，否则按字符串比。 */
    public static int compareScalar(String left, String right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        try {
            BigDecimal l = new BigDecimal(left.trim());
            BigDecimal r = new BigDecimal(right.trim());
            return l.compareTo(r);
        } catch (NumberFormatException e) {
            return left.compareTo(right);
        }
    }

    /** 时间值归一。 */
    public static Timestamp toTimestamp(Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        }
        if (value instanceof java.time.LocalDateTime) {
            return Timestamp.valueOf((java.time.LocalDateTime) value);
        }
        return Timestamp.valueOf(String.valueOf(value));
    }

    /** 数值归一。 */
    public static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value).trim());
    }
}
