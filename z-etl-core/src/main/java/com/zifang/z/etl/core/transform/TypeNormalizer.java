package com.zifang.z.etl.core.transform;

import com.zifang.z.etl.core.job.TypeNames;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 类型归一器（引擎层规则，见需求 008 notes.md §9.6.1 E1）。
 *
 * <p>三条硬约束来自 TASK-20261012-010 的实跑结论：
 * <ul>
 *   <li>JDBC 侧给的是原生类型（{@code BigDecimal} / {@code java.sql.Date} / {@code Timestamp}），
 *       但从 Excel 侧来的全是 {@code String}，从内存 SQL 侧来的是浮点 ⇒ 归一必须能同时吃下这三种。</li>
 *   <li>金额一律落 {@link BigDecimal}：内存 SQL 是浮点语义，实测 {@code 80.00*1.13=90.39999999999999}。</li>
 *   <li>空值默认保留（null 落 null），除非映射显式声明不可空。</li>
 * </ul>
 *
 * @author zifang
 */
public final class TypeNormalizer {

    private TypeNormalizer() {
    }

    /**
     * 按类型名归一。
     *
     * @param value 原值（可空）
     * @param type  类型名（{@link TypeNames} 全集之一）；为空表示原样透传
     * @return 归一后的值
     */
    public static Object normalize(Object value, String type) {
        if (type == null || type.trim().isEmpty()) {
            return value;
        }
        String key = type.trim().toUpperCase();
        if (value == null) {
            return null;
        }
        if (TypeNames.LONG.equals(key)) {
            return toLong(value);
        }
        if (TypeNames.INT.equals(key)) {
            return toInt(value);
        }
        if (TypeNames.STRING.equals(key)) {
            return toStringValue(value);
        }
        if (TypeNames.BIGDECIMAL.equals(key)) {
            return toBigDecimal(value);
        }
        if (TypeNames.DOUBLE.equals(key)) {
            return toDouble(value);
        }
        if (TypeNames.BOOLEAN.equals(key)) {
            return toBoolean(value);
        }
        if (TypeNames.TIMESTAMP.equals(key)) {
            return toTimestamp(value);
        }
        if (TypeNames.DATE.equals(key)) {
            return toDate(value);
        }
        return value;
    }

    /** 字符串化：日期/时间戳落 ISO 文本，BigDecimal 保留精度（不用 double 兜）。 */
    public static String toStringValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toString();
        }
        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime()).toString();
        }
        return String.valueOf(value);
    }

    public static Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        return Long.valueOf(new BigDecimal(text).longValue());
    }

    public static Integer toInt(Object value) {
        Long boxed = toLong(value);
        return boxed == null ? null : Integer.valueOf(boxed.intValue());
    }

    public static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof java.math.BigInteger) {
            return new BigDecimal((java.math.BigInteger) value);
        }
        if (value instanceof Double || value instanceof Float) {
            // 浮点来源（内存 SQL）转 BigDecimal 必须走字符串，否则 80.00 会变成 80.0 之外的长尾
            return new BigDecimal(String.valueOf(value));
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        return new BigDecimal(text);
    }

    public static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        return Double.valueOf(text);
    }

    public static Boolean toBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        if ("1".equals(text) || "true".equalsIgnoreCase(text) || "Y".equalsIgnoreCase(text)) {
            return Boolean.TRUE;
        }
        if ("0".equals(text) || "false".equalsIgnoreCase(text) || "N".equalsIgnoreCase(text)) {
            return Boolean.FALSE;
        }
        return Boolean.valueOf(text);
    }

    public static Timestamp toTimestamp(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.sql.Date) {
            return new Timestamp(((java.sql.Date) value).getTime());
        }
        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Timestamp(((Number) value).longValue());
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        return Timestamp.valueOf(text);
    }

    public static java.sql.Date toDate(Object value) {
        Timestamp timestamp = toTimestamp(value);
        return timestamp == null ? null : new java.sql.Date(timestamp.getTime());
    }
}
