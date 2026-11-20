package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.transform.TypeNormalizer;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

/**
 * 列类型处理：声明优先，其次推断（文件 / JSON / HTTP 这类「无 schema」输入的通用口径）。
 *
 * <p>推断顺序：布尔 → 整数 → 定点小数 → 时间戳 → 文本。
 * 推断只用于**没有声明**的列；声明了 {@code columnTypes} 的列一律按声明转。
 *
 * @author zifang
 */
public final class ColumnTypes {

    private ColumnTypes() {
    }

    /**
     * 取列的声明类型（没声明返回 {@code null}）。
     *
     * @param types 列类型声明（列名 → 类型名）
     * @param column 列名
     * @return 类型名
     */
    public static String declared(Map<String, Object> types, String column) {
        if (types == null || column == null) {
            return null;
        }
        Object value = types.get(column);
        if (value == null) {
            value = types.get(column.toLowerCase());
        }
        return Config.text(value);
    }

    /**
     * 把原始值转成声明类型（没声明就按文本推断）。
     *
     * @param value  原始值
     * @param type   声明类型（可空）
     * @return 转换后的值
     */
    public static Object convert(Object value, String type) {
        if (value == null) {
            return null;
        }
        if (type == null || type.trim().isEmpty()) {
            return infer(value);
        }
        return TypeNormalizer.normalize(value, type);
    }

    /**
     * 文本推断类型。
     *
     * @param value 原始值
     * @return 类型名（{@link TypeNames} 口径）
     */
    public static String inferType(Object value) {
        if (value == null) {
            return "STRING";
        }
        if (value instanceof Boolean) {
            return "BOOLEAN";
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Short) {
            return "LONG";
        }
        if (value instanceof BigDecimal) {
            return "BIGDECIMAL";
        }
        if (value instanceof Double || value instanceof Float) {
            return "DOUBLE";
        }
        if (value instanceof Timestamp || value instanceof java.util.Date) {
            return "TIMESTAMP";
        }
        return inferType(String.valueOf(value).trim());
    }

    /**
     * 文本推断类型（{@code LONG} / {@code BIGDECIMAL} / {@code DOUBLE} / {@code BOOLEAN} /
     * {@code TIMESTAMP} / {@code STRING}）。
     *
     * @param text 原始文本
     * @return 类型名
     */
    public static String inferType(String text) {
        if (text == null || text.isEmpty()) {
            return "STRING";
        }
        String value = text.trim();
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return "BOOLEAN";
        }
        if (isInteger(value)) {
            return "LONG";
        }
        if (isDecimal(value)) {
            return "BIGDECIMAL";
        }
        if (isTimestamp(value)) {
            return "TIMESTAMP";
        }
        return "STRING";
    }

    /**
     * 值（无声明类型时）按推断转成结构化值。
     *
     * @param value 原始值
     * @return 转换后的值
     */
    public static Object infer(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number || value instanceof Boolean
                || value instanceof Timestamp || value instanceof java.util.Date) {
            return value;
        }
        String type = inferType(String.valueOf(value));
        if ("STRING".equals(type)) {
            return value;
        }
        return TypeNormalizer.normalize(value, type);
    }

    private static boolean isInteger(String value) {
        if (value.isEmpty()) {
            return false;
        }
        int start = value.charAt(0) == '-' || value.charAt(0) == '+' ? 1 : 0;
        if (start >= value.length()) {
            return false;
        }
        for (int i = start; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDecimal(String value) {
        if (value.isEmpty()) {
            return false;
        }
        boolean dot = false;
        int digits = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '-' || ch == '+') {
                if (i != 0) {
                    return false;
                }
                continue;
            }
            if (ch == '.') {
                if (dot) {
                    return false;
                }
                dot = true;
                continue;
            }
            if (!Character.isDigit(ch)) {
                return false;
            }
            digits++;
        }
        return dot && digits > 0;
    }

    private static boolean isTimestamp(String value) {
        if (value.length() < 10) {
            return false;
        }
        try {
            Timestamp.valueOf(value.replace('T', ' '));
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
