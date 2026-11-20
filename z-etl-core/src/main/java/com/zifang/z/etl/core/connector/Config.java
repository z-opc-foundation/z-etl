package com.zifang.z.etl.core.connector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 连接器侧的取值助手（配置里的自由结构 → 引擎侧的类型安全取值）。
 *
 * @author zifang
 */
public final class Config {

    private Config() {
    }

    /** 取文本（null / 空串 → {@code null}）。 */
    public static String text(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    /** 取长整型（取不到给缺省）。 */
    public static long asLong(Object value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /** 取整型（取不到给缺省）。 */
    public static int asInt(Object value, int defaultValue) {
        return (int) asLong(value, defaultValue);
    }

    /** 取布尔（认 {@code true/1/yes}）。 */
    public static boolean asBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }
        String text = String.valueOf(value).trim();
        if ("true".equalsIgnoreCase(text) || "1".equals(text) || "yes".equalsIgnoreCase(text)) {
            return true;
        }
        if ("false".equalsIgnoreCase(text) || "0".equals(text) || "no".equalsIgnoreCase(text)) {
            return false;
        }
        return defaultValue;
    }

    /** 取字符串列表（配置里写单值也当单元素列表）。 */
    public static List<String> strings(Object value) {
        List<String> result = new ArrayList<String>();
        if (value == null) {
            return result;
        }
        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                String text = text(item);
                if (text != null) {
                    result.add(text);
                }
            }
        } else {
            String text = text(value);
            if (text != null) {
                result.add(text);
            }
        }
        return result;
    }

    /** 取映射（不是映射就当空）。 */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> map(Object value) {
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return new LinkedHashMap<String, Object>();
    }
}
