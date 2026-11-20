package com.zifang.z.etl.core.connector;

import java.util.ArrayList;
import java.util.List;

/**
 * CSV 解析 / 生成的最小实现（零依赖）。
 *
 * <p>口径：RFC 4180 味道 —— 字段可用引号包住，引号里可含分隔符与换行，
 * 双写两个引号表示一个引号（{@code ""}）。
 *
 * @author zifang
 */
public final class CsvSupport {

    private CsvSupport() {
    }

    /**
     * 解析一行 CSV。
     *
     * @param line      原始行
     * @param delimiter 分隔符
     * @param quote     引号字符
     * @return 字段列表
     */
    public static List<String> parseLine(String line, char delimiter, char quote) {
        List<String> fields = new ArrayList<String>();
        if (line == null) {
            return fields;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (inQuote) {
                if (ch == quote) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                        current.append(quote);
                        i++;
                    } else {
                        inQuote = false;
                    }
                } else {
                    current.append(ch);
                }
                continue;
            }
            if (ch == quote) {
                inQuote = true;
                continue;
            }
            if (ch == delimiter) {
                fields.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        fields.add(current.toString());
        return fields;
    }

    /**
     * 把一行渲染成 CSV 文本。
     *
     * @param values    字段值
     * @param delimiter 分隔符
     * @param quote     引号字符
     * @return CSV 文本
     */
    public static String formatRow(List<String> values, char delimiter, char quote) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(formatValue(values.get(i), quote));
        }
        return sb.toString();
    }

    /**
     * 单个字段的转义（含分隔符 / 引号 / 换行就加引号）。
     *
     * @param value 字段值
     * @param quote 引号字符
     * @return 转义后的文本
     */
    public static String formatValue(String value, char quote) {
        if (value == null) {
            return "";
        }
        boolean needQuote = value.indexOf(quote) >= 0 || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0;
        if (!needQuote) {
            return value;
        }
        StringBuilder sb = new StringBuilder(value.length() + 4);
        sb.append(quote);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == quote) {
                sb.append(quote);
            }
            sb.append(ch);
        }
        sb.append(quote);
        return sb.toString();
    }
}
