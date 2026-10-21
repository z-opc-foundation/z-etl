package com.zifang.z.etl.core.util;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * 校验和工具（对账第二口径）。
 *
 * <p>口径：**与顺序无关的键多重集校验和** —— 对每行的幂等键取值拼串做 FNV-1a 64 位散列，
 * 再把所有散列值求和。所以源侧与目标侧的校验和相等 ⟺ 两边的「幂等键多重集」相同
 * （重复行会被计入两次，因此「不重不漏」都能被它抓到）。
 *
 * @author zifang
 */
public final class Checksums {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private Checksums() {
    }

    /**
     * FNV-1a 64 位散列。
     *
     * @param text 文本
     * @return 散列值
     */
    public static long fnv1a64(String text) {
        long hash = 0xcbf29ce484222325L;
        byte[] bytes = text.getBytes(UTF8);
        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    /**
     * 单行的键散列（各键值以 {@code \u0001} 分隔，避免 "12"+"3" 与 "1"+"23" 撞车）。
     *
     * @param values 键值序列（按固定列序）
     * @return 散列值
     */
    public static long rowHash(Object[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append('\u0001');
            }
            Object value = values[i];
            sb.append(value == null ? "\u0000NULL" : value.toString());
        }
        return fnv1a64(sb.toString());
    }

    /**
     * 从一行里按列序取值并散列。
     *
     * @param row     行
     * @param columns 列名（大小写不敏感取列）
     * @return 散列值
     */
    public static long rowHash(Map<String, Object> row, java.util.List<String> columns) {
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            values[i] = RowValues.get(row, columns.get(i));
        }
        return rowHash(values);
    }
}
