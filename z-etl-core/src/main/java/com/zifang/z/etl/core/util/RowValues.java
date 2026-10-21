package com.zifang.z.etl.core.util;

import java.util.Map;

/**
 * 行内取值的**大小写不敏感**助手。
 *
 * <p>为什么要它：不同 JDBC 驱动返回的列标签大小写不一致（H2 默认大写、MySQL 按建表定义），
 * 而 job 描述里的列名是人写的（通常小写）。若直接 {@code row.get("id")}，在 H2 上会拿到 null
 * ——这是「量具坏了伪装成产品坏了」的同类坑，必须在引擎层一次性收口。
 *
 * @author zifang
 */
public final class RowValues {

    private RowValues() {
    }

    /**
     * 按列名取值，先精确、再忽略大小写。
     *
     * @return 命中返回值；未命中返回 {@code null}
     */
    public static Object get(Map<String, Object> row, String column) {
        if (row == null || column == null) {
            return null;
        }
        if (row.containsKey(column)) {
            return row.get(column);
        }
        Object direct = row.get(column.toLowerCase());
        if (direct != null || row.containsKey(column.toLowerCase())) {
            return direct;
        }
        Object upper = row.get(column.toUpperCase());
        if (upper != null || row.containsKey(column.toUpperCase())) {
            return upper;
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(column)) {
                return entry.getValue();
            }
        }
        return null;
    }
}
