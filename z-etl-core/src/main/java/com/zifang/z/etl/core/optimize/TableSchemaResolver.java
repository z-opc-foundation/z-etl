package com.zifang.z.etl.core.optimize;

import java.util.List;

/**
 * 表结构解析（优化器需要知道「源表到底有哪些列」才能安全改写计划）。
 *
 * <p>口径：<b>解析不出来就返回 {@code null}</b>（= 不知道）——
 * 优化器遇到「不知道」时一律保守（不摘节点 / 不裁列），绝不猜。
 *
 * @author zifang
 */
public interface TableSchemaResolver {

    /**
     * 某张表的列清单。
     *
     * @param dataSourceRef 数据源引用
     * @param table         表名（可带 schema 前缀）
     * @return 列名清单（小写）；解析不出来返回 {@code null}
     */
    List<String> columnsOf(String dataSourceRef, String table);
}
