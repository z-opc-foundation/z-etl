package com.zifang.z.etl.core.operator;

import java.util.Map;

/**
 * 聚合函数声明（{@code fn} + {@code column} + {@code alias}）。
 *
 * <p>函数名走**白名单**（照抄旧引擎聚合下推的口径）：{@code SUM / COUNT / AVG / MIN / MAX /
 * FIRST / LAST}。不接受自由 SQL 文本 —— 聚合表达式无法参数绑定，拼文本就是注入面。
 *
 * @author zifang
 */
public class AggregateDef {

    private final String fn;
    private final String column;
    private final String alias;

    /**
     * @param fn     聚合函数（大写）
     * @param column 源列（{@code *} 表示 COUNT(*))
     * @param alias  输出列名（为空则用 {@code fn_列名}）
     */
    public AggregateDef(String fn, String column, String alias) {
        this.fn = fn == null ? null : fn.trim().toUpperCase();
        this.column = column == null ? null : column.trim();
        this.alias = alias;
    }

    /** 从配置项构造。 */
    public static AggregateDef from(Map<String, Object> item) {
        String fn = MapOperator.text(item.get("fn"));
        if (fn == null) {
            fn = MapOperator.text(item.get("function"));
        }
        String column = MapOperator.text(item.get("column"));
        String alias = MapOperator.text(item.get("alias"));
        if (fn == null) {
            throw new IllegalArgumentException("聚合声明缺少 fn");
        }
        AggregateDef def = new AggregateDef(fn, column == null ? "*" : column, alias);
        def.validate();
        return def;
    }

    /** 校验函数名在白名单内。 */
    public void validate() {
        if (!Accumulator.FN_SUM.equals(fn) && !Accumulator.FN_COUNT.equals(fn)
                && !Accumulator.FN_AVG.equals(fn) && !Accumulator.FN_MIN.equals(fn)
                && !Accumulator.FN_MAX.equals(fn) && !Accumulator.FN_FIRST.equals(fn)
                && !Accumulator.FN_LAST.equals(fn)) {
            throw new IllegalArgumentException("聚合函数不在白名单内: " + fn
                    + "（可用: SUM/COUNT/AVG/MIN/MAX/FIRST/LAST）");
        }
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("聚合声明缺少 column");
        }
    }

    /** 聚合函数。 */
    public String fn() {
        return fn;
    }

    /** 源列。 */
    public String column() {
        return column;
    }

    /** 输出列名。 */
    public String alias() {
        if (alias != null && !alias.isEmpty()) {
            return alias;
        }
        return fn.toLowerCase() + "_" + column;
    }

    /** 是否 COUNT(*)。 */
    public boolean isCountStar() {
        return Accumulator.FN_COUNT.equals(fn) && "*".equals(column);
    }
}
