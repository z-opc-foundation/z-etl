package com.zifang.z.etl.core.job;

/**
 * 聚合下推的一格（{@code GROUP BY} + 聚合函数）。
 *
 * <p>安全口径：聚合表达式**不是**自由 SQL，而是「函数名白名单 + 列名标识符白名单」的组合
 * （见 {@link #SAFE_FUNCTIONS} 与 {@code JobDescriptorValidator}）——
 * 聚合表达式没法参数绑定，一旦放开自由文本就是注入面（需求 008 notes.md §9.1 / §9.8）。
 *
 * @author zifang
 */
public class AggregateSpec {

    /** 允许的聚合函数白名单（小写比较）。 */
    public static final String[] SAFE_FUNCTIONS = {"SUM", "COUNT", "AVG", "MIN", "MAX"};

    /** 聚合函数：{@code SUM / COUNT / AVG / MIN / MAX}。 */
    private String function;

    /** 聚合列；{@code COUNT} 允许 {@code *}，其余必须是真实列名。 */
    private String column;

    /** 输出列名（别名）；为空时按「函数_列」生成。 */
    private String alias;

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    /** 归一后的函数名（大写）。 */
    public String effectiveFunction() {
        return function == null ? null : function.trim().toUpperCase();
    }

    /** 输出列名：显式别名优先，否则「函数_列」小写。 */
    public String effectiveAlias() {
        if (alias != null && !alias.trim().isEmpty()) {
            return alias.trim();
        }
        String columnPart = (column == null || column.trim().isEmpty()) ? "all" : column.trim();
        return (effectiveFunction() + "_" + columnPart).toLowerCase();
    }
}
