package com.zifang.z.etl.core.job;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL 下推（pushdown）声明：把计算**推到源库**做，而不是把明细拉回引擎再算。
 *
 * <p>三类下推（本仓实现的全部口径）：
 * <ul>
 *   <li><b>过滤下推</b>（{@link #filter}）：静态过滤条件进源库 {@code WHERE}，明细不过网。
 *       与 {@link SourceSpec#getWhere()} 同为「运维写死在作业里」的静态片段，不接受外部拼接。</li>
 *   <li><b>列裁剪下推</b>：{@link SourceSpec#getColumns()} 声明的列即投影，
 *       源库只返回用得上的列（本仓不额外引入 {@code SELECT *}）。</li>
 *   <li><b>聚合下推</b>（{@link #groupBy} + {@link #aggregates}）：
 *       {@code GROUP BY} + 聚合函数在源库算完，引擎只拿到聚合结果行。</li>
 * </ul>
 *
 * <p>纪律：聚合表达式是「函数白名单 + 标识符白名单」的组合（{@link AggregateSpec}），
 * 不接受自由 SQL 文本 —— 聚合表达式无法参数绑定，自由文本即注入面。
 *
 * @author zifang
 */
public class PushdownSpec {

    /** 过滤下推片段（静态 SQL 布尔表达式，运维写死在作业里）。 */
    private String filter;

    /** 聚合下推的分组列。 */
    private List<String> groupBy = new ArrayList<String>();

    /** 聚合下推的聚合格。 */
    private List<AggregateSpec> aggregates = new ArrayList<AggregateSpec>();

    /** 是否做 {@code SELECT DISTINCT}（配合聚合下推去重）。 */
    private Boolean distinct = Boolean.FALSE;

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy == null ? new ArrayList<String>() : groupBy;
    }

    public List<AggregateSpec> getAggregates() {
        return aggregates;
    }

    public void setAggregates(List<AggregateSpec> aggregates) {
        this.aggregates = aggregates == null ? new ArrayList<AggregateSpec>() : aggregates;
    }

    public Boolean getDistinct() {
        return distinct;
    }

    public void setDistinct(Boolean distinct) {
        this.distinct = distinct;
    }

    /** 是否是聚合下推（有分组列或聚合格）。 */
    public boolean isAggregatePushdown() {
        return (groupBy != null && !groupBy.isEmpty())
                || (aggregates != null && !aggregates.isEmpty());
    }

    /** 是否声明了过滤下推。 */
    public boolean hasFilter() {
        return filter != null && !filter.trim().isEmpty();
    }
}
