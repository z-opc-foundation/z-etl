package com.zifang.z.etl.core.job;

/**
 * job 描述的源端定义（读）。
 *
 * <p>{@code dataSourceRef} 引用 z-dms 的 {@code DataSourceDef}（需求 008 D2：z-etl 是 z-dms 的第一个重度消费者）。
 * z-dms 仓本轮尚未落地，故本仓用 {@link com.zifang.z.etl.core.spi.DataSourceProvider} SPI 承接，
 * z-dms 落地后由它提供实现即可，job 描述格式不变。
 *
 * @author zifang
 */
public class SourceSpec {

    /** 数据源引用（z-dms DataSourceDef 的 ref / 名称）。 */
    private String dataSourceRef;

    /** 源表名（支持 {@code schema.table} 两段，走 z-util-jdbc 的标识符白名单校验）。 */
    private String table;

    /** 读取列；为空表示全列。 */
    private java.util.List<String> columns;

    /** 增量水位列（源侧列名），为空表示该 job 无增量语义。 */
    private String watermarkColumn;

    /** 静态过滤条件（SQL 片段）；只允许运维在 job 定义里写死，不接受外部拼接。 */
    private String where;

    /** 排序列（保证分页/水位扫描稳定）。 */
    private String orderBy;

    /** 单页行数（全量模式为分页游标步长；增量模式为水位过滤后的批大小）。 */
    private int pageSize = 500;

    /** SQL 下推声明（过滤下推 / 聚合下推）；为空表示不下推。 */
    private PushdownSpec pushdown;

    public PushdownSpec getPushdown() {
        return pushdown;
    }

    public void setPushdown(PushdownSpec pushdown) {
        this.pushdown = pushdown;
    }

    public String getDataSourceRef() {
        return dataSourceRef;
    }

    public void setDataSourceRef(String dataSourceRef) {
        this.dataSourceRef = dataSourceRef;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public java.util.List<String> getColumns() {
        return columns;
    }

    public void setColumns(java.util.List<String> columns) {
        this.columns = columns;
    }

    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    public void setWatermarkColumn(String watermarkColumn) {
        this.watermarkColumn = watermarkColumn;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
