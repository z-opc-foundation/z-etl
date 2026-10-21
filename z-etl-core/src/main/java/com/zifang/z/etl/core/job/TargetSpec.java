package com.zifang.z.etl.core.job;

/**
 * job 描述的目标端定义（写）。
 *
 * @author zifang
 */
public class TargetSpec {

    /** 数据源引用（z-dms DataSourceDef 的 ref / 名称）。 */
    private String dataSourceRef;

    /** 目标表名（支持 {@code schema.table} 两段）。 */
    private String table;

    /** 批量写出的行数阈值（背压的第一道闸）。 */
    private int batchSize = 200;

    /** 写模式：{@code upsert}（幂等键 upsert，默认）/ {@code insert}（只插不更新）。 */
    private String writeMode = "upsert";

    /** 目标表列；为空表示由字段映射推导。 */
    private java.util.List<String> columns;

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

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public java.util.List<String> getColumns() {
        return columns;
    }

    public void setColumns(java.util.List<String> columns) {
        this.columns = columns;
    }
}
