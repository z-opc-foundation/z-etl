package com.zifang.z.etl.core.engine;

/**
 * 数据源登记条目（{@code etl_datasource} 一行）。
 *
 * <p>数据源管理页的落库口径：流水线声明里的 {@code dataSourceRef} 按名指向这里的 {@code name}。
 * 凭据属运行时数据（落本域库），管控最终归 z-dms/z-mist；对外接口一律掩码，不出网。
 *
 * @author zifang
 */
public class DataSourceDef {

    private String name;
    private String type = "jdbc";
    private String url;
    private String username;
    private String password;
    private String description;
    /** 台账附加信息（JSON 文本）：显示名等非密元数据，不参与连接。 */
    private String extra;
    private long createdAt;
    private long updatedAt;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
