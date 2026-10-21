package com.zifang.z.etl.core.mapper;

/**
 * 水位视图（MyBatis 读面）。
 *
 * @author zifang
 */
public class EtlCheckpointView {

    private String jobName;
    private String watermarkColumn;
    private String watermarkType;
    private String watermarkValue;
    private String lastMode;
    private Long lastRunAt;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    public void setWatermarkColumn(String watermarkColumn) {
        this.watermarkColumn = watermarkColumn;
    }

    public String getWatermarkType() {
        return watermarkType;
    }

    public void setWatermarkType(String watermarkType) {
        this.watermarkType = watermarkType;
    }

    public String getWatermarkValue() {
        return watermarkValue;
    }

    public void setWatermarkValue(String watermarkValue) {
        this.watermarkValue = watermarkValue;
    }

    public String getLastMode() {
        return lastMode;
    }

    public void setLastMode(String lastMode) {
        this.lastMode = lastMode;
    }

    public Long getLastRunAt() {
        return lastRunAt;
    }

    public void setLastRunAt(Long lastRunAt) {
        this.lastRunAt = lastRunAt;
    }
}
