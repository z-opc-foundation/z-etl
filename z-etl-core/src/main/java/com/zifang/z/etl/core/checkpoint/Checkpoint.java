package com.zifang.z.etl.core.checkpoint;

/**
 * 水位（增量位点）记录。
 *
 * <p>水位是引擎层的东西（需求 008 notes.md §9.6.1 E2）：零件面五个都没有位点概念。
 *
 * @author zifang
 */
public class Checkpoint {

    private String jobName;
    private String watermarkColumn;
    private String watermarkType;
    private String watermarkValue;
    private String lastMode;
    private long lastRunAt;

    public Checkpoint() {
    }

    public Checkpoint(String jobName, String watermarkColumn, String watermarkType, String watermarkValue,
                      String lastMode, long lastRunAt) {
        this.jobName = jobName;
        this.watermarkColumn = watermarkColumn;
        this.watermarkType = watermarkType;
        this.watermarkValue = watermarkValue;
        this.lastMode = lastMode;
        this.lastRunAt = lastRunAt;
    }

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

    /** 上次跑完的实际模式（init / incremental）——「首跑后自动切增量」的可见证据。 */
    public String getLastMode() {
        return lastMode;
    }

    public void setLastMode(String lastMode) {
        this.lastMode = lastMode;
    }

    public long getLastRunAt() {
        return lastRunAt;
    }

    public void setLastRunAt(long lastRunAt) {
        this.lastRunAt = lastRunAt;
    }
}
