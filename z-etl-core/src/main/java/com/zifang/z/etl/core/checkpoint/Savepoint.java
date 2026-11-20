package com.zifang.z.etl.core.checkpoint;

/**
 * 保存点（savepoint）：一条流水线「可搬运的全部运行时状态」的打包。
 *
 * <p>口径照抄 Flink 的 savepoint：把**键控状态快照 + 水位游标**导出成一个自描述的 JSON 报文，
 * 可以灌回同一条流水线（恢复），也可以灌到**另一个名字**下（作业迁移 / 复制）。
 *
 * <p>与普通检查点的差别：检查点是引擎**自动**滚动推进的续跑位点；保存点是**人工**导出导入的
 * 迁移载体 —— 导出纯读不动现场，导入是覆盖写（目标管道原有的状态与水位被替换）。
 *
 * @author zifang
 */
public class Savepoint {

    /** 报文格式版本（结构与灌回逻辑都按它解释）。 */
    private int version = 1;

    /** 导出时的流水线名。 */
    private String pipeline;

    /** 导出时间（epoch 毫秒）。 */
    private long savedAt;

    /** 键控状态快照字节（Base64；无状态管道为 {@code null}）。 */
    private String stateBase64;

    /** 水位列（无水位记录为 {@code null}）。 */
    private String watermarkColumn;

    /** 水位类型（LONG / TIMESTAMP）。 */
    private String watermarkType;

    /** 水位值（复合游标的主键值，文本形态）。 */
    private String watermarkValue;

    /** 平局列（可空）。 */
    private String tiebreakColumn;

    /** 平局列值（可空）。 */
    private String tiebreakValue;

    /** 上次运行模式（init / incremental）。 */
    private String lastMode;

    /** 上次运行时间（epoch 毫秒）。 */
    private long lastRunAt;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getPipeline() {
        return pipeline;
    }

    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    public long getSavedAt() {
        return savedAt;
    }

    public void setSavedAt(long savedAt) {
        this.savedAt = savedAt;
    }

    public String getStateBase64() {
        return stateBase64;
    }

    public void setStateBase64(String stateBase64) {
        this.stateBase64 = stateBase64;
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

    public String getTiebreakColumn() {
        return tiebreakColumn;
    }

    public void setTiebreakColumn(String tiebreakColumn) {
        this.tiebreakColumn = tiebreakColumn;
    }

    public String getTiebreakValue() {
        return tiebreakValue;
    }

    public void setTiebreakValue(String tiebreakValue) {
        this.tiebreakValue = tiebreakValue;
    }

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

    /** 有没有带状态快照。 */
    public boolean hasState() {
        return stateBase64 != null && !stateBase64.isEmpty();
    }

    /** 有没有带水位游标。 */
    public boolean hasWatermark() {
        return watermarkValue != null;
    }
}
