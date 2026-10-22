package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.reconcile.ReconciliationReport;

import java.util.ArrayList;
import java.util.List;

/**
 * 一次作业运行的结果。
 *
 * @author zifang
 */
public class EtlRunResult {

    private String jobName;
    /** 实际执行模式：{@code init} / {@code incremental}。 */
    private String mode;
    private long rowsRead;
    private long rowsWritten;
    private String watermarkBefore;
    private String watermarkAfter;
    private long startedAt;
    private long finishedAt;
    private boolean reconciled;
    private ReconciliationReport report;
    /** 游标越界（源表被清 / 水位跑到源数据前面）。 */
    private boolean watermarkOutOfRange;
    /** 源侧水位早于当前游标的行数（时钟回拨 / 迟到数据 ⇒ 纯时间游标搬不到的那些行）。 */
    private long staleRows;
    /** 本轮推进水位的次数（= 成功提交并落水位的页数）。 */
    private int checkpointAdvances;
    /** 复合游标的平局列值（推进后的）。 */
    private String watermarkTiebreakAfter;
    private final List<String> notes = new ArrayList<String>();

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public long getRowsRead() {
        return rowsRead;
    }

    public void setRowsRead(long rowsRead) {
        this.rowsRead = rowsRead;
    }

    public long getRowsWritten() {
        return rowsWritten;
    }

    public void setRowsWritten(long rowsWritten) {
        this.rowsWritten = rowsWritten;
    }

    public String getWatermarkBefore() {
        return watermarkBefore;
    }

    public void setWatermarkBefore(String watermarkBefore) {
        this.watermarkBefore = watermarkBefore;
    }

    public String getWatermarkAfter() {
        return watermarkAfter;
    }

    public void setWatermarkAfter(String watermarkAfter) {
        this.watermarkAfter = watermarkAfter;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(long finishedAt) {
        this.finishedAt = finishedAt;
    }

    public long durationMs() {
        return finishedAt - startedAt;
    }

    public boolean isReconciled() {
        return reconciled;
    }

    public void setReconciled(boolean reconciled) {
        this.reconciled = reconciled;
    }

    /** 游标越界（源表被清 / 水位跑到源数据前面）。 */
    public boolean isWatermarkOutOfRange() {
        return watermarkOutOfRange;
    }

    public void setWatermarkOutOfRange(boolean watermarkOutOfRange) {
        this.watermarkOutOfRange = watermarkOutOfRange;
    }

    /** 源侧水位早于当前游标的行数（时钟回拨 / 迟到数据的滞留量）。 */
    public long getStaleRows() {
        return staleRows;
    }

    public void setStaleRows(long staleRows) {
        this.staleRows = staleRows;
    }

    /** 本轮推进水位的次数（= 成功提交并落水位的页数）。 */
    public int getCheckpointAdvances() {
        return checkpointAdvances;
    }

    public void setCheckpointAdvances(int checkpointAdvances) {
        this.checkpointAdvances = checkpointAdvances;
    }

    /** 复合游标的平局列值（推进后的）。 */
    public String getWatermarkTiebreakAfter() {
        return watermarkTiebreakAfter;
    }

    public void setWatermarkTiebreakAfter(String watermarkTiebreakAfter) {
        this.watermarkTiebreakAfter = watermarkTiebreakAfter;
    }

    public ReconciliationReport getReport() {
        return report;
    }

    public void setReport(ReconciliationReport report) {
        this.report = report;
    }

    public List<String> getNotes() {
        return notes;
    }

    public void addNote(String note) {
        if (note != null) {
            notes.add(note);
        }
    }

    @Override
    public String toString() {
        return "作业 " + jobName + " [" + mode + "] 读 " + rowsRead + " 行 / 写 " + rowsWritten
                + " 行，水位 " + watermarkBefore + " → " + watermarkAfter
                + "，对账 " + (reconciled ? "一致" : "不一致");
    }
}
