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
