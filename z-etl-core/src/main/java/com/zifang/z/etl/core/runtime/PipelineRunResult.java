package com.zifang.z.etl.core.runtime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 流水线运行结果。
 *
 * <p>观测三件套：读了多少、写了多少、每个节点各跑了什么（计数器快照）。
 * 外加水位前后值与状态恢复标记 —— 断点续传的可观测面。
 *
 * @author zifang
 */
public class PipelineRunResult {

    /** 运行成功。 */
    public static final String STATUS_SUCCESS = "SUCCESS";
    /** 运行失败（重试耗尽后的最后一次失败也落履历）。 */
    public static final String STATUS_FAILED = "FAILED";

    private String pipelineName;
    private String mode;
    private String status = STATUS_SUCCESS;
    private long startedAt;
    private long finishedAt;
    private long recordsRead;
    private long recordsWritten;
    private boolean restored;
    private String watermarkBefore;
    private String watermarkAfter;

    /** 节点 id → 计数器快照（records-in / emitted / late-dropped / …）。 */
    private Map<String, Map<String, Long>> nodeMetrics = new LinkedHashMap<String, Map<String, Long>>();

    /** 运行备注（模式判定 / 恢复情况等）。 */
    private List<String> notes = new ArrayList<String>();

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    /** 耗时（毫秒）。 */
    public long durationMs() {
        return finishedAt - startedAt;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public void setRecordsRead(long recordsRead) {
        this.recordsRead = recordsRead;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public void setRecordsWritten(long recordsWritten) {
        this.recordsWritten = recordsWritten;
    }

    /** 是否从状态快照恢复过。 */
    public boolean isRestored() {
        return restored;
    }

    public void setRestored(boolean restored) {
        this.restored = restored;
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

    public Map<String, Map<String, Long>> getNodeMetrics() {
        return nodeMetrics;
    }

    public void setNodeMetrics(Map<String, Map<String, Long>> nodeMetrics) {
        this.nodeMetrics = nodeMetrics == null
                ? new LinkedHashMap<String, Map<String, Long>>() : nodeMetrics;
    }

    /** 挂一个节点的计数器快照。 */
    public void putNodeMetrics(String nodeId, Map<String, Long> counters) {
        nodeMetrics.put(nodeId, counters);
    }

    public List<String> getNotes() {
        return notes;
    }

    public void setNotes(List<String> notes) {
        this.notes = notes == null ? new ArrayList<String>() : notes;
    }

    /** 加一条运行备注。 */
    public void addNote(String note) {
        notes.add(note);
    }
}
