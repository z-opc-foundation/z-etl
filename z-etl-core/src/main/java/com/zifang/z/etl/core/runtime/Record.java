package com.zifang.z.etl.core.runtime;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 流中的一条记录（行信封）。
 *
 * <p>照抄 Flink 的 {@code StreamRecord} 思路：数据 + **事件时间** + 摄取时间 + 键。
 * 一条记录从头走到尾，事件时间与键一路带着（下游的窗口 / 键控状态都靠它）。
 *
 * @author zifang
 */
public final class Record {

    /** 没有事件时间的哨兵值。 */
    public static final long NO_EVENT_TIME = -1L;

    private final Map<String, Object> row;
    private final long eventTime;
    private final long ingestTime;
    private final String key;

    /**
     * @param row        行内容（列名 → 值）
     * @param eventTime  事件时间（毫秒）；{@link #NO_EVENT_TIME} 表示没有
     * @param ingestTime 摄取时间（毫秒）
     * @param key        分组键（{@code null} = 未分组）
     */
    public Record(Map<String, Object> row, long eventTime, long ingestTime, String key) {
        this.row = row == null ? new LinkedHashMap<String, Object>() : row;
        this.eventTime = eventTime;
        this.ingestTime = ingestTime;
        this.key = key;
    }

    /** 造一条无事件时间、无键的记录。 */
    public static Record of(Map<String, Object> row) {
        return new Record(row, NO_EVENT_TIME, System.currentTimeMillis(), null);
    }

    /** 行内容。 */
    public Map<String, Object> row() {
        return row;
    }

    /** 事件时间（毫秒）；没有则 {@link #NO_EVENT_TIME}。 */
    public long eventTime() {
        return eventTime;
    }

    /** 摄取时间（毫秒）。 */
    public long ingestTime() {
        return ingestTime;
    }

    /** 分组键；未分组为 {@code null}。 */
    public String key() {
        return key;
    }

    /** 是否有事件时间。 */
    public boolean hasEventTime() {
        return eventTime != NO_EVENT_TIME;
    }

    /** 换行内容（保留事件时间 / 摄取时间 / 键）。 */
    public Record withRow(Map<String, Object> newRow) {
        return new Record(newRow, eventTime, ingestTime, key);
    }

    /** 换键（保留其余）。 */
    public Record withKey(String newKey) {
        return new Record(row, eventTime, ingestTime, newKey);
    }

    /** 换事件时间（保留其余）。 */
    public Record withEventTime(long newEventTime) {
        return new Record(row, newEventTime, ingestTime, key);
    }

    @Override
    public String toString() {
        return "Record{key=" + key + ", eventTime=" + eventTime + ", row=" + row + "}";
    }
}
