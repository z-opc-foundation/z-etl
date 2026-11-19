package com.zifang.z.etl.core.runtime;

/**
 * 事件时间窗口（Flink 的 {@code TimeWindow}）：左闭右开 {@code [start, end)}。
 *
 * @author zifang
 */
public final class TimeWindow {

    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /** 窗口起点（含）。 */
    public long start() {
        return start;
    }

    /** 窗口终点（不含）。 */
    public long end() {
        return end;
    }

    /** 时间戳是否落在窗口内。 */
    public boolean contains(long timestamp) {
        return timestamp >= start && timestamp < end;
    }

    /** 与另一个窗口是否相交。 */
    public boolean intersects(TimeWindow other) {
        return other != null && start < other.end && other.start < end;
    }

    /** 合并两个窗口（取外包）。 */
    public TimeWindow merge(TimeWindow other) {
        return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TimeWindow)) {
            return false;
        }
        TimeWindow that = (TimeWindow) other;
        return start == that.start && end == that.end;
    }

    @Override
    public int hashCode() {
        return (int) (start ^ (start >>> 32)) * 31 + (int) (end ^ (end >>> 32));
    }

    @Override
    public String toString() {
        return "[" + start + ", " + end + ")";
    }
}
