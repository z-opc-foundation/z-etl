package com.zifang.z.etl.core.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 窗口分配器（Flink 的 {@code WindowAssigner}）。
 *
 * <p>四种：
 * <ul>
 *   <li>{@code tumbling} 滚动窗口：固定长度，等分切段（{@code TumblingEventTimeWindows}）；</li>
 *   <li>{@code sliding} 滑动窗口：固定长度 + 滑动步长（{@code SlidingEventTimeWindows}）；</li>
 *   <li>{@code session} 会话窗口：按不活跃间隔切分（{@code EventTimeSessionWindows}）；</li>
 *   <li>{@code global} 全局窗口：整段流一个窗口，流结束才结算。</li>
 * </ul>
 *
 * <p>会话窗口的「合并」不在分配器里做（分配器无状态）⇒ 分配器只给候选窗口，
 * 合并由窗口算子按 key 维护（见 {@code WindowAggregateOperator}）。
 *
 * @author zifang
 */
public final class WindowAssigners {

    private WindowAssigners() {
    }

    /** 窗口分配器。 */
    public interface WindowAssigner {

        /**
         * 给一条记录分配窗口。
         *
         * @param eventTime 事件时间（毫秒）
         * @return 命中的窗口（可能为空）
         */
        Collection<TimeWindow> assign(long eventTime);
    }

    /**
     * 滚动窗口。
     *
     * @param size   窗口长度（毫秒）
     * @param offset 窗口对齐偏移（毫秒；默认 0）
     * @return 分配器
     */
    public static WindowAssigner tumbling(long size, long offset) {
        final long windowSize = Math.max(1L, size);
        final long shift = offset;
        return new WindowAssigner() {
            @Override
            public Collection<TimeWindow> assign(long eventTime) {
                long start = eventTime - Math.floorMod(eventTime - shift, windowSize);
                return Collections.singletonList(new TimeWindow(start, start + windowSize));
            }
        };
    }

    /**
     * 滑动窗口。
     *
     * @param size   窗口长度（毫秒）
     * @param slide  滑动步长（毫秒）
     * @param offset 对齐偏移（毫秒）
     * @return 分配器
     */
    public static WindowAssigner sliding(long size, long slide, long offset) {
        final long windowSize = Math.max(1L, size);
        final long slideSize = Math.max(1L, slide);
        final long shift = offset;
        return new WindowAssigner() {
            @Override
            public Collection<TimeWindow> assign(long eventTime) {
                List<TimeWindow> windows = new ArrayList<TimeWindow>();
                long start = eventTime - Math.floorMod(eventTime - shift, slideSize);
                for (long begin = start; begin + windowSize > eventTime; begin -= slideSize) {
                    windows.add(new TimeWindow(begin, begin + windowSize));
                    if (windows.size() > 10000) {
                        break;
                    }
                }
                return windows;
            }
        };
    }

    /**
     * 会话窗口（按不活跃间隔切分；实际合并由窗口算子按 key 做）。
     *
     * @param gap 不活跃间隔（毫秒）
     * @return 分配器
     */
    public static WindowAssigner session(long gap) {
        final long sessionGap = Math.max(1L, gap);
        return new WindowAssigner() {
            @Override
            public Collection<TimeWindow> assign(long eventTime) {
                return Collections.singletonList(new TimeWindow(eventTime, eventTime + sessionGap));
            }
        };
    }

    /** 全局窗口（流结束才结算）。 */
    public static WindowAssigner global() {
        return new WindowAssigner() {
            @Override
            public Collection<TimeWindow> assign(long eventTime) {
                return Collections.singletonList(new TimeWindow(Long.MIN_VALUE, Long.MAX_VALUE));
            }
        };
    }

    /**
     * 取模并保证非负（{@code Math.floorMod} 是 Java 8+，此处自己实现一份以免版本歧义）。
     *
     * @param value 被除数
     * @param mod   除数（正）
     * @return 非负余数
     */
    static long floorMod(long value, long mod) {
        long result = value % mod;
        return result < 0 ? result + mod : result;
    }
}
