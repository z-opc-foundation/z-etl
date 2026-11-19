package com.zifang.z.etl.core.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 算子级定时器队列（事件时间 + 处理时间）。
 *
 * <p>执行器在水位推进 / 流结束时调 {@link #fireEventTimeDue}，把到点的定时器交给算子的
 * {@link Operator#onTimer}。同刻定时器按注册顺序 FIFO（定序口径，重跑结果可复现）。
 *
 * @author zifang
 */
public class OperatorTimers implements TimerService {

    /** 注册序号（同刻定时器 FIFO 用）。 */
    private static final AtomicLong SEQ = new AtomicLong();

    /** 事件时间定时器队列（按触发时间升序）。 */
    private final PriorityQueue<Timer> eventTimers = new PriorityQueue<Timer>(11, new TimerComparator());

    /** 处理时间定时器队列。 */
    private final PriorityQueue<Timer> processingTimers = new PriorityQueue<Timer>(11, new TimerComparator());

    @Override
    public void registerEventTimeTimer(long timestamp, String name) {
        eventTimers.add(new Timer(timestamp, name, true, SEQ.incrementAndGet()));
    }

    @Override
    public void registerProcessingTimeTimer(long timestamp, String name) {
        processingTimers.add(new Timer(timestamp, name, false, SEQ.incrementAndGet()));
    }

    /**
     * 取到点的事件时间定时器（并移除）。
     *
     * @param watermark 当前水位
     * @return 到点的定时器列表
     */
    public List<Timer> fireEventTimeDue(long watermark) {
        return drain(eventTimers, watermark);
    }

    /**
     * 取到点的处理时间定时器（并移除）。
     *
     * @param now 墙上时钟
     * @return 到点的定时器列表
     */
    public List<Timer> fireProcessingTimeDue(long now) {
        return drain(processingTimers, now);
    }

    /** 还有多少定时器没触发。 */
    public int pending() {
        return eventTimers.size() + processingTimers.size();
    }

    private List<Timer> drain(PriorityQueue<Timer> queue, long threshold) {
        List<Timer> due = new ArrayList<Timer>();
        while (!queue.isEmpty()) {
            Timer timer = queue.peek();
            if (timer.getTimestamp() <= threshold) {
                queue.poll();
                due.add(timer);
            } else {
                break;
            }
        }
        return due;
    }

    /** 一个定时器。 */
    public static final class Timer {

        private final long timestamp;
        private final String name;
        private final boolean eventTime;
        private final long sequence;

        Timer(long timestamp, String name, boolean eventTime, long sequence) {
            this.timestamp = timestamp;
            this.name = name;
            this.eventTime = eventTime;
            this.sequence = sequence;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getName() {
            return name;
        }

        public boolean isEventTime() {
            return eventTime;
        }

        long sequence() {
            return sequence;
        }

        @Override
        public String toString() {
            return "Timer{" + name + "@" + timestamp + (eventTime ? ", event" : ", processing") + "}";
        }
    }

    /** 按触发时间升序；同刻按注册顺序（FIFO）。 */
    private static final class TimerComparator implements Comparator<Timer> {

        @Override
        public int compare(Timer left, Timer right) {
            if (left.getTimestamp() != right.getTimestamp()) {
                return left.getTimestamp() < right.getTimestamp() ? -1 : 1;
            }
            return Long.compare(left.sequence(), right.sequence());
        }
    }
}
