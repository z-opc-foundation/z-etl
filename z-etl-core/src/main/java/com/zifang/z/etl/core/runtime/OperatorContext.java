package com.zifang.z.etl.core.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 算子上下文（算子能看见的「世界」）。
 *
 * <p>给算子的东西只有三样：**状态**、**定时器**、**当前水位**（外加计数器）。
 * 算子拿不到下游是谁、也拿不到作业全貌 ⇒ 同一个算子实现可以在任何图里复用。
 *
 * @author zifang
 */
public class OperatorContext {

    private final String operatorId;
    private final StateBackend.KeyedStateStore state;
    private final TimerService timers;
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<String, AtomicLong>();
    private volatile long currentWatermark = Long.MIN_VALUE;

    /**
     * @param operatorId 算子 id
     * @param state      键控状态（无状态算子传 {@code null}）
     * @param timers     定时器服务（无定时器需求传 {@code null}）
     */
    public OperatorContext(String operatorId, StateBackend.KeyedStateStore state, TimerService timers) {
        this.operatorId = operatorId;
        this.state = state;
        this.timers = timers;
    }

    /** 算子 id。 */
    public String operatorId() {
        return operatorId;
    }

    /** 键控状态；无状态算子返回 {@code null}。 */
    public StateBackend.KeyedStateStore state() {
        return state;
    }

    /** 定时器服务；返回 {@code null} 表示本算子不用定时器。 */
    public TimerService timers() {
        return timers;
    }

    /** 当前水位（毫秒）；还没推进过时是 {@link Long#MIN_VALUE}。 */
    public long currentWatermark() {
        return currentWatermark;
    }

    /** 执行器推进水位时回调。 */
    public void setCurrentWatermark(long watermark) {
        this.currentWatermark = watermark;
    }

    /** 计数器 +1。 */
    public void count(String name) {
        counter(name).incrementAndGet();
    }

    /** 计数器 +n。 */
    public void count(String name, long delta) {
        counter(name).addAndGet(delta);
    }

    /** 取计数器当前值。 */
    public long counterValue(String name) {
        AtomicLong counter = counters.get(name);
        return counter == null ? 0L : counter.get();
    }

    /** 记一段处理耗时（纳秒；落进计数器表，键 {@code elapsed-nanos}）。 */
    public void addElapsedNanos(long deltaNanos) {
        if (deltaNanos > 0L) {
            counter("elapsed-nanos").addAndGet(deltaNanos);
        }
    }

    /** 本算子累计处理耗时（纳秒）。 */
    public long elapsedNanos() {
        AtomicLong counter = counters.get("elapsed-nanos");
        return counter == null ? 0L : counter.get();
    }

    /** 计数器快照。 */
    public Map<String, Long> counters() {
        Map<String, Long> snapshot = new java.util.LinkedHashMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
            snapshot.put(entry.getKey(), Long.valueOf(entry.getValue().get()));
        }
        return snapshot;
    }

    private AtomicLong counter(String name) {
        AtomicLong counter = counters.get(name);
        if (counter == null) {
            synchronized (counters) {
                counter = counters.get(name);
                if (counter == null) {
                    counter = new AtomicLong();
                    counters.put(name, counter);
                }
            }
        }
        return counter;
    }
}
