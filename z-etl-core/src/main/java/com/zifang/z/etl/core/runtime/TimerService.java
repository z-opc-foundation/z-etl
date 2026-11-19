package com.zifang.z.etl.core.runtime;

/**
 * 定时器服务（Flink 的 {@code TimerService}）。
 *
 * <p>两种钟：
 * <ul>
 *   <li><b>事件时间</b>：水位推进到某个时间点时触发 ⇒ 与 Flink 的事件时间窗口同一口径
 *       （窗口为什么能关，靠的就是它）。</li>
 *   <li><b>处理时间</b>：墙上时钟到点触发 ⇒ 用于「不依赖事件时间」的场景（如超时兜底）。</li>
 * </ul>
 *
 * @author zifang
 */
public interface TimerService {

    /**
     * 注册事件时间定时器（水位推进到 {@code timestamp} 时触发）。
     *
     * @param timestamp 触发时间（毫秒）
     * @param name      定时器名（同一算子内唯一；重复注册同名定时器会覆盖）
     */
    void registerEventTimeTimer(long timestamp, String name);

    /**
     * 注册处理时间定时器（墙上时钟到 {@code timestamp} 时触发）。
     *
     * @param timestamp 触发时间（毫秒）
     * @param name      定时器名
     */
    void registerProcessingTimeTimer(long timestamp, String name);
}
