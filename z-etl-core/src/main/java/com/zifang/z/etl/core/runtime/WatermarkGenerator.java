package com.zifang.z.etl.core.runtime;

/**
 * 水位生成器（Flink 的 {@code WatermarkGenerator}）。
 *
 * <p>水位 = 「事件时间上界 − 乱序容忍」。事件时间越往后走，水位越往后推；
 * 水位推过某个窗口的结束点 ⇒ 该窗口可以结算了。
 *
 * @author zifang
 */
public interface WatermarkGenerator {

    /**
     * 收到一条事件时间戳后的新水位。
     *
     * @param eventTime 本条记录的事件时间（毫秒）
     * @return 新水位（毫秒）
     */
    long onEvent(long eventTime);

    /**
     * 有界乱序水位生成器（{@code BoundedOutOfOrdernessWatermarks}）。
     *
     * <p>口径：水位 = 观察到的最大事件时间 − 乱序容忍窗口。源表里「时间戳乱着写」时，
     * 乱序容忍窗口要盖住最大乱序幅度，否则窗口会被提前结算、把迟到数据甩掉。
     */
    final class BoundedOutOfOrderness implements WatermarkGenerator {

        private final long maxOutOfOrderness;
        private long maxEventTime = Long.MIN_VALUE;

        /**
         * @param maxOutOfOrderness 乱序容忍窗口（毫秒）
         */
        public BoundedOutOfOrderness(long maxOutOfOrderness) {
            this.maxOutOfOrderness = Math.max(0L, maxOutOfOrderness);
        }

        @Override
        public long onEvent(long eventTime) {
            if (eventTime > maxEventTime) {
                maxEventTime = eventTime;
            }
            return currentWatermark();
        }

        /** 当前水位。 */
        public long currentWatermark() {
            if (maxEventTime == Long.MIN_VALUE) {
                return Long.MIN_VALUE;
            }
            return maxEventTime - maxOutOfOrderness;
        }
    }
}
