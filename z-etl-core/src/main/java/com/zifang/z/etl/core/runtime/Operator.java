package com.zifang.z.etl.core.runtime;

/**
 * 流水线算子（Flink 的 {@code AbstractStreamOperator} 简化面）。
 *
 * <p>生命周期：{@link #open} → {@link #processElement}（每条记录）→ {@link #processWatermark}（水位推进）
 * → {@link #onTimer}（定时器触发）→ {@link #close}。
 *
 * <p>算子只管「收一条、发若干条」，**不知道上下游是谁**（拓扑由执行器管）——
 * 这正是 Flink 让作业逻辑与执行计划解耦的那一刀。
 *
 * @author zifang
 */
public interface Operator extends AutoCloseable {

    /**
     * 打开（建状态、定时器、连接等）。
     *
     * @param context 算子上下文
     * @throws Exception 打开失败
     */
    void open(OperatorContext context) throws Exception;

    /**
     * 处理一条记录。
     *
     * @param record 记录
     * @param output 输出口
     * @throws Exception 处理失败
     */
    void processElement(Record record, Output output) throws Exception;

    /**
     * 处理一条记录（带**输入标签**）。
     *
     * <p>输入标签 = 上游节点 id（执行器在派发时带上）。连接算子靠它分辨
     * 「这条记录来自左路还是右路」；其余算子用不上（默认实现直接转发给两参版本）。
     *
     * @param record   记录
     * @param output   输出口
     * @param inputTag 上游节点 id（可能为 {@code null}）
     * @throws Exception 处理失败
     */
    default void processElement(Record record, Output output, String inputTag) throws Exception {
        processElement(record, output);
    }

    /**
     * 水位推进（Flink 的 {@code processWatermark}）。
     *
     * @param watermark 新水位（毫秒）
     * @throws Exception 处理失败
     */
    void processWatermark(long watermark) throws Exception;

    /**
     * 定时器触发（Flink 的 {@code onTimer}）。
     *
     * @param timestamp 定时器时间戳（毫秒）
     * @param timerName 定时器名
     * @throws Exception 处理失败
     */
    void onTimer(long timestamp, String timerName) throws Exception;

    /**
     * 关闭（刷批、关连接）。
     *
     * @throws Exception 关闭失败
     */
    void close() throws Exception;

    /** 算子 id（= 节点 id）。 */
    String operatorId();
}
