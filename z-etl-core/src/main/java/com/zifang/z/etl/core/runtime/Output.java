package com.zifang.z.etl.core.runtime;

/**
 * 算子输出口（Flink 的 {@code Collector}）。
 *
 * <p>主输出 + 旁路输出两个出口：主输出往下游节点走；旁路输出按 {@link OutputTag} 的 id
 * 找到对应节点投递（找不到就丢弃并计数，不静默吞）。
 *
 * @author zifang
 */
public interface Output {

    /**
     * 发主输出。
     *
     * @param record 记录
     */
    void collect(Record record);

    /**
     * 发旁路输出。
     *
     * @param tag    旁路标签（id = 目标节点 id）
     * @param record 记录
     */
    void collect(OutputTag tag, Record record);
}
