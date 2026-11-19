package com.zifang.z.etl.core.runtime;

/**
 * 流水线状态存储 SPI（检查点快照的落点）。
 *
 * <p>状态后端（{@link StateBackend}）管「状态长什么样」，这里管「快照存哪里」：
 * 堆内后端序列化出的字节交给它落库，重启再灌回来 ⇒ 断点续传。
 *
 * @author zifang
 */
public interface PipelineStateStore {

    /**
     * 读快照。
     *
     * @param pipelineId 流水线名
     * @return 快照字节；没有返回 {@code null}
     */
    byte[] load(String pipelineId);

    /**
     * 存快照（覆盖旧快照）。
     *
     * @param pipelineId 流水线名
     * @param state      快照字节
     */
    void save(String pipelineId, byte[] state);

    /**
     * 清快照（mode=init 重跑用）。
     *
     * @param pipelineId 流水线名
     */
    void clear(String pipelineId);
}
