package com.zifang.z.etl.core.checkpoint;

/**
 * 水位存储。
 *
 * @author zifang
 */
public interface CheckpointStore {

    /**
     * 取作业的水位；没有记录返回 {@code null}。
     *
     * @param jobName 作业名
     * @return 水位记录或 null
     */
    Checkpoint load(String jobName);

    /**
     * 存作业的水位（覆盖写）。
     *
     * @param checkpoint 水位记录
     */
    void save(Checkpoint checkpoint);

    /**
     * 清掉作业的水位（把作业退回全量首跑）。
     *
     * @param jobName 作业名
     */
    void clear(String jobName);
}
