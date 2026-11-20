package com.zifang.z.etl.core.engine;

import java.util.List;

/**
 * 调度登记仓储（cron 表达式 + 上次触发时间）。
 *
 * @author zifang
 */
public interface PipelineScheduleRepository {

    /**
     * 登记 / 更新调度。
     *
     * @param schedule 调度登记
     */
    void save(PipelineSchedule schedule);

    /**
     * 按流水线名取调度登记。
     *
     * @param pipelineName 流水线名
     * @return 登记；没有返回 {@code null}
     */
    PipelineSchedule find(String pipelineName);

    /**
     * 全部调度登记。
     *
     * @return 登记列表
     */
    List<PipelineSchedule> list();

    /**
     * 记录一次触发（更新「上次触发时间」）。
     *
     * @param pipelineName 流水线名
     */
    void touch(String pipelineName);

    /**
     * 取消调度。
     *
     * @param pipelineName 流水线名
     * @return 是否真的删掉了
     */
    boolean remove(String pipelineName);
}
