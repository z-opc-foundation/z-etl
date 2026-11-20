package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.util.CronExpression;

/**
 * 流水线调度登记（cron）。
 *
 * <p>口径：**z-etl 只管「登记 + 可观测」，不起常驻定时线程** ——
 * 真正的定时器由宿主驱动（z-ops 定时任务 / k8s CronJob 调 {@code POST /api/etl/pipelines/{name}/run}），
 * 这样 ETL 进程不背调度职责，也不用管自己的存活。
 *
 * @author zifang
 */
public class PipelineSchedule {

    /** 流水线名（主键）。 */
    private String pipelineName;

    /** cron 表达式（5 段：分 时 日 月 周）。 */
    private String cronExpression;

    /** 是否启用。 */
    private boolean enabled = true;

    /** 上次被触发的时间（毫秒；宿主调触发口时更新）。 */
    private Long lastTriggeredAt;

    /** 登记时间（毫秒）。 */
    private Long createdAt;

    /** 更新时间（毫秒）。 */
    private Long updatedAt;

    public PipelineSchedule() {
    }

    public PipelineSchedule(String pipelineName, String cronExpression) {
        this.pipelineName = pipelineName;
        this.cronExpression = cronExpression;
    }

    /** 下一次触发时间（毫秒）；表达式非法或算不出返回 {@code null}。 */
    public Long nextFireTime() {
        if (cronExpression == null || cronExpression.trim().isEmpty()) {
            return null;
        }
        try {
            long next = CronExpression.parse(cronExpression).nextFireTimeAfter(System.currentTimeMillis());
            return next < 0L ? null : Long.valueOf(next);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** 下一次触发时间（getter 形态 ⇒ REST 响应里能直接看到）。 */
    public Long getNextFireTime() {
        return nextFireTime();
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Long getLastTriggeredAt() {
        return lastTriggeredAt;
    }

    public void setLastTriggeredAt(Long lastTriggeredAt) {
        this.lastTriggeredAt = lastTriggeredAt;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
