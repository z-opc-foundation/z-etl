package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.optimize.PlanOptimizer;
import com.zifang.z.etl.core.job.RetrySpec;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.pipeline.PipelineValidator;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.runtime.PipelineStateStore;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.util.Backoff;
import com.zifang.z.etl.core.util.CronExpression;

import java.util.List;

/**
 * 流水线管控引擎（与 {@link EtlEngine} 同形状的门面）。
 *
 * <p>登记（声明原文 → 解析校验 → 落库）/ 触发（取定义 → 执行器跑 → 履历落库）/
 * 水位查询 / 履历查询 / 计划解释（{@code explain}）。每次运行用一个新
 * {@link PipelineExecutor}：执行器本身无状态，跨次恢复靠状态快照 + 水位游标。
 *
 * @author zifang
 */
public class PipelineEngine {

    private final DataSourceProvider dataSourceProvider;
    private final PipelineRepository repository;
    private final CheckpointStore checkpointStore;
    private final PipelineStateStore stateStore;
    private final MessageBus messageBus;
    private final PipelineScheduleRepository scheduleRepository;

    public PipelineEngine(DataSourceProvider dataSourceProvider, PipelineRepository repository,
                          CheckpointStore checkpointStore, PipelineStateStore stateStore) {
        this(dataSourceProvider, repository, checkpointStore, stateStore, null, null);
    }

    /**
     * @param dataSourceProvider 数据源解析
     * @param repository          流水线定义仓储
     * @param checkpointStore     水位游标存储
     * @param stateStore          状态快照存储
     * @param messageBus          消息总线（消息类源 / 汇用；可空）
     */
    public PipelineEngine(DataSourceProvider dataSourceProvider, PipelineRepository repository,
                          CheckpointStore checkpointStore, PipelineStateStore stateStore,
                          MessageBus messageBus) {
        this(dataSourceProvider, repository, checkpointStore, stateStore, messageBus, null);
    }

    /**
     * @param dataSourceProvider 数据源解析
     * @param repository          流水线定义仓储
     * @param checkpointStore     水位游标存储
     * @param stateStore          状态快照存储
     * @param messageBus          消息总线（消息类源 / 汇用；可空）
     * @param scheduleRepository  调度登记仓储（可空 ⇒ 不提供调度登记）
     */
    public PipelineEngine(DataSourceProvider dataSourceProvider, PipelineRepository repository,
                          CheckpointStore checkpointStore, PipelineStateStore stateStore,
                          MessageBus messageBus, PipelineScheduleRepository scheduleRepository) {
        this.dataSourceProvider = dataSourceProvider;
        this.repository = repository;
        this.checkpointStore = checkpointStore;
        this.stateStore = stateStore;
        this.messageBus = messageBus;
        this.scheduleRepository = scheduleRepository;
    }

    /**
     * 登记流水线（声明原文可以是 JSON 或 yml）。
     *
     * @param specText 声明原文
     * @return 流水线名
     */
    public String register(String specText) {
        PipelineSpec spec = PipelineParser.parse(specText);
        PipelineValidator.validate(spec);
        repository.save(spec);
        return spec.getName();
    }

    /** 全部流水线名。 */
    public List<String> listNames() {
        return repository.listNames();
    }

    /** 按名取定义；不存在返回 {@code null}。 */
    public PipelineSpec get(String name) {
        return repository.find(name);
    }

    /**
     * 触发流水线（取落库定义执行，履历落库）。
     *
     * @param name 流水线名
     * @return 运行结果
     * @throws Exception 定义不存在或执行失败
     */
    public PipelineRunResult run(String name) throws Exception {
        PipelineSpec spec = repository.find(name);
        if (spec == null) {
            throw new IllegalArgumentException("流水线不存在: " + name
                    + "（已登记: " + String.join(", ", repository.listNames()) + "）");
        }
        RetrySpec retry = spec.getRetry() == null ? new RetrySpec() : spec.getRetry();
        int maxAttempts = retry.effectiveMaxAttempts();
        if (maxAttempts == 1) {
            // 不声明重试 ⇒ 异常原样抛出（行为与从前完全一致）
            PipelineRunResult result = runOnce(spec);
            repository.saveRun(result);
            touchSchedule(name);
            return result;
        }
        Exception last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                PipelineRunResult result = runOnce(spec);
                if (attempt > 1) {
                    result.addNote("第 " + attempt + " 次尝试成功（前 " + (attempt - 1) + " 次失败后重试）");
                }
                repository.saveRun(result);
                touchSchedule(name);
                return result;
            } catch (Exception e) {
                last = e;
                if (attempt < maxAttempts) {
                    Backoff.sleep(retry.backoffFor(attempt));
                }
            }
        }
        throw new IllegalStateException("流水线 " + name + " 连续 " + maxAttempts
                + " 次失败（最后一次: " + (last == null ? "未知" : last.getMessage()) + "）", last);
    }

    /** 跑一次（不落履历）；重试与落履历由 {@link #run(String)} 负责。 */
    private PipelineRunResult runOnce(PipelineSpec spec) throws Exception {
        PipelineExecutor executor = new PipelineExecutor(dataSourceProvider, new HeapStateBackend(),
                new ScriptSandbox(), checkpointStore, stateStore, messageBus);
        return executor.execute(spec);
    }

    /** 水位记录（断点续传的「读到哪了」）；没有返回 {@code null}。 */
    public Checkpoint watermark(String name) {
        return checkpointStore == null ? null : checkpointStore.load(name);
    }

    /** 运行履历（新→旧）。 */
    public List<PipelineRunResult> history(String name, int limit) {
        return repository.runHistory(name, limit);
    }

    /**
     * 登记 / 更新调度（cron）。
     *
     * <p>z-etl 只**登记**：真正的定时器由宿主驱动（z-ops 定时任务 / k8s CronJob 调
     * {@code POST /api/etl/pipelines/{name}/run}）⇒ ETL 进程不背调度职责。
     *
     * @param pipelineName 流水线名
     * @param cron         cron 表达式（5 段：分 时 日 月 周）
     * @param enabled      是否启用
     * @return 登记（含下一次触发时间）
     */
    public PipelineSchedule schedule(String pipelineName, String cron, boolean enabled) {
        if (scheduleRepository == null) {
            throw new IllegalStateException("未配置调度登记仓储");
        }
        if (repository.find(pipelineName) == null) {
            throw new IllegalArgumentException("流水线不存在: " + pipelineName);
        }
        CronExpression expression = CronExpression.parse(cron);
        PipelineSchedule schedule = new PipelineSchedule(pipelineName, expression.expression());
        schedule.setEnabled(enabled);
        scheduleRepository.save(schedule);
        return scheduleRepository.find(pipelineName);
    }

    /**
     * 取消调度。
     *
     * @param pipelineName 流水线名
     * @return 是否真的取消了
     */
    public boolean unschedule(String pipelineName) {
        return scheduleRepository != null && scheduleRepository.remove(pipelineName);
    }

    /** 全部调度登记。 */
    public List<PipelineSchedule> schedules() {
        return scheduleRepository == null ? new java.util.ArrayList<PipelineSchedule>() : scheduleRepository.list();
    }

    /**
     * 某条流水线的调度登记。
     *
     * @param pipelineName 流水线名
     * @return 登记；没有返回 {@code null}
     */
    public PipelineSchedule schedule(String pipelineName) {
        return scheduleRepository == null ? null : scheduleRepository.find(pipelineName);
    }

    /** 触发成功后记一次「上次触发时间」。 */
    private void touchSchedule(String pipelineName) {
        if (scheduleRepository == null) {
            return;
        }
        try {
            scheduleRepository.touch(pipelineName);
        } catch (RuntimeException ignore) {
            // 调度记账失败不影响本次运行结果
        }
    }

    /** 计划解释（拓扑序文本计划 + 优化备注；不落库）。 */
    public String explain(String name) {
        PipelineSpec spec = repository.find(name);
        if (spec == null) {
            throw new IllegalArgumentException("流水线不存在: " + name);
        }
        return new PlanOptimizer().explain(spec);
    }
}
