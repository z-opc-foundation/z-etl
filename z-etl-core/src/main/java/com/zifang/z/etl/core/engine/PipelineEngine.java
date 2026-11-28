package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.Savepoint;
import com.zifang.z.etl.core.checkpoint.Savepoints;
import com.zifang.z.etl.core.optimize.PlanOptimizer;
import com.zifang.z.etl.core.job.RetrySpec;
import com.zifang.z.etl.core.pipeline.PipelineParams;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.pipeline.PipelineValidator;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.runtime.PipelineStateStore;
import com.zifang.z.etl.core.spi.AiExecutor;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import com.zifang.z.etl.core.spi.PipelineRefResolver;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.util.Backoff;
import com.zifang.z.etl.core.util.CronExpression;

import java.util.List;
import java.util.Map;

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
    private final AiExecutor aiExecutor;

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
        this(dataSourceProvider, repository, checkpointStore, stateStore, messageBus, scheduleRepository, null);
    }

    /**
     * @param dataSourceProvider 数据源解析
     * @param repository          流水线定义仓储
     * @param checkpointStore     水位游标存储
     * @param stateStore          状态快照存储
     * @param messageBus          消息总线（消息类源 / 汇用；可空）
     * @param scheduleRepository  调度登记仓储（可空 ⇒ 不提供调度登记）
     * @param aiExecutor          AI 执行器（ai.map 用；可空 ⇒ ai.map 节点打开时报错说明）
     */
    public PipelineEngine(DataSourceProvider dataSourceProvider, PipelineRepository repository,
                          CheckpointStore checkpointStore, PipelineStateStore stateStore,
                          MessageBus messageBus, PipelineScheduleRepository scheduleRepository,
                          AiExecutor aiExecutor) {
        this.dataSourceProvider = dataSourceProvider;
        this.repository = repository;
        this.checkpointStore = checkpointStore;
        this.stateStore = stateStore;
        this.messageBus = messageBus;
        this.scheduleRepository = scheduleRepository;
        this.aiExecutor = aiExecutor;
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
     * 删除流水线（定义 + 运行履历）。
     *
     * @param name 流水线名
     * @return 删掉了返回 {@code true}；本来就没有返回 {@code false}
     */
    public boolean delete(String name) {
        return repository.delete(name);
    }

    /**
     * 触发流水线（取落库定义执行，履历落库）。
     *
     * @param name 流水线名
     * @return 运行结果
     * @throws Exception 定义不存在或执行失败
     */
    public PipelineRunResult run(String name) throws Exception {
        return run(name, null);
    }

    /**
     * 触发流水线（带运行参数）。
     *
     * <p>参数只作用于**本次运行**：落库的定义里占位符原样保留 ⇒ 同一份声明天天跑不同批次。
     *
     * @param name   流水线名
     * @param params 运行参数（可为 {@code null} ⇒ 只有内置参数 bizdate/yesterday/now 可用）
     * @return 运行结果
     * @throws Exception 定义不存在或执行失败
     */
    public PipelineRunResult run(String name, Map<String, String> params) throws Exception {
        PipelineSpec stored = repository.find(name);
        if (stored == null) {
            throw new IllegalArgumentException("流水线不存在: " + name
                    + "（已登记: " + String.join(", ", repository.listNames()) + "）");
        }
        // 在副本上替换占位符：落库的定义是「带占位符的模板」，不该被一次运行改坏
        PipelineSpec spec = PipelineParams.resolve(PipelineParser.copyOf(stored), params);
        RetrySpec retry = spec.getRetry() == null ? new RetrySpec() : spec.getRetry();
        int maxAttempts = retry.effectiveMaxAttempts();
        if (maxAttempts == 1) {
            // 不声明重试 ⇒ 异常原样抛出（行为与从前完全一致）；失败也落履历（运行总览口径）
            try {
                PipelineRunResult result = runOnce(spec);
                repository.saveRun(result);
                touchSchedule(name);
                return result;
            } catch (Exception e) {
                repository.saveRun(failedRun(name, e));
                throw e;
            }
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
        repository.saveRun(failedRun(name, last));
        throw new IllegalStateException("流水线 " + name + " 连续 " + maxAttempts
                + " 次失败（最后一次: " + (last == null ? "未知" : last.getMessage()) + "）", last);
    }

    /** 失败运行履历：状态 FAILED，异常摘要进 notes（履历分隔符「；」替换掉，避免拆散）。 */
    private PipelineRunResult failedRun(String name, Exception cause) {
        PipelineRunResult result = new PipelineRunResult();
        result.setPipelineName(name);
        result.setStatus(PipelineRunResult.STATUS_FAILED);
        long now = System.currentTimeMillis();
        result.setStartedAt(now);
        result.setFinishedAt(now);
        String message = cause == null ? "未知错误" : (cause.getClass().getSimpleName() + ": " + cause.getMessage());
        result.addNote(message.replace("；", "; "));
        return result;
    }

    /**
     * 试跑：最多处理 {@code sampleRows} 条，汇只计数不落地，不读不写水位与状态快照。
     *
     * <p>试跑不算履历（不算正式运行），也不碰任何持久状态 —— 纯观测。
     *
     * @param name       流水线名
     * @param params     运行参数（占位符取值；可为 {@code null}）
     * @param sampleRows 最多处理的行数
     * @return 运行结果（{@code recordsWritten} 是「试跑下来会写多少行」）
     */
    public PipelineRunResult dryRun(String name, Map<String, String> params, long sampleRows) throws Exception {
        PipelineSpec stored = repository.find(name);
        if (stored == null) {
            throw new IllegalArgumentException("流水线不存在: " + name
                    + "（已登记: " + String.join(", ", repository.listNames()) + "）");
        }
        PipelineSpec spec = PipelineParams.resolve(PipelineParser.copyOf(stored), params);
        PipelineExecutor executor = new PipelineExecutor(dataSourceProvider, new HeapStateBackend(),
                new ScriptSandbox(), null, null, messageBus, pipelineResolver(), aiExecutor);
        return executor.dryRun(spec, sampleRows);
    }

    /** 跑一次（不落履历）；重试与落履历由 {@link #run(String)} 负责。 */
    private PipelineRunResult runOnce(PipelineSpec spec) throws Exception {
        PipelineExecutor executor = new PipelineExecutor(dataSourceProvider, new HeapStateBackend(),
                new ScriptSandbox(), checkpointStore, stateStore, messageBus, pipelineResolver(), aiExecutor);
        return executor.execute(spec);
    }

    /** 水位记录（断点续传的「读到哪了」）；没有返回 {@code null}。 */
    public Checkpoint watermark(String name) {
        return checkpointStore == null ? null : checkpointStore.load(name);
    }

    /** 管道解析：pipeline.ref 源按名取落库定义。 */
    private PipelineRefResolver pipelineResolver() {
        return new PipelineRefResolver() {
            @Override
            public PipelineSpec resolve(String name) {
                return repository.find(name);
            }
        };
    }

    /**
     * 导出保存点（纯读，不动现场）。
     *
     * <p>保存点 = 键控状态快照 + 水位游标打包成一个自描述 JSON 报文，
     * 可以搬去别的环境灌回（可换名）。空管道（状态与水位都没有）导不出。
     *
     * @param name 流水线名
     * @return 保存点
     */
    public Savepoint exportSavepoint(String name) {
        requirePipeline(name);
        return Savepoints.export(name, stateStore, checkpointStore);
    }

    /**
     * 灌回保存点（覆盖写；目标名取报文里的导出名）。
     *
     * @param savepointJson 保存点 JSON 文本
     */
    public void importSavepoint(String savepointJson) throws Exception {
        Savepoint savepoint = Savepoints.fromJson(savepointJson);
        Savepoints.apply(savepoint, savepoint.getPipeline(), stateStore, checkpointStore);
    }

    /**
     * 灌回保存点（覆盖写，换名迁移：灌到 {@code targetName} 名下）。
     *
     * @param savepointJson 保存点 JSON 文本
     * @param targetName    目标流水线名
     */
    public void importSavepoint(String savepointJson, String targetName) throws Exception {
        Savepoint savepoint = Savepoints.fromJson(savepointJson);
        Savepoints.apply(savepoint, targetName, stateStore, checkpointStore);
    }

    private void requirePipeline(String name) {
        if (repository.find(name) == null) {
            throw new IllegalArgumentException("流水线不存在: " + name
                    + "（已登记: " + String.join(", ", repository.listNames()) + "）");
        }
    }

    /** 运行履历（新→旧）。 */
    public List<PipelineRunResult> history(String name, int limit) {
        return repository.runHistory(name, limit);
    }

    /** 跨流水线最近运行（新→旧，运行总览用）；{@code limit ≤ 0} 不限。 */
    public List<PipelineRunResult> recentRuns(int limit) {
        return repository.recentRuns(limit);
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
