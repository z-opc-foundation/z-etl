package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.optimize.PlanOptimizer;
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

    public PipelineEngine(DataSourceProvider dataSourceProvider, PipelineRepository repository,
                          CheckpointStore checkpointStore, PipelineStateStore stateStore) {
        this(dataSourceProvider, repository, checkpointStore, stateStore, null);
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
        this.dataSourceProvider = dataSourceProvider;
        this.repository = repository;
        this.checkpointStore = checkpointStore;
        this.stateStore = stateStore;
        this.messageBus = messageBus;
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
        PipelineExecutor executor = new PipelineExecutor(dataSourceProvider, new HeapStateBackend(),
                new ScriptSandbox(), checkpointStore, stateStore, messageBus);
        PipelineRunResult result = executor.execute(spec);
        repository.saveRun(result);
        return result;
    }

    /** 水位记录（断点续传的「读到哪了」）；没有返回 {@code null}。 */
    public Checkpoint watermark(String name) {
        return checkpointStore == null ? null : checkpointStore.load(name);
    }

    /** 运行履历（新→旧）。 */
    public List<PipelineRunResult> history(String name, int limit) {
        return repository.runHistory(name, limit);
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
