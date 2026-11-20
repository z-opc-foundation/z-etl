package com.zifang.z.etl.core.runtime;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.WatermarkCursor;
import com.zifang.z.etl.core.operator.OperatorFactory;
import com.zifang.z.etl.core.optimize.JdbcTableSchemaResolver;
import com.zifang.z.etl.core.optimize.PlanOptimizer;
import com.zifang.z.etl.core.pipeline.PipelineEdge;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.pipeline.PipelineValidator;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import com.zifang.z.etl.core.transform.ScriptSandbox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 流水线执行器（Flink 式推送执行的编排层）。
 *
 * <p>执行口径：
 * <ol>
 *   <li>建图：节点 → 算子，边 → 派发路由（带输入标签）；</li>
 *   <li>拉源：源算子一条一条吐记录，路由到下游（单线程定序 ⇒ 结果可复现）；</li>
 *   <li>推水位：全局水位 = 各活跃源水位的最小值（Flink 的多入流口径）；
 *       水位推进沿拓扑序传给每个算子，再触发到点的事件时间定时器；</li>
 *   <li>流结束：水位推到最大 ⇒ 所有窗口结算；然后关算子（汇刷批）、存快照、落水位。</li>
 * </ol>
 *
 * <p>模式（与旧引擎同口径）：{@code auto}（有快照/水位则续跑）/ {@code init}（清掉重跑）/
 * {@code incremental}（必须续跑，否则失败不静默退化）。
 *
 * @author zifang
 */
public class PipelineExecutor {

    private final DataSourceProvider dataSourceProvider;
    private final StateBackend stateBackend;
    private final ScriptSandbox sandbox;
    private final CheckpointStore checkpointStore;
    private final PipelineStateStore stateStore;
    private final MessageBus messageBus;

    /** 运行期建好的图（算子 id → 算子）。 */
    private final Map<String, com.zifang.z.etl.core.runtime.Operator> operators =
            new LinkedHashMap<String, com.zifang.z.etl.core.runtime.Operator>();

    /** 算子 id → 上下文。 */
    private final Map<String, OperatorContext> contexts = new LinkedHashMap<String, OperatorContext>();

    /** 算子 id → 输出口（扇出）。 */
    private final Map<String, Output> nodeOutputs = new LinkedHashMap<String, Output>();

    /** 算子 id → 定时器队列。 */
    private final Map<String, OperatorTimers> timers = new LinkedHashMap<String, OperatorTimers>();

    public PipelineExecutor(DataSourceProvider dataSourceProvider) {
        this(dataSourceProvider, new HeapStateBackend(), new ScriptSandbox(), null, null, null);
    }

    public PipelineExecutor(DataSourceProvider dataSourceProvider, StateBackend stateBackend) {
        this(dataSourceProvider, stateBackend, new ScriptSandbox(), null, null, null);
    }

    public PipelineExecutor(DataSourceProvider dataSourceProvider, StateBackend stateBackend,
                            ScriptSandbox sandbox) {
        this(dataSourceProvider, stateBackend, sandbox, null, null, null);
    }

    /**
     * @param dataSourceProvider 数据源解析 SPI
     * @param stateBackend       状态后端（键控状态放哪）
     * @param sandbox            脚本沙箱（filter / map / script 用）
     * @param checkpointStore    水位游标存储（断点续传用；可空）
     * @param stateStore         状态快照存储（键控状态落库用；可空）
     */
    public PipelineExecutor(DataSourceProvider dataSourceProvider, StateBackend stateBackend,
                            ScriptSandbox sandbox, CheckpointStore checkpointStore,
                            PipelineStateStore stateStore) {
        this(dataSourceProvider, stateBackend, sandbox, checkpointStore, stateStore, null);
    }

    /**
     * @param dataSourceProvider 数据源解析 SPI
     * @param stateBackend       状态后端（键控状态放哪）
     * @param sandbox            脚本沙箱（filter / map / script 用）
     * @param checkpointStore    水位游标存储（断点续传用；可空）
     * @param stateStore         状态快照存储（键控状态落库用；可空）
     * @param messageBus         消息总线（消息类源 / 汇用；可空）
     */
    public PipelineExecutor(DataSourceProvider dataSourceProvider, StateBackend stateBackend,
                            ScriptSandbox sandbox, CheckpointStore checkpointStore,
                            PipelineStateStore stateStore, MessageBus messageBus) {
        this.dataSourceProvider = dataSourceProvider;
        this.stateBackend = stateBackend;
        this.sandbox = sandbox;
        this.checkpointStore = checkpointStore;
        this.stateStore = stateStore;
        this.messageBus = messageBus;
    }

    /**
     * 跑一条流水线。
     *
     * @param spec 流水线声明
     * @return 运行结果
     * @throws Exception 执行失败（任何算子失败都会中断整条流水线，不静默吞）
     */
    public PipelineRunResult execute(PipelineSpec spec) throws Exception {
        return execute(spec, null);
    }

    /**
     * 跑一条流水线（可捕获运行期算子实例）。
     *
     * @param spec            流水线声明
     * @param operatorCapture 非空时，跑完后把「节点 id → 算子」拷进去（collect 汇断言用；算子已 close）
     * @return 运行结果
     * @throws Exception 执行失败
     */
    public PipelineRunResult execute(PipelineSpec spec,
                                     Map<String, com.zifang.z.etl.core.runtime.Operator> operatorCapture) throws Exception {
        long start = System.currentTimeMillis();
        // —— 计划优化（过滤下推 / 投影下推 / 聚合下推 / 合并映射 / 摘死节点）——
        // 优化会改写声明 ⇒ 先深拷贝一份，别把调用方的对象改了
        // 表结构解析交给优化器：依赖列集合的规则（聚合下推）要靠它判定「下游引用的列还在不在」
        PlanOptimizer.Optimized optimized = new PlanOptimizer().optimize(
                PipelineParser.copyOf(spec), new JdbcTableSchemaResolver(dataSourceProvider));
        PipelineSpec plan = optimized.spec();
        PipelineValidator.validate(plan);

        PipelineRunResult result = new PipelineRunResult();
        result.setPipelineName(plan.getName());
        for (String note : optimized.notes()) {
            result.addNote(note);
        }
        String mode = plan.getMode() == null ? "auto" : plan.getMode().trim().toLowerCase();
        result.setMode(mode);
        result.setStartedAt(start);
        lastSeenWatermark = Long.MIN_VALUE;

        // —— 状态快照恢复（mode=init 例外：清掉从头跑）——
        if (stateStore != null) {
            if ("init".equals(mode)) {
                stateStore.clear(plan.getName());
                result.addNote("mode=init：清掉状态快照，从头跑");
            } else {
                byte[] snapshot = stateStore.load(plan.getName());
                if (snapshot != null && snapshot.length > 0) {
                    stateBackend.restore(snapshot);
                    result.setRestored(true);
                    result.addNote("已从状态快照恢复（" + snapshot.length + " 字节）");
                }
            }
        }

        // —— 水位游标（断点续传的「读到哪了」）——
        Checkpoint checkpoint = checkpointStore == null ? null : checkpointStore.load(plan.getName());
        if ("incremental".equals(mode)
                && (checkpoint == null || checkpoint.getWatermarkValue() == null
                || checkpoint.getWatermarkValue().trim().isEmpty())) {
            throw new IllegalStateException("流水线 " + plan.getName()
                    + " 声明为 incremental 但没有水位记录（先跑一次全量，或改 mode=auto）");
        }
        if ("init".equals(mode) && checkpointStore != null) {
            checkpointStore.clear(plan.getName());
            checkpoint = null;
        }
        result.setWatermarkBefore(checkpoint == null ? null : checkpoint.getWatermarkValue());

        // —— 建图 ——
        Map<String, PipelineNode> index = new LinkedHashMap<String, PipelineNode>();
        for (PipelineNode node : plan.getNodes()) {
            index.put(node.getId(), node);
        }
        List<String> topo = PipelineValidator.topologicalOrder(plan, index);

        List<PullSource> sources = new ArrayList<PullSource>();
        List<String> sourceIds = new ArrayList<String>();
        List<com.zifang.z.etl.core.runtime.Operator> closables = new ArrayList<com.zifang.z.etl.core.runtime.Operator>();
        try {
            for (String id : topo) {
                PipelineNode node = index.get(id);
                boolean stateful = node.getOperatorType().isStateful();
                com.zifang.z.etl.core.runtime.Operator operator =
                        OperatorFactory.create(node, dataSourceProvider, sandbox, messageBus);
                if (operator instanceof PullSource) {
                    PullSource source = (PullSource) operator;
                    if (checkpoint != null && checkpoint.getWatermarkValue() != null) {
                        source.setResume(checkpoint.getWatermarkValue(), checkpoint.getTiebreakValue());
                    }
                    sources.add(source);
                    sourceIds.add(id);
                }
                OperatorTimers operatorTimers = stateful ? new OperatorTimers() : null;
                OperatorContext context = new OperatorContext(id,
                        stateful ? stateBackend.createStore(id) : null, operatorTimers);
                operator.open(context);
                operators.put(id, operator);
                contexts.put(id, context);
                timers.put(id, operatorTimers);
                closables.add(operator);
            }

            // 旁路输出的目标节点必须存在（开跑前挑出来，不等到运行时静默丢）
            for (PipelineNode node : plan.getNodes()) {
                String rejectedTo = node.string("rejectedTo");
                if (rejectedTo != null && !index.containsKey(rejectedTo)) {
                    throw new IllegalArgumentException("节点 " + node.getId() + " 的 rejectedTo 指向不存在的节点: "
                            + rejectedTo);
                }
            }

            // —— 边 → 派发路由 ——
            for (String id : topo) {
                List<EdgeRoute> routes = new ArrayList<EdgeRoute>();
                for (PipelineEdge edge : plan.edgesFrom(id)) {
                    routes.add(new EdgeRoute(edge.getFrom(), edge.getTo()));
                }
                nodeOutputs.put(id, new FanoutOutput(id, routes));
            }

            // —— 主循环：拉源 → 派发 → 推水位 ——
            long recordsRead = 0L;
            boolean[] done = new boolean[sources.size()];
            long lastWatermark = Long.MIN_VALUE;
            while (true) {
                boolean allDone = true;
                for (int i = 0; i < sources.size(); i++) {
                    if (done[i]) {
                        continue;
                    }
                    allDone = false;
                    Record record = sources.get(i).next();
                    if (record == null) {
                        done[i] = true;
                        continue;
                    }
                    recordsRead++;
                    nodeOutputs.get(sourceIds.get(i)).collect(record);
                    long candidate = minWatermarkOfActive(sources, done);
                    if (candidate > lastWatermark) {
                        lastWatermark = candidate;
                        propagateWatermark(candidate, topo);
                    }
                }
                if (allDone) {
                    break;
                }
            }
            result.setRecordsRead(recordsRead);

            // —— 流结束：水位推到最大 ⇒ 全部窗口结算 ——
            propagateWatermark(Long.MAX_VALUE, topo);

            // —— 收尾：关算子（汇刷批）——
            for (int i = closables.size() - 1; i >= 0; i--) {
                closables.get(i).close();
            }

            // —— 状态快照落库 ——
            if (stateStore != null) {
                stateStore.save(plan.getName(), stateBackend.snapshot());
            }

            // —— 水位游标落库 ——
            for (int i = 0; i < sources.size(); i++) {
                PullSource source = sources.get(i);
                String checkpointColumn = source.checkpointColumn();
                Object checkpointValue = source.checkpointValue();
                if (checkpointStore == null || checkpointColumn == null || checkpointValue == null) {
                    continue;
                }
                String type = source.checkpointType();
                String value = WatermarkCursor.format(checkpointValue, type);
                String tiebreakValue = source.lastTiebreakValue() == null
                        ? null : String.valueOf(source.lastTiebreakValue());
                checkpointStore.save(new Checkpoint(plan.getName(), checkpointColumn, type, value,
                        source.tiebreakColumn(), tiebreakValue, mode, System.currentTimeMillis()));
                result.setWatermarkAfter(value);
            }

            // —— 结果汇总 ——
            long written = 0L;
            for (com.zifang.z.etl.core.runtime.Operator operator : operators.values()) {
                if (operator instanceof CountingSink) {
                    written += ((CountingSink) operator).written();
                }
            }
            result.setRecordsWritten(written);
            for (String id : topo) {
                OperatorContext context = contexts.get(id);
                if (context != null) {
                    result.putNodeMetrics(id, context.counters());
                }
            }
            if (checkpoint == null) {
                result.addNote("无水位记录，本轮全量");
            } else {
                result.addNote("从水位 " + checkpoint.getWatermarkValue() + " 接力增量");
            }
            if (operatorCapture != null) {
                operatorCapture.putAll(operators);
            }
            return result;
        } finally {
            for (int i = closables.size() - 1; i >= 0; i--) {
                try {
                    closables.get(i).close();
                } catch (Exception ignore) {
                    // 收尾失败不影响主异常
                }
            }
            operators.clear();
            contexts.clear();
            nodeOutputs.clear();
            timers.clear();
        }
    }

    /** 活跃源的水位最小值（Flink 多入流口径：已结束的源不再拖住水位）。 */
    private long minWatermarkOfActive(List<PullSource> sources, boolean[] done) {
        long min = Long.MAX_VALUE;
        boolean any = false;
        for (int i = 0; i < sources.size(); i++) {
            if (done[i]) {
                continue;
            }
            long watermark = sources.get(i).currentWatermark();
            if (watermark == Long.MIN_VALUE) {
                return lastSeenWatermark;
            }
            if (watermark < min) {
                min = watermark;
            }
            any = true;
        }
        lastSeenWatermark = any ? min : Long.MAX_VALUE;
        return lastSeenWatermark;
    }

    /** 上一次的全局水位（源还没产生事件时间时保持水位不回退）。 */
    private long lastSeenWatermark = Long.MIN_VALUE;

    /**
     * 水位推进：沿拓扑序传给每个算子，再触发到点的事件时间定时器。
     *
     * @param watermark 新水位
     * @param topo      拓扑序
     */
    private void propagateWatermark(long watermark, List<String> topo) throws Exception {
        for (String id : topo) {
            com.zifang.z.etl.core.runtime.Operator operator = operators.get(id);
            operator.processWatermark(watermark);
            OperatorContext context = contexts.get(id);
            if (context != null) {
                context.setCurrentWatermark(watermark);
            }
            OperatorTimers operatorTimers = timers.get(id);
            if (operatorTimers == null) {
                continue;
            }
            for (OperatorTimers.Timer timer : operatorTimers.fireEventTimeDue(watermark)) {
                operator.onTimer(timer.getTimestamp(), timer.getName());
            }
        }
    }

    /** 一条边的派发路由：把记录送进下游算子（带上游节点 id 作输入标签）。 */
    private final class EdgeRoute {

        private final String from;
        private final String to;

        EdgeRoute(String from, String to) {
            this.from = from;
            this.to = to;
        }

        void deliver(Record record) {
            com.zifang.z.etl.core.runtime.Operator downstream = operators.get(to);
            OperatorContext context = contexts.get(to);
            if (context != null) {
                context.count("records-in");
            }
            try {
                downstream.processElement(record, nodeOutputs.get(to), from);
            } catch (Exception e) {
                throw new IllegalStateException("节点 " + from + " → " + to + " 派发失败: " + e.getMessage(), e);
            }
        }
    }

    /** 一个节点的输出口：主输出扇出到全部出边；旁路输出按标签 id 直投对应节点。 */
    private final class FanoutOutput implements Output {

        private final String nodeId;
        private final List<EdgeRoute> routes;

        FanoutOutput(String nodeId, List<EdgeRoute> routes) {
            this.nodeId = nodeId;
            this.routes = routes;
        }

        @Override
        public void collect(Record record) {
            for (EdgeRoute route : routes) {
                route.deliver(record);
            }
        }

        @Override
        public void collect(OutputTag tag, Record record) {
            com.zifang.z.etl.core.runtime.Operator target = operators.get(tag.id());
            if (target == null) {
                OperatorContext context = contexts.get(nodeId);
                if (context != null) {
                    context.count("side-output-dropped");
                }
                return;
            }
            OperatorContext context = contexts.get(nodeId);
            if (context != null) {
                context.count("side-output-sent");
            }
            OperatorContext targetContext = contexts.get(tag.id());
            if (targetContext != null) {
                targetContext.count("records-in");
            }
            try {
                target.processElement(record, nodeOutputs.get(tag.id()), nodeId);
            } catch (Exception e) {
                throw new IllegalStateException("旁路输出 " + nodeId + " → " + tag.id()
                        + " 派发失败: " + e.getMessage(), e);
            }
        }
    }
}
