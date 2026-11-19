package com.zifang.z.etl.core;

import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import com.zifang.z.wf.core.definition.WfDefinition;
import com.zifang.z.wf.core.definition.WfFlow;
import com.zifang.z.wf.core.definition.WfNode;
import com.zifang.z.wf.core.definition.WfNodeType;
import com.zifang.z.wf.core.engine.WfContext;
import com.zifang.z.wf.core.engine.WfEngine;
import com.zifang.z.wf.core.engine.delegate.WfJavaDelegate;
import com.zifang.z.wf.core.model.WfExecution;
import com.zifang.z.wf.core.model.WfProcessInstance;
import com.zifang.z.wf.core.model.WfProcessStatus;
import com.zifang.z.wf.core.persistence.InMemoryWorkflowPersistence;
import com.zifang.z.wf.core.service.WfRepositoryService;
import com.zifang.z.wf.core.service.WfRuntimeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * z-wf 编排 z-etl 流水线 —— 跨仓集成验收。
 *
 * <p>回答的问题：「z-etl 的 DAG 能不能用上 z-wf？」—— 能，且分工干净：
 * <ul>
 *   <li><b>z-etl 的 DAG</b>管**单条管道内部**的数据流（source → transform → sink，记录级推送）；</li>
 *   <li><b>z-wf</b>管**管道与管道之间**的编排（serviceTask 里跑一条管道，
 *       排他网关按搬运行数分支：搬到了东西才跑下游汇总，颗粒无收就直接收工）。</li>
 * </ul>
 *
 * <p>接线只有一个点：{@link PipelineRunnerDelegate}（{@code WfJavaDelegate}）里调
 * {@link PipelineExecutor#execute(PipelineSpec)}。core 主代码不耦合 z-wf（依赖 test scope），
 * 生产编排时把该依赖升为 compile、delegate 交给 Spring 装配即可。
 *
 * <p>⚠️ 硬边界：数据全在本地 H2 内存库；流程实例用 z-wf 的内存持久化，不落任何真实库。
 *
 * @author zifang
 */
class WfPipelineOrchestrationTest {

    private EtlTestFixtures fixtures;
    private MapDataSourceProvider provider;
    private InMemoryWorkflowPersistence persistence;
    private WfRepositoryService repositoryService;
    private WfRuntimeService runtimeService;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("wf-orch");
        fixtures.createSchema();
        fixtures.seedSource(10, 1L, new java.sql.Timestamp(1767225600000L));
        // 编排第二条管道的落点：按支付渠道汇总
        execute(fixtures.target(), "CREATE TABLE IF NOT EXISTS etl_order_rollup ("
                + "pay_channel VARCHAR(32) PRIMARY KEY, "
                + "order_cnt BIGINT, "
                + "total_amount DECIMAL(18,2))");
        provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());

        persistence = new InMemoryWorkflowPersistence();
        persistence.initialize();
        WfEngine engine = new WfEngine();
        engine.getDelegateRegistry().register("runPipeline", new PipelineRunnerDelegate(provider));
        repositoryService = new WfRepositoryService(persistence);
        runtimeService = new WfRuntimeService(repositoryService, persistence, engine,
                new com.zifang.z.wf.core.hook.WfHookDispatcher());
    }

    @Test
    @DisplayName("编排正路：serviceTask 跑全量搬运 → 网关见 written>0 → 跑汇总管道 → 流程完成")
    void orchestrationRunsPipelineChainThroughGateway() throws Exception {
        deployOrchestration();

        Map<String, Object> vars = new HashMap<String, Object>();
        String pid = runtimeService.startProcessInstance("etl-orchestration", "ETL-1", "u_etl", vars);

        WfProcessInstance instance = persistence.findProcessInstance(pid);
        assertEquals(WfProcessStatus.COMPLETED, instance.getStatus(), "serviceTask 同步执行 ⇒ 流程应直接跑完");

        // 第一条管道的产出：快照表 10 行
        assertEquals(10L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));
        // 网关走了「搬到了东西」分支：第二条管道跑了，汇总表 1 行（payment_method 恒为 alipay）
        assertEquals(1L, EtlTestFixtures.count(fixtures.target(), "etl_order_rollup"));
        try (Connection connection = fixtures.target().getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT order_cnt, total_amount FROM etl_order_rollup WHERE pay_channel = 'alipay'")) {
            assertEquals(true, rs.next(), "汇总行应存在");
            assertEquals(10L, rs.getLong(1), "cnt 聚合 10 条");
            assertEquals(10L, rs.getLong(1), "cnt 聚合 10 条");
            assertEquals(0, rs.getBigDecimal(2).compareTo(new java.math.BigDecimal("1055.00")),
                    "sum(amount)：id 1..10 ⇒ 100.00+(id%50) 求和 = 1000 + 55");
        }
    }

    @Test
    @DisplayName("编排兜路：空源 ⇒ written=0 ⇒ 网关走 default，汇总管道不跑")
    void orchestrationSkipsDownstreamWhenNothingMoved() throws Exception {
        // 清空源表 ⇒ 第一条管道颗粒无收
        execute(fixtures.source(), "DELETE FROM z_trade_order");
        deployOrchestration();

        Map<String, Object> vars = new HashMap<String, Object>();
        String pid = runtimeService.startProcessInstance("etl-orchestration", "ETL-2", "u_etl", vars);

        WfProcessInstance instance = persistence.findProcessInstance(pid);
        assertEquals(WfProcessStatus.COMPLETED, instance.getStatus());
        assertEquals(0L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_snapshot"));
        assertEquals(0L, EtlTestFixtures.count(fixtures.target(), "etl_order_rollup"),
                "网关 default 分支 ⇒ 汇总管道不应执行");
    }

    // ==================== 流程定义 ====================

    /**
     * <pre>
     * start → runFull（serviceTask：跑快照搬运管道）
     *       → gw（排他网关）
     *           ├─ lastWritten > 0 → runRollup（serviceTask：跑汇总管道）→ end
     *           └─ default → end
     * </pre>
     */
    private void deployOrchestration() {
        WfDefinition definition = new WfDefinition("etl-orchestration", "ETL 编排：搬运 + 条件汇总");

        WfNode runFull = serviceTask("runFull", "全量搬运",
                snapshotPipelineYaml());
        WfNode runRollup = serviceTask("runRollup", "渠道汇总",
                rollupPipelineYaml());

        WfFlow moved = flow("f-gw-rollup", "gw", "runRollup");
        moved.setConditionExpression("lastWritten > 0");
        WfFlow skip = flow("f-gw-end", "gw", "end1");
        skip.setDefaultFlow(true);

        definition.setNodes(java.util.Arrays.asList(
                new WfNode("start1", "开始", WfNodeType.START_EVENT),
                runFull,
                new WfNode("gw", "搬运结果判断", WfNodeType.EXCLUSIVE_GATEWAY),
                runRollup,
                new WfNode("end1", "结束", WfNodeType.END_EVENT)));
        definition.setFlows(java.util.Arrays.asList(
                flow("f-start-runFull", "start1", "runFull"),
                flow("f-runFull-gw", "runFull", "gw"),
                moved, skip,
                flow("f-runRollup-end", "runRollup", "end1")));
        definition.buildIndex();
        repositoryService.deploy(definition);
    }

    /** serviceTask 节点：delegateExpression 指向注册好的 delegate，管道 yml 挂在扩展属性上。 */
    private WfNode serviceTask(String id, String name, String pipelineYaml) {
        WfNode node = new WfNode(id, name, WfNodeType.SERVICE_TASK);
        node.setDelegateExpression("runPipeline");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("pipelineYaml", pipelineYaml);
        node.setProperties(properties);
        return node;
    }

    private WfFlow flow(String id, String source, String target) {
        WfFlow flow = new WfFlow(source, target);
        flow.setId(id);
        return flow;
    }

    // ==================== delegate：z-wf → z-etl 的唯一接点 ====================

    /**
     * 流程 serviceTask → z-etl 流水线的委托：从节点扩展属性拿管道 yml，
     * 跑完把搬运行数写回流程变量（网关条件用）。
     *
     * <p>数据源 SPI 走构造注入而不是流程变量：z-wf 持久化会深拷贝变量，
     * DataSource 这类连接性对象不能进变量（NotSerializableException）。
     */
    public static class PipelineRunnerDelegate implements WfJavaDelegate {

        private final DataSourceProvider dataSourceProvider;

        public PipelineRunnerDelegate(DataSourceProvider dataSourceProvider) {
            this.dataSourceProvider = dataSourceProvider;
        }

        @Override
        public void execute(WfContext context, WfExecution execution) {
            WfNode node = context.getDefinition().node(execution.getActivityId());
            String yaml = String.valueOf(node.property("pipelineYaml"));

            PipelineSpec spec = PipelineParser.parse(yaml);
            // 约定：delegate 抛异常 ⇒ 节点失败 ⇒ 流程内部终止（不静默吞）
            PipelineRunResult result;
            try {
                result = new PipelineExecutor(dataSourceProvider, new HeapStateBackend()).execute(spec);
            } catch (Exception e) {
                throw new IllegalStateException("管道 " + spec.getName() + " 执行失败: " + e.getMessage(), e);
            }

            context.setVariable("lastWritten", result.getRecordsWritten());
            context.setVariable("written_" + node.getId(), result.getRecordsWritten());
        }
    }

    // ==================== 两条管道的 yml ====================

    /** 管道一：快照搬运（与 PipelineEndToEndTest 的 snapshotPipeline 同口径）。 */
    private static String snapshotPipelineYaml() {
        return ""
                + "name: wf-snapshot\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "      columns: [id, order_no, user_id, amount, status, updated_at]\n"
                + "      pageSize: 3\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: id, target: order_id, type: LONG}\n"
                + "        - {source: order_no, target: order_no, type: STRING}\n"
                + "        - {source: user_id, target: user_id, type: LONG}\n"
                + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                + "        - {source: status, target: status, type: INT}\n"
                + "        - {source: updated_at, target: updated_at, type: TIMESTAMP}\n"
                + "  - id: sink\n"
                + "    type: jdbc.sink\n"
                + "    config:\n"
                + "      dataSourceRef: ds-etl-landing\n"
                + "      table: etl_trade_order_snapshot\n"
                + "      writeMode: upsert\n"
                + "      idempotencyKey: [order_id]\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: sink}\n";
    }

    /** 管道二：键控聚合 → 汇总落地。 */
    private static String rollupPipelineYaml() {
        return ""
                + "name: wf-rollup\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: jdbc.source\n"
                + "    config:\n"
                + "      dataSourceRef: ds-oc-main\n"
                + "      table: z_trade_order\n"
                + "  - id: fm\n"
                + "    type: field.mapping\n"
                + "    config:\n"
                + "      mappings:\n"
                + "        - {source: payment_method, target: pay_channel, type: STRING}\n"
                + "        - {source: amount, target: amount, type: BIGDECIMAL}\n"
                + "  - id: agg\n"
                + "    type: aggregate\n"
                + "    config:\n"
                + "      keyBy: [pay_channel]\n"
                + "      aggregates:\n"
                + "        - {fn: COUNT, column: \"*\", alias: order_cnt}\n"
                + "        - {fn: SUM, column: amount, alias: total_amount}\n"
                + "  - id: sink\n"
                + "    type: jdbc.sink\n"
                + "    config:\n"
                + "      dataSourceRef: ds-etl-landing\n"
                + "      table: etl_order_rollup\n"
                + "      columns: [pay_channel, order_cnt, total_amount]\n"
                + "      writeMode: upsert\n"
                + "      idempotencyKey: [pay_channel]\n"
                + "edges:\n"
                + "  - {from: src, to: fm}\n"
                + "  - {from: fm, to: agg}\n"
                + "  - {from: agg, to: sink}\n";
    }

    private static void execute(DataSource dataSource, String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
