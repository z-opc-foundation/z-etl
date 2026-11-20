package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.connector.AbstractPullSource;
import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.pipeline.PipelineParams;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.PipelineRefResolver;
import com.zifang.z.etl.core.transform.ScriptSandbox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 子管道引用源：把**已登记的另一条管道**当作数据源复用（管道当视图）。
 *
 * <p>口径（照抄数据库「视图」的直觉，不是「触发另一条管道跑一遍落库」）：
 * <ul>
 *   <li><b>汇全部换成收集</b>：子管道声明里的 jdbc.sink / file.*.sink 等一律替换成
 *       collect.sink —— 只要产出，不落地、不发 HTTP、不发消息；</li>
 *   <li><b>不碰水位与状态</b>：子管道用全新的内存状态后端、无游标存储 ⇒ 每次全量重算，
 *       引用方的水位断点续传不受影响；</li>
 *   <li><b>参数透传</b>：节点 {@code params} 传给子管道占位符（{@code ${...}}），
 *       同一份子管道定义天天跑不同批次；</li>
 *   <li><b>嵌套防护</b>：子管道还能再引用管道，但深度超限（8 层）直接报错 ——
 *       自引用不会无限递归。</li>
 * </ul>
 *
 * <pre>
 * - id: orders
 *   type: pipeline.ref
 *   config:
 *     name: 清洗后的订单          # 已登记的管道名
 *     params: {status: "1"}      # 可选：子管道占位符取值
 *     eventTimeColumn: created_at # 可选：引用方自己接水位 / 断点续传
 * </pre>
 *
 * @author zifang
 */
public class PipelineRefSource extends AbstractPullSource {

    /** 嵌套引用深度上限（自引用 / 环引用在这里断掉）。 */
    static final int MAX_DEPTH = 8;

    private static final ThreadLocal<Integer> DEPTH = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    private final DataSourceProvider provider;
    private final ScriptSandbox sandbox;
    private final PipelineRefResolver resolver;

    private List<Map<String, Object>> rows;
    private int index;

    /**
     * @param node     节点声明
     * @param provider 数据源解析（透传给子管道执行）
     * @param sandbox  脚本沙箱（透传给子管道执行）
     * @param resolver 管道解析（名字 → 定义）
     */
    public PipelineRefSource(PipelineNode node, DataSourceProvider provider, ScriptSandbox sandbox,
                             PipelineRefResolver resolver) {
        super(node);
        this.provider = provider;
        this.sandbox = sandbox;
        this.resolver = resolver;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        String name = node.string("name");
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("pipeline.ref 节点 " + node.getId()
                    + " 必须声明 name（要引用的管道名）");
        }
        if (resolver == null) {
            throw new IllegalStateException("pipeline.ref 节点 " + node.getId()
                    + " 需要管道解析器（PipelineRefResolver），当前执行环境没有配置");
        }
        int depth = DEPTH.get();
        if (depth >= MAX_DEPTH) {
            throw new IllegalStateException("pipeline.ref 嵌套超过 " + MAX_DEPTH
                    + " 层（可能自引用成环）：节点 " + node.getId());
        }
        DEPTH.set(depth + 1);
        try {
            load(name.trim());
        } finally {
            DEPTH.set(depth);
        }
    }

    /** 执行子管道（视图口径）并收下全部产出。 */
    private void load(String name) throws Exception {
        PipelineSpec stored = resolver.resolve(name);
        if (stored == null) {
            throw new IllegalArgumentException("引用的管道不存在: " + name);
        }
        PipelineSpec sub = PipelineParams.resolve(PipelineParser.copyOf(stored), params());
        // 视图口径：子管道的汇全部换成 collect（只要产出，不落地）
        for (PipelineNode child : sub.getNodes()) {
            if (child.getOperatorType() != null && child.getOperatorType().isSink()) {
                child.setType("collect.sink");
                child.setOperatorType(OperatorType.COLLECT_SINK);
            }
        }
        // 全新内存状态 + 无游标存储 ⇒ 不读不写水位与状态快照
        PipelineExecutor executor = new PipelineExecutor(provider, new HeapStateBackend(),
                sandbox, null, null, null, resolver);
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        executor.execute(sub, captured);
        rows = new ArrayList<Map<String, Object>>();
        for (Operator operator : captured.values()) {
            if (operator instanceof SinkOperators.CollectSink) {
                rows.addAll(((SinkOperators.CollectSink) operator).rows());
            }
        }
        index = 0;
    }

    /** 节点声明的参数（Object → String，供占位符替换）。 */
    private Map<String, String> params() {
        Map<String, Object> declared = node.map("params");
        Map<String, String> params = new LinkedHashMap<String, String>();
        if (declared == null) {
            return params;
        }
        for (Map.Entry<String, Object> entry : declared.entrySet()) {
            if (entry.getValue() != null) {
                params.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return params;
    }

    @Override
    protected Map<String, Object> doNext() {
        if (rows == null || index >= rows.size()) {
            return null;
        }
        return rows.get(index++);
    }
}
