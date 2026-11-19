package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.transform.ScriptSandbox;

/**
 * 算子工厂：节点声明 → 算子实例。
 *
 * <p>执行器不认识任何具体算子（只认 {@link Operator} 面），建算子的事全在这里 ——
 * 加一个新算子 = 加一个类型串 + 工厂一个分支，执行器零改动。
 *
 * @author zifang
 */
public final class OperatorFactory {

    private OperatorFactory() {
    }

    /**
     * 按节点声明建算子。
     *
     * @param node     节点声明
     * @param provider 数据源解析（源 / 汇用）
     * @param sandbox  脚本沙箱（filter / map / script 用）
     * @return 算子实例
     */
    public static Operator create(PipelineNode node, DataSourceProvider provider, ScriptSandbox sandbox) {
        OperatorType type = node.getOperatorType();
        if (type == null) {
            type = OperatorType.fromId(node.getType());
        }
        if (type == null) {
            throw new IllegalArgumentException("节点 " + node.getId() + " 的类型无法识别: " + node.getType());
        }
        switch (type) {
            case JDBC_SOURCE:
                return new SourceOperator(node, provider.resolve(node.string("dataSourceRef")));
            case FILTER:
                return new FilterOperator(node, sandbox);
            case MAP:
                return new MapOperator(node, sandbox);
            case SCRIPT:
                return new ScriptOperator(node, sandbox);
            case FIELD_MAPPING:
                return new FieldMappingOperator(node);
            case TYPE_NORMALIZE:
                return new TypeNormalizeOperator(node);
            case AGGREGATE:
                return new AggregateOperator(node);
            case WINDOW_AGGREGATE:
                return new WindowAggregateOperator(node);
            case DEDUP:
                return new DedupOperator(node);
            case JOIN:
                return new JoinOperator(node);
            case UNION:
                return new UnionOperator(node);
            case JDBC_SINK:
                return new SinkOperators.JdbcSink(node, provider.resolve(node.string("dataSourceRef")));
            case CONSOLE_SINK:
                return new SinkOperators.ConsoleSink(node);
            case COLLECT_SINK:
                return new SinkOperators.CollectSink(node);
            default:
                throw new IllegalArgumentException("节点 " + node.getId() + " 的类型没有对应算子: " + type.id());
        }
    }

    /** 多路合并（透传）：多条入边天然汇成一路，节点本身只做转发。 */
    public static final class UnionOperator implements Operator {

        private final PipelineNode node;

        public UnionOperator(PipelineNode node) {
            this.node = node;
        }

        @Override
        public void open(OperatorContext context) {
            // 透传，无资源
        }

        @Override
        public void processElement(Record record, Output output) {
            output.collect(record);
        }

        @Override
        public void processWatermark(long watermark) {
            // 透传
        }

        @Override
        public void onTimer(long timestamp, String timerName) {
            // 无定时器
        }

        @Override
        public void close() {
            // 无资源
        }

        @Override
        public String operatorId() {
            return node.getId();
        }
    }
}
