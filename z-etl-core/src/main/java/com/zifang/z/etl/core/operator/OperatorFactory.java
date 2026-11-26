package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.connector.FileCsvSink;
import com.zifang.z.etl.core.connector.FileCsvSource;
import com.zifang.z.etl.core.connector.FileJsonSink;
import com.zifang.z.etl.core.connector.FileXlsxSink;
import com.zifang.z.etl.core.connector.FileJsonSource;
import com.zifang.z.etl.core.connector.FileXlsxSource;
import com.zifang.z.etl.core.connector.FileXmlSink;
import com.zifang.z.etl.core.connector.FileXmlSource;
import com.zifang.z.etl.core.connector.HttpSink;
import com.zifang.z.etl.core.connector.HttpSource;
import com.zifang.z.etl.core.connector.MillSource;
import com.zifang.z.etl.core.connector.InlineSource;
import com.zifang.z.etl.core.connector.MessageSink;
import com.zifang.z.etl.core.connector.MessageSource;
import com.zifang.z.etl.core.connector.ScriptSource;
import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import com.zifang.z.etl.core.spi.PipelineRefResolver;
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
        return create(node, provider, sandbox, null);
    }

    /**
     * 按节点声明建算子（带消息总线）。
     *
     * @param node       节点声明
     * @param provider   数据源解析（源 / 汇用）
     * @param sandbox    脚本沙箱（filter / map / script 用）
     * @param messageBus 消息总线（消息类源 / 汇用；可空 ⇒ 用到时报错说明）
     * @return 算子实例
     */
    public static Operator create(PipelineNode node, DataSourceProvider provider, ScriptSandbox sandbox,
                                  MessageBus messageBus) {
        return create(node, provider, sandbox, messageBus, null);
    }

    /**
     * 按节点声明建算子（带消息总线与管道解析）。
     *
     * @param node       节点声明
     * @param provider   数据源解析（源 / 汇用）
     * @param sandbox    脚本沙箱（filter / map / script 用）
     * @param messageBus 消息总线（消息类源 / 汇用；可空 ⇒ 用到时报错说明）
     * @param resolver   管道解析（pipeline.ref 源用；可空 ⇒ 引用节点打开时报错说明）
     * @return 算子实例
     */
    public static Operator create(PipelineNode node, DataSourceProvider provider, ScriptSandbox sandbox,
                                  MessageBus messageBus, PipelineRefResolver resolver) {
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
            case FILE_CSV_SOURCE:
                return new FileCsvSource(node);
            case FILE_JSON_SOURCE:
                return new FileJsonSource(node);
            case FILE_XLSX_SOURCE:
                return new FileXlsxSource(node);
            case FILE_XML_SOURCE:
                return new FileXmlSource(node);
            case HTTP_SOURCE:
                return new HttpSource(node);
            case MILL_SOURCE:
                return new MillSource(node);
            case INLINE_SOURCE:
                return new InlineSource(node);
            case SCRIPT_SOURCE:
                return new ScriptSource(node, sandbox);
            case MESSAGE_SOURCE:
                return new MessageSource(node, messageBus);
            case PIPELINE_REF:
                return new PipelineRefSource(node, provider, sandbox, resolver);
            case FILTER:
                return new FilterOperator(node, sandbox);
            case ROUTE:
                return new RouteOperator(node, sandbox);
            case MAP:
                return new MapOperator(node, sandbox);
            case INDEX:
                return new IndexOperator(node);
            case SCRIPT:
                return new ScriptOperator(node, sandbox);
            case SCRIPT_REMOTE:
                return new ScriptRemoteOperator(node);
            case ASYNC_MAP:
                return new AsyncMapOperator(node);
            case FIELD_MAPPING:
                return new FieldMappingOperator(node);
            case TYPE_NORMALIZE:
                return new TypeNormalizeOperator(node);
            case ASSERT:
                return new AssertOperator(node, sandbox);
            case AGGREGATE:
                return new AggregateOperator(node);
            case WINDOW_AGGREGATE:
                return new WindowAggregateOperator(node);
            case DEDUP:
                return new DedupOperator(node);
            case JOIN:
                return new JoinOperator(node);
            case DIFF:
                return new DiffOperator(node);
            case INTERVAL_JOIN:
                return new IntervalJoinOperator(node);
            case DIM_JOIN:
                return new DimJoinOperator(node);
            case CEP_PATTERN:
                return new CepPatternOperator(node, sandbox);
            case UNION:
                return new UnionOperator(node);
            case EXPLODE:
                return new ExplodeOperator(node);
            case TOPN:
                return new TopNOperator(node);
            case LIMIT:
                return new LimitOperator(node);
            case SAMPLE:
                return new SampleOperator(node);
            case DISTINCT:
                return new DistinctOperator(node);
            case UNPIVOT:
                return new UnpivotOperator(node);
            case PIVOT:
                return new PivotOperator(node);
            case REGEX_EXTRACT:
                return new RegexExtractOperator(node);
            case SPLIT:
                return new SplitOperator(node);
            case TEMPLATE:
                return new TemplateOperator(node);
            case RENAME:
                return new RenameOperator(node);
            case FLATTEN:
                return new FlattenOperator(node);
            case JSON_EXTRACT:
                return new JsonExtractOperator(node);
            case HASH:
                return new HashOperator(node);
            case MASK:
                return new MaskOperator(node);
            case DATE_FORMAT:
                return new DateFormatOperator(node);
            case SORT:
                return new SortOperator(node);
            case PROFILE:
                return new ProfileOperator(node);
            case JDBC_SINK:
                return new SinkOperators.JdbcSink(node, provider.resolve(node.string("dataSourceRef")));
            case FILE_CSV_SINK:
                return new FileCsvSink(node);
            case FILE_JSON_SINK:
                return new FileJsonSink(node);
            case FILE_XLSX_SINK:
                return new FileXlsxSink(node);
            case FILE_XML_SINK:
                return new FileXmlSink(node);
            case HTTP_SINK:
                return new HttpSink(node);
            case MESSAGE_SINK:
                return new MessageSink(node, messageBus);
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
