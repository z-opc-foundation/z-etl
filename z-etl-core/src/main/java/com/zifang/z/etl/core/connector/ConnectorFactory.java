package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.z.etl.core.job.TargetSpec;
import com.zifang.z.etl.core.operator.OperatorFactory;
import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.PullSource;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import com.zifang.z.etl.core.transform.ScriptSandbox;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 线性作业层的连接器工厂：job 描述 → DAG 层那套算子。
 *
 * <p>关键点：**不另写一套实现**。job 描述里的 {@code source.type: file.csv} 落到
 * {@code file.csv.source} 算子上，行为与 DAG 层完全一致（同一份文件解析、同一套水位口径）。
 *
 * @author zifang
 */
public final class ConnectorFactory {

    private ConnectorFactory() {
    }

    /**
     * 建源算子（并打开）。
     *
     * @param spec             源定义
     * @param eventTimeColumn  事件时间列（线性层的水位列）
     * @param eventTimeType    事件时间类型（LONG / TIMESTAMP）
     * @param tiebreakColumn   平局列（断点续传第二键；可空）
     * @param resumeValue      续跑游标值（首跑传 {@code null}）
     * @param resumeTiebreak   续跑平局值
     * @param provider         数据源解析（JDBC 才用；非 JDBC 传 {@code null} 也行）
     * @param sandbox          脚本沙箱
     * @param messageBus       消息总线
     * @return 已打开的源
     */
    public static PullSource createSource(SourceSpec spec, String eventTimeColumn, String eventTimeType,
                                           String tiebreakColumn, Object resumeValue, Object resumeTiebreak,
                                           DataSourceProvider provider, ScriptSandbox sandbox,
                                           MessageBus messageBus) {
        OperatorType type = ConnectorTypes.sourceType(spec.effectiveType());
        if (type == null) {
            throw new IllegalArgumentException("未知的 source.type: " + spec.getType());
        }
        PipelineNode node = new PipelineNode("src", type);
        node.setConfig(nodeConfig(spec.getOptions(), eventTimeColumn, eventTimeType,
                tiebreakColumn, spec.effectiveType()));
        Operator operator = OperatorFactory.create(node, provider, sandbox, messageBus);
        if (!(operator instanceof PullSource)) {
            throw new IllegalStateException("算子不是拉取式源: " + type.id());
        }
        PullSource source = (PullSource) operator;
        source.setResume(resumeValue, resumeTiebreak);
        try {
            source.open(new OperatorContext("src", null, null));
        } catch (Exception e) {
            throw new IllegalStateException("连接器源打开失败: " + e.getMessage(), e);
        }
        return source;
    }

    /**
     * 建汇算子（并打开）。
     *
     * @param spec       目标定义
     * @param provider   数据源解析（JDBC 才用）
     * @param sandbox    脚本沙箱
     * @param messageBus 消息总线
     * @return 已打开的汇
     */
    public static CountingSink createSink(TargetSpec spec, DataSourceProvider provider,
                                           ScriptSandbox sandbox, MessageBus messageBus) {
        OperatorType type = ConnectorTypes.sinkType(spec.effectiveType());
        if (type == null) {
            throw new IllegalArgumentException("未知的 target.type: " + spec.getType());
        }
        PipelineNode node = new PipelineNode("sink", type);
        node.setConfig(nodeConfig(spec.getOptions(), null, null, null, spec.effectiveType()));
        Operator operator = OperatorFactory.create(node, provider, sandbox, messageBus);
        if (!(operator instanceof CountingSink)) {
            throw new IllegalStateException("算子不是汇: " + type.id());
        }
        CountingSink sink = (CountingSink) operator;
        try {
            sink.open(new OperatorContext("sink", null, null));
        } catch (Exception e) {
            throw new IllegalStateException("连接器汇打开失败: " + e.getMessage(), e);
        }
        return sink;
    }

    /** 连接器配置 = 声明的 options + 线性层的水位口径。 */
    private static Map<String, Object> nodeConfig(Map<String, Object> options, String eventTimeColumn,
                                                  String eventTimeType, String tiebreakColumn, String type) {
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        if (options != null) {
            config.putAll(options);
        }
        if (eventTimeColumn != null) {
            config.put("eventTimeColumn", eventTimeColumn);
            config.put("eventTimeType", eventTimeType == null ? "TIMESTAMP" : eventTimeType);
            Map<String, Object> resume = new LinkedHashMap<String, Object>();
            resume.put("column", eventTimeColumn);
            resume.put("type", eventTimeType == null ? "TIMESTAMP" : eventTimeType);
            if (tiebreakColumn != null) {
                resume.put("tiebreakColumn", tiebreakColumn);
            }
            config.put("resume", resume);
        }
        return config;
    }

    /** 校验连接器必填项（缺项直接报错，不静默跑空）。 */
    public static void validateOptions(String type, Map<String, Object> options, String side) {
        OperatorType operatorType = "source".equals(side)
                ? ConnectorTypes.sourceType(type) : ConnectorTypes.sinkType(type);
        if (operatorType == null) {
            throw new IllegalArgumentException("未知的 " + side + ".type: " + type);
        }
        List<String> required = ConnectorTypes.requiredOptions(operatorType);
        for (String key : required) {
            Object value = options == null ? null : options.get(key);
            if (value == null || String.valueOf(value).trim().isEmpty()) {
                throw new IllegalArgumentException(side + ".options." + key + " 不能为空（类型 "
                        + type + " 需要它）");
            }
        }
    }
}
