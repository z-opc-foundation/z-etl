package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.job.ExpressionSpec;
import com.zifang.z.etl.core.job.FieldMapping;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorValidator;
import com.zifang.z.etl.core.job.JobMode;
import com.zifang.z.etl.core.job.ScriptSpec;
import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.z.etl.core.job.TargetSpec;
import com.zifang.z.etl.core.job.TransformSpec;
import com.zifang.z.etl.core.reader.JdbcRowReader;
import com.zifang.z.etl.core.reader.RowConsumer;
import com.zifang.z.etl.core.reconcile.ReconciliationReport;
import com.zifang.z.etl.core.reconcile.Reconciler;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.transform.ExpressionTransformer;
import com.zifang.z.etl.core.transform.FieldMappingTransformer;
import com.zifang.z.etl.core.transform.RowTransformer;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.transform.ScriptTransformer;
import com.zifang.z.etl.core.transform.TransformChain;
import com.zifang.z.etl.core.transform.TypeNormalizationTransformer;
import com.zifang.z.etl.core.util.RowValues;
import com.zifang.z.etl.core.writer.JdbcUpsertWriter;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ETL 执行器：reader → transform 链 → writer。
 *
 * <p>两段模式（需求 008）：**初始化模式（首跑全量）→ 自动切增量模式（水位接力）**。
 * 切不切增量由「有没有水位记录」决定，不靠人工改配置：
 * <ul>
 *   <li>{@code mode=auto}（默认）：首跑无水位 ⇒ 全量；跑完落水位 ⇒ 下次自动走增量。</li>
 *   <li>{@code mode=init}：强制全量重跑（幂等重跑验证用）。</li>
 *   <li>{@code mode=incremental}：强制增量；无水位直接失败（不静默退化成全量）。</li>
 * </ul>
 *
 * <p>纪律：单机线程池起步，不做分布式 master-worker（卡面纪律）。
 *
 * @author zifang
 */
public class EtlEngine {

    private final DataSourceProvider dataSourceProvider;
    private final JobRepository jobRepository;
    private final CheckpointStore checkpointStore;
    private final Reconciler reconciler = new Reconciler();

    public EtlEngine(DataSourceProvider dataSourceProvider, CheckpointStore checkpointStore) {
        this(dataSourceProvider, null, checkpointStore);
    }

    public EtlEngine(DataSourceProvider dataSourceProvider, JobRepository jobRepository,
                     CheckpointStore checkpointStore) {
        this.dataSourceProvider = dataSourceProvider;
        this.jobRepository = jobRepository;
        this.checkpointStore = checkpointStore;
    }

    /**
     * 按作业名跑（描述从仓储取 —— 「落库后可重跑」的口径）。
     *
     * @param jobName 作业名
     * @return 运行结果
     */
    public EtlRunResult run(String jobName) {
        if (jobRepository == null) {
            throw new IllegalStateException("未配置作业仓储，无法按作业名运行");
        }
        JobDescriptor descriptor = jobRepository.find(jobName);
        if (descriptor == null) {
            throw new IllegalArgumentException("作业不存在: " + jobName);
        }
        return run(descriptor);
    }

    /**
     * 跑一个作业描述。
     *
     * @param descriptor 作业描述
     * @return 运行结果
     */
    public EtlRunResult run(JobDescriptor descriptor) {
        JobDescriptorValidator.validate(descriptor);

        EtlRunResult result = new EtlRunResult();
        result.setJobName(descriptor.getName());
        result.setStartedAt(System.currentTimeMillis());

        SourceSpec sourceSpec = descriptor.getSource();
        TargetSpec targetSpec = descriptor.getTarget();
        String watermarkColumn = resolveWatermarkColumn(descriptor);
        String watermarkType = resolveWatermarkType(descriptor);

        Checkpoint checkpoint = checkpointStore.load(descriptor.getName());
        String watermarkBefore = checkpoint == null ? null : checkpoint.getWatermarkValue();

        boolean incremental = decideIncremental(descriptor, checkpoint, watermarkColumn, result);

        DataSource sourceDs = dataSourceProvider.resolve(sourceSpec.getDataSourceRef());
        DataSource targetDs = dataSourceProvider.resolve(targetSpec.getDataSourceRef());

        final Object[] maxWatermark = new Object[]{null};
        final long[] read = new long[]{0L};

        JdbcRowReader reader = new JdbcRowReader(sourceDs, sourceSpec,
                incremental ? watermarkColumn : null,
                incremental ? watermarkBefore : null,
                watermarkType);

        TransformChain chain;
        ScriptSandbox sandbox = new ScriptSandbox();
        try {
            chain = buildChain(descriptor.getTransform(), sandbox);
        } catch (RuntimeException e) {
            sandbox.close();
            throw e;
        }
        List<String> keyColumns = new ArrayList<String>(descriptor.getIdempotencyKey());
        JdbcUpsertWriter writer = new JdbcUpsertWriter(targetDs, targetSpec, keyColumns,
                targetColumns(descriptor, targetSpec));

        try {
            read[0] = reader.read(new RowConsumer() {
                @Override
                public void accept(Map<String, Object> row) throws Exception {
                    Map<String, Object> transformed = chain.transform(row);
                    writer.write(transformed);
                    if (watermarkColumn != null) {
                        Object value = RowValues.get(row, watermarkColumn);
                        if (value != null && isNewerWatermark(value, maxWatermark[0], watermarkType)) {
                            maxWatermark[0] = value;
                        }
                    }
                }
            });
            writer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("作业 " + descriptor.getName() + " 执行失败: " + e.getMessage(), e);
        } finally {
            writer.close();
            sandbox.close();
        }

        result.setRowsRead(read[0]);
        result.setRowsWritten(writer.written());
        result.setWatermarkBefore(watermarkBefore);
        result.setMode(incremental ? "incremental" : "init");

        // 水位落库：全量首跑也落 ⇒ 下次自动切增量；增量跑完推进位点
        if (watermarkColumn != null) {
            String watermarkAfter = maxWatermark[0] == null ? watermarkBefore : watermarkValue(maxWatermark[0], watermarkType);
            result.setWatermarkAfter(watermarkAfter);
            checkpointStore.save(new Checkpoint(descriptor.getName(), watermarkColumn, watermarkType,
                    watermarkAfter, result.getMode(), System.currentTimeMillis()));
        } else {
            result.setWatermarkAfter(watermarkBefore);
            result.addNote("该作业未声明水位列，只能全量跑");
        }

        // 对账（行数 + 校验和双口径；源侧按作业实际取数口径复现：过滤下推 / 聚合下推都算进去）
        ReconciliationReport report;
        try {
            report = reconciler.reconcile(sourceDs, sourceSpec, sourceKeyColumns(descriptor),
                    targetDs, targetSpec.getTable(), keyColumns);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("作业 " + descriptor.getName() + " 对账失败: " + e.getMessage(), e);
        }
        result.setReport(report);
        result.setReconciled(report.matched());
        result.setFinishedAt(System.currentTimeMillis());

        if (jobRepository != null) {
            jobRepository.recordRun(result);
        }
        return result;
    }

    /**
     * 决定这次是不是增量跑。
     *
     * @return true = 增量
     */
    private boolean decideIncremental(JobDescriptor descriptor, Checkpoint checkpoint, String watermarkColumn,
                                      EtlRunResult result) {
        JobMode declared = descriptor.modeEnum();
        if (declared == null) {
            throw new IllegalArgumentException("mode 非法: " + descriptor.getMode());
        }
        boolean hasWatermark = checkpoint != null
                && checkpoint.getWatermarkValue() != null
                && !checkpoint.getWatermarkValue().trim().isEmpty();
        if (declared == JobMode.INCREMENTAL) {
            if (!hasWatermark) {
                throw new IllegalStateException("作业 " + descriptor.getName()
                        + " 声明为 incremental 但没有水位记录（先跑一次全量，或改 mode=auto）");
            }
            return true;
        }
        if (declared == JobMode.INIT) {
            return false;
        }
        // auto：无水位 ⇒ 全量首跑；有水位 ⇒ 自动切增量
        if (hasWatermark && watermarkColumn != null) {
            result.addNote("检测到水位记录，自动切增量模式");
            return true;
        }
        result.addNote("无水位记录，初始化模式（全量首跑）");
        return false;
    }

    /**
     * transform 链：字段映射 → 行级表达式 → 脚本沙箱 → 类型归一（链尾收口）。
     *
     * <p>顺序口径：
     * <ol>
     *   <li>字段映射先做 —— 表达式与脚本看到的列名就是**目标列名**（不是源列名）。</li>
     *   <li>表达式与脚本产出新列，类型归一最后统一收口（含定点小数位）。</li>
     * </ol>
     */
    private TransformChain buildChain(TransformSpec transform, ScriptSandbox sandbox) {
        TransformChain chain = new TransformChain();
        chain.add(new FieldMappingTransformer(transform));
        chain.add(new ExpressionTransformer(transform == null ? null : transform.getExpressions(), sandbox));
        chain.add(new ScriptTransformer(transform == null ? null : transform.getScripts(), sandbox));
        chain.add(new TypeNormalizationTransformer(transform == null ? null : transform.getTypeNormalizations()));
        return chain;
    }

    /** 目标列：声明优先；否则由字段映射 + 表达式 + 脚本产出推导。 */
    private List<String> targetColumns(JobDescriptor descriptor, TargetSpec targetSpec) {
        if (targetSpec.getColumns() != null && !targetSpec.getColumns().isEmpty()) {
            return targetSpec.getColumns();
        }
        TransformSpec transform = descriptor.getTransform();
        List<String> columns = new ArrayList<String>();
        if (transform != null && transform.getFieldMappings() != null && !transform.getFieldMappings().isEmpty()) {
            for (FieldMapping mapping : transform.getFieldMappings()) {
                if (mapping != null && mapping.getSource() != null) {
                    columns.add(mapping.effectiveTarget());
                }
            }
        }
        if (transform != null && transform.getExpressions() != null) {
            for (ExpressionSpec expression : transform.getExpressions()) {
                if (expression != null && expression.getName() != null) {
                    columns.add(expression.getName());
                }
            }
        }
        if (transform != null && transform.getScripts() != null) {
            for (ScriptSpec script : transform.getScripts()) {
                if (script != null && script.getName() != null) {
                    columns.add(script.getName());
                }
            }
        }
        return columns.isEmpty() ? null : columns;
    }

    /** 源侧幂等键列：聚合下推时 = 分组列；否则由目标幂等键列经字段映射反查源列名；查不到就同名假设。 */
    private List<String> sourceKeyColumns(JobDescriptor descriptor) {
        SourceSpec sourceSpec = descriptor.getSource();
        if (sourceSpec != null && sourceSpec.getPushdown() != null
                && sourceSpec.getPushdown().isAggregatePushdown()) {
            // 聚合下推：源侧「行」是聚合结果行，键列 = 分组列
            return new ArrayList<String>(sourceSpec.getPushdown().getGroupBy());
        }
        Map<String, String> targetToSource = new LinkedHashMap<String, String>();
        TransformSpec transform = descriptor.getTransform();
        if (transform != null && transform.getFieldMappings() != null) {
            for (FieldMapping mapping : transform.getFieldMappings()) {
                if (mapping != null && mapping.getSource() != null) {
                    targetToSource.put(mapping.effectiveTarget(), mapping.getSource());
                }
            }
        }
        List<String> sourceColumns = new ArrayList<String>();
        for (String key : descriptor.getIdempotencyKey()) {
            String sourceColumn = targetToSource.get(key);
            sourceColumns.add(sourceColumn == null ? key : sourceColumn);
        }
        return sourceColumns;
    }

    private String resolveWatermarkColumn(JobDescriptor descriptor) {
        if (descriptor.getWatermark() != null
                && descriptor.getWatermark().getColumn() != null
                && !descriptor.getWatermark().getColumn().trim().isEmpty()) {
            return descriptor.getWatermark().getColumn();
        }
        SourceSpec source = descriptor.getSource();
        if (source != null && source.getWatermarkColumn() != null
                && !source.getWatermarkColumn().trim().isEmpty()) {
            return source.getWatermarkColumn();
        }
        return null;
    }

    private String resolveWatermarkType(JobDescriptor descriptor) {
        if (descriptor.getWatermark() != null && descriptor.getWatermark().getType() != null
                && !descriptor.getWatermark().getType().trim().isEmpty()) {
            return descriptor.getWatermark().getType().trim().toUpperCase();
        }
        return "LONG";
    }

    /** 水位比较：LONG 比数值，TIMESTAMP 比时间。 */
    private boolean isNewerWatermark(Object candidate, Object current, String type) {
        if (current == null) {
            return true;
        }
        if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return toTimestamp(candidate).after(toTimestamp(current));
        }
        return toLong(candidate) > toLong(current);
    }

    private String watermarkValue(Object value, String watermarkType) {
        if ("TIMESTAMP".equalsIgnoreCase(watermarkType)) {
            return toTimestamp(value).toString();
        }
        return String.valueOf(toLong(value));
    }

    private Timestamp toTimestamp(Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        return Timestamp.valueOf(String.valueOf(value));
    }

    private long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }
}
