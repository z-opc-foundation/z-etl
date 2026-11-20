package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.WatermarkCursor;
import com.zifang.z.etl.core.job.ExpressionSpec;
import com.zifang.z.etl.core.job.FieldMapping;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorValidator;
import com.zifang.z.etl.core.job.JobMode;
import com.zifang.z.etl.core.job.PushdownSpec;
import com.zifang.z.etl.core.job.RetrySpec;
import com.zifang.z.etl.core.job.ScriptSpec;
import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.z.etl.core.job.TargetSpec;
import com.zifang.z.etl.core.job.TransformSpec;
import com.zifang.z.etl.core.connector.ConnectorFactory;
import com.zifang.z.etl.core.connector.ConnectorTypes;
import com.zifang.z.etl.core.reader.ConnectorRowReader;
import com.zifang.z.etl.core.reader.JdbcRowReader;
import com.zifang.z.etl.core.reader.PageHandler;
import com.zifang.z.etl.core.reader.PagedRowReader;
import com.zifang.z.etl.core.reader.RowConsumer;
import com.zifang.z.etl.core.reconcile.ReconciliationReport;
import com.zifang.z.etl.core.reconcile.Reconciler;
import com.zifang.z.etl.core.runtime.PullSource;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import com.zifang.z.etl.core.spi.MessageBus;
import com.zifang.z.etl.core.transform.FieldMappingTransformer;
import com.zifang.z.etl.core.transform.ExpressionTransformer;
import com.zifang.z.etl.core.transform.RowTransformer;
import com.zifang.z.etl.core.transform.ScriptSandbox;
import com.zifang.z.etl.core.transform.ScriptTransformer;
import com.zifang.z.etl.core.transform.TransformChain;
import com.zifang.z.etl.core.transform.TypeNormalizationTransformer;
import com.zifang.z.etl.core.util.Backoff;
import com.zifang.z.etl.core.util.Checksums;
import com.zifang.z.etl.core.util.RowValues;
import com.zifang.z.etl.core.writer.ConnectorRowWriter;
import com.zifang.z.etl.core.writer.JobWriter;
import com.zifang.z.etl.core.writer.JdbcUpsertWriter;
import com.zifang.util.db.support.Identifiers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    private final MessageBus messageBus;
    private final Reconciler reconciler = new Reconciler();

    public EtlEngine(DataSourceProvider dataSourceProvider, CheckpointStore checkpointStore) {
        this(dataSourceProvider, null, checkpointStore, null);
    }

    public EtlEngine(DataSourceProvider dataSourceProvider, JobRepository jobRepository,
                     CheckpointStore checkpointStore) {
        this(dataSourceProvider, jobRepository, checkpointStore, null);
    }

    /**
     * @param dataSourceProvider 数据源解析
     * @param jobRepository      作业仓储
     * @param checkpointStore    水位存储
     * @param messageBus         消息总线（消息类源 / 汇用；可空）
     */
    public EtlEngine(DataSourceProvider dataSourceProvider, JobRepository jobRepository,
                     CheckpointStore checkpointStore, MessageBus messageBus) {
        this.dataSourceProvider = dataSourceProvider;
        this.jobRepository = jobRepository;
        this.checkpointStore = checkpointStore;
        this.messageBus = messageBus;
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
     * 跑一个作业描述（按 {@code retry} 策略失败重试）。
     *
     * <p>默认不重试（{@code maxAttempts=1}）；声明了重试时，失败会**整体重跑**——
     * 水位只在本页写成功后推进 ⇒ 重跑从上次成功页接力，目标侧又是幂等 upsert ⇒ 不重不漏。
     *
     * @param descriptor 作业描述
     * @return 运行结果
     */
    public EtlRunResult run(JobDescriptor descriptor) {
        RetrySpec retry = descriptor.getRetry() == null ? new RetrySpec() : descriptor.getRetry();
        int maxAttempts = retry.effectiveMaxAttempts();
        if (maxAttempts == 1) {
            // 不声明重试 ⇒ 异常原样抛出（行为与从前完全一致）
            EtlRunResult result = runOnce(descriptor);
            record(descriptor, result);
            return result;
        }
        RuntimeException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                EtlRunResult result = runOnce(descriptor);
                if (attempt > 1) {
                    result.addNote("第 " + attempt + " 次尝试成功（前 " + (attempt - 1) + " 次失败后重试）");
                }
                record(descriptor, result);
                return result;
            } catch (RuntimeException e) {
                last = e;
                if (attempt < maxAttempts) {
                    Backoff.sleep(retry.backoffFor(attempt));
                }
            }
        }
        throw new IllegalStateException("作业 " + descriptor.getName() + " 连续 " + maxAttempts
                + " 次失败（最后一次: " + (last == null ? "未知" : last.getMessage()) + "）", last);
    }

    /**
     * 跑一个作业描述（单次尝试；重试由 {@link #run(JobDescriptor)} 负责）。
     *
     * @param descriptor 作业描述
     * @return 运行结果
     */
    private EtlRunResult runOnce(JobDescriptor descriptor) {
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

        // —— 水位游标解析（复合游标 = 水位值 + 平局列值；回溯窗口；越界策略）——
        String tiebreakColumn = resolveTiebreakColumn(descriptor);
        long overlap = resolveOverlap(descriptor);
        String onOutOfRange = resolveOnOutOfRange(descriptor);
        String tiebreakBefore = checkpoint == null ? null : checkpoint.getTiebreakValue();

        boolean sourceJdbc = ConnectorTypes.isJdbc(sourceSpec.effectiveType());
        boolean targetJdbc = ConnectorTypes.isJdbc(targetSpec.effectiveType());
        DataSource sourceDs = sourceJdbc
                ? dataSourceProvider.resolve(sourceSpec.getDataSourceRef()) : null;
        DataSource targetDs = targetJdbc
                ? dataSourceProvider.resolve(targetSpec.getDataSourceRef()) : null;

        // —— 越界检测（只在增量模式、且已有水位时才有意义）——
        if (sourceJdbc && incremental && watermarkColumn != null && watermarkBefore != null
                && !isAggregatePushdown(sourceSpec)) {
            SourceWatermarkStats stats = sourceWatermarkStats(sourceDs, sourceSpec, watermarkColumn,
                    watermarkBefore, watermarkType);
            if (stats.isOutOfRange()) {
                result.setWatermarkOutOfRange(true);
                if ("RESET".equals(onOutOfRange)) {
                    checkpointStore.clear(descriptor.getName());
                    incremental = false;
                    watermarkBefore = null;
                    tiebreakBefore = null;
                    result.addNote("游标越界（源表当前最大水位 " + stats.getMaxValue() + " 早于已存水位 "
                            + checkpoint.getWatermarkValue() + "）：按 onOutOfRange=RESET 清掉水位，本轮退回全量重跑");
                } else {
                    throw new IllegalStateException("作业 " + descriptor.getName() + " 游标越界：源表当前最大水位 "
                            + stats.getMaxValue() + " 早于已存水位 " + watermarkBefore
                            + "（源表可能被清空 / 重建）。按 onOutOfRange=FAIL 口径**不推进水位**；"
                            + "确认后可清掉水位重跑，或把 watermark.onOutOfRange 改成 RESET");
                }
            }
        }

        String runMode = incremental ? "incremental" : "init";
        result.setMode(runMode);
        final String loopMode = runMode;

        final WatermarkCursor[] cursor = new WatermarkCursor[]{null};
        final long[] read = new long[]{0L};

        TransformChain chain;
        ScriptSandbox sandbox = new ScriptSandbox();
        try {
            chain = buildChain(descriptor.getTransform(), sandbox);
        } catch (RuntimeException e) {
            sandbox.close();
            throw e;
        }
        List<String> keyColumns = new ArrayList<String>(descriptor.getIdempotencyKey());

        // —— 取数：JDBC 走 SQL 游标，其余走连接器（文件 / HTTP / 消息 …）——
        PagedRowReader reader;
        PullSource connectorSource = null;
        if (sourceJdbc) {
            reader = new JdbcRowReader(sourceDs, sourceSpec,
                    incremental ? watermarkColumn : null,
                    incremental ? watermarkBefore : null,
                    watermarkType,
                    incremental ? tiebreakColumn : null,
                    incremental ? tiebreakBefore : null,
                    incremental ? overlap : 0L);
        } else {
            connectorSource = ConnectorFactory.createSource(sourceSpec, watermarkColumn, watermarkType,
                    incremental ? tiebreakColumn : null,
                    incremental ? watermarkBefore : null,
                    incremental ? tiebreakBefore : null,
                    dataSourceProvider, sandbox, messageBus);
            reader = new ConnectorRowReader(connectorSource, sourceSpec.getPageSize());
        }

        // —— 写出：JDBC 走 upsert 写出器，其余走连接器汇 ——
        JobWriter writer;
        if (targetJdbc) {
            writer = new JdbcUpsertWriter(targetDs, targetSpec, keyColumns,
                    targetColumns(descriptor, targetSpec));
        } else {
            writer = new ConnectorRowWriter(
                    ConnectorFactory.createSink(targetSpec, dataSourceProvider, sandbox, messageBus));
        }

        // 非 JDBC 侧没有整表可扫 ⇒ 对账改「本轮行内口径」（源行 vs 写出行的键多重集）
        final boolean inlineReconcile = !(sourceJdbc && targetJdbc);
        final long[] sourceChecksumSum = new long[1];
        final long[] targetChecksumSum = new long[1];
        final List<String> sourceKeyColumns = sourceKeyColumns(descriptor);

        try {
            // 页级搬运：读一页 → 写一页 → **提交成功才推进水位** ⇒ 崩在中途水位停在上一个成功页
            read[0] = reader.readPages(new PageHandler() {
                @Override
                public void handle(List<Map<String, Object>> page) throws Exception {
                    for (Map<String, Object> row : page) {
                        Map<String, Object> transformed = chain.transform(row);
                        writer.write(transformed);
                        if (inlineReconcile) {
                            sourceChecksumSum[0] += Checksums.rowHash(row, sourceKeyColumns);
                            targetChecksumSum[0] += Checksums.rowHash(transformed, keyColumns);
                        }
                        WatermarkCursor rowCursor = cursorOf(row, watermarkColumn, tiebreakColumn, watermarkType);
                        if (rowCursor != null && (cursor[0] == null || rowCursor.isAfter(cursor[0]))) {
                            cursor[0] = rowCursor;
                        }
                    }
                    writer.flush();
                    if (cursor[0] != null && watermarkColumn != null) {
                        saveCheckpoint(descriptor.getName(), watermarkColumn, watermarkType,
                                tiebreakColumn, cursor[0], loopMode);
                        result.setCheckpointAdvances(result.getCheckpointAdvances() + 1);
                    }
                }
            });
            writer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("作业 " + descriptor.getName() + " 执行失败: " + e.getMessage(), e);
        } finally {
            try {
                writer.close();
            } catch (Exception ignore) {
                // 关闭异常不影响主异常
            }
            if (connectorSource != null) {
                try {
                    connectorSource.close();
                } catch (Exception ignore) {
                    // 关闭异常不影响主异常
                }
            }
            sandbox.close();
        }

        result.setRowsRead(read[0]);
        result.setRowsWritten(writer.written());
        result.setWatermarkBefore(watermarkBefore);
        result.setMode(loopMode);

        // 水位落库：全量首跑也落 ⇒ 下次自动切增量；增量跑完推进位点
        if (watermarkColumn != null) {
            WatermarkCursor after = cursor[0];
            String watermarkAfter = after == null ? watermarkBefore : after.getValue();
            result.setWatermarkAfter(watermarkAfter);
            result.setWatermarkTiebreakAfter(after == null ? tiebreakBefore : after.getTiebreakValue());
            if (after != null) {
                saveCheckpoint(descriptor.getName(), watermarkColumn, watermarkType, tiebreakColumn, after, loopMode);
            } else {
                result.addNote("本轮没有读到带水位值的行，水位保持不变");
            }
        } else {
            result.setWatermarkAfter(watermarkBefore);
            result.addNote("该作业未声明水位列，只能全量跑");
        }

        // 对账（行数 + 校验和双口径；源侧按作业实际取数口径复现：过滤下推 / 聚合下推都算进去）
        ReconciliationReport report;
        if (inlineReconcile) {
            report = inlineReport(read[0], writer.written(), sourceChecksumSum[0], targetChecksumSum[0]);
            result.addNote("非 JDBC 侧没有整表可扫：对账按本轮行内口径（源行 vs 写出行的幂等键多重集）");
        } else {
            try {
                report = reconciler.reconcile(sourceDs, sourceSpec, sourceKeyColumns,
                        targetDs, targetSpec.getTable(), keyColumns);
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException("作业 " + descriptor.getName() + " 对账失败: " + e.getMessage(), e);
            }
        }
        result.setReport(report);
        result.setReconciled(report.matched());

        // —— 时钟回拨 / 迟到数据：源目标行数有落差 ⇒ 报「没搬过去的行数」+ 游标之前压着的行数 ——
        if (sourceJdbc && !report.isCountMatched() && watermarkColumn != null && watermarkBefore != null
                && !isAggregatePushdown(sourceSpec)) {
            long shortfall = report.getSourceCount() - report.getTargetCount();
            if (shortfall > 0L) {
                long behindCursor = countRowsBehindCursor(sourceDs, sourceSpec, watermarkColumn,
                        watermarkBefore, watermarkType);
                result.setStaleRows(shortfall);
                result.addNote("源/目标差 " + shortfall + " 行未搬过去，且源侧有 " + behindCursor
                        + " 行的水位早于当前游标（疑似时钟回拨 / 迟到数据）："
                        + "纯时间游标搬不到游标之前的行；配 watermark.overlap（回溯窗口）可兜住，"
                        + "或改用自增 id 游标");
            }
        }

        result.setFinishedAt(System.currentTimeMillis());

        return result;
    }

    /** 记录运行履历（重试包装层负责，保证「第 N 次尝试成功」的注记也在履历里）。 */
    private void record(JobDescriptor descriptor, EtlRunResult result) {
        if (jobRepository != null) {
            jobRepository.recordRun(result);
        }
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
        SourceSpec source = descriptor.getSource();
        if (source != null && source.getPushdown() != null && source.getPushdown().isAggregatePushdown()) {
            // 聚合下推：源侧「行」是聚合结果行，键列 = 分组列
            return new ArrayList<String>(source.getPushdown().getGroupBy());
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

    /** 平局列（同秒多多的第二键）；未声明则单键游标。 */
    private String resolveTiebreakColumn(JobDescriptor descriptor) {
        if (descriptor.getWatermark() != null
                && descriptor.getWatermark().getTiebreakColumn() != null
                && !descriptor.getWatermark().getTiebreakColumn().trim().isEmpty()) {
            return descriptor.getWatermark().getTiebreakColumn().trim();
        }
        return null;
    }

    /** 回溯窗口（0 = 关闭）。 */
    private long resolveOverlap(JobDescriptor descriptor) {
        if (descriptor.getWatermark() != null) {
            return Math.max(0L, descriptor.getWatermark().getOverlap());
        }
        return 0L;
    }

    /** 游标越界策略（FAIL / RESET）。 */
    private String resolveOnOutOfRange(JobDescriptor descriptor) {
        if (descriptor.getWatermark() != null) {
            return descriptor.getWatermark().effectiveOnOutOfRange();
        }
        return "FAIL";
    }

    /** 行 → 游标（水位值 + 平局列值）；水位列为空时返回 null。 */
    private WatermarkCursor cursorOf(Map<String, Object> row, String watermarkColumn,
                                     String tiebreakColumn, String watermarkType) {
        if (watermarkColumn == null) {
            return null;
        }
        Object value = RowValues.get(row, watermarkColumn);
        if (value == null) {
            return null;
        }
        Object tiebreak = tiebreakColumn == null ? null : RowValues.get(row, tiebreakColumn);
        return WatermarkCursor.of(watermarkColumn, watermarkType, value, tiebreakColumn, tiebreak);
    }

    /** 落水位（复合游标：水位值 + 平局列值 + 上次推进时间）。 */
    private void saveCheckpoint(String jobName, String watermarkColumn, String watermarkType,
                                String tiebreakColumn, WatermarkCursor cursor, String mode) {
        checkpointStore.save(new Checkpoint(jobName, watermarkColumn, watermarkType,
                cursor == null ? null : cursor.getValue(),
                tiebreakColumn, cursor == null ? null : cursor.getTiebreakValue(),
                mode, System.currentTimeMillis()));
    }

    /**
     * 行内对账报告（非 JDBC 侧：没有整表可扫，按本轮读到的行与写出的行算键多重集）。
     *
     * @param rowsRead     读出行数
     * @param rowsWritten  写出行数
     * @param sourceSum    源侧键散列和
     * @param targetSum    目标侧键散列和
     * @return 对账报告
     */
    private ReconciliationReport inlineReport(long rowsRead, long rowsWritten,
                                              long sourceSum, long targetSum) {
        ReconciliationReport report = new ReconciliationReport();
        report.setSourceCount(rowsRead);
        report.setTargetCount(rowsWritten);
        report.setSourceChecksum(Long.toHexString(sourceSum));
        report.setTargetChecksum(Long.toHexString(targetSum));
        report.setCountMatched(rowsRead == rowsWritten);
        report.setChecksumMatched(sourceSum == targetSum);
        return report;
    }

    /**
     * 源侧水位统计：当前最大水位 + 早于游标的滞留行数（时钟回拨 / 迟到数据的量）。
     *
     * <p>口径与对账一致：按作业的过滤口径（静态 where + 下推过滤）统计。
     */
    private SourceWatermarkStats sourceWatermarkStats(DataSource sourceDs, SourceSpec sourceSpec,
                                                      String watermarkColumn, String watermarkValue,
                                                      String watermarkType) {
        SourceWatermarkStats stats = new SourceWatermarkStats();
        String column = Identifiers.require(watermarkColumn);
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total, MAX(").append(column)
                .append(") AS max_value FROM ").append(tableRef(sourceSpec.getTable()));
        String where = sourceWhere(sourceSpec);
        if (where != null) {
            sql.append(" WHERE ").append(where);
        }
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = sourceDs.getConnection();
            statement = connection.prepareStatement(sql.toString());
            rs = statement.executeQuery();
            if (rs.next()) {
                stats.setTotal(rs.getLong("total"));
                Object max = rs.getObject("max_value");
                stats.setMaxValue(max == null ? null : WatermarkCursor.format(max, watermarkType));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("统计源侧水位失败: " + e.getMessage(), e);
        } finally {
            closeQuietly(rs);
            closeQuietly(statement);
            closeQuietly(connection);
        }
        return stats;
    }

    /**
     * 统计「水位早于当前游标」的行数（游标已经越过、不会再被扫到的行）。
     *
     * <p>这是**时钟回拨 / 迟到数据**的辅助信号：源库时钟往回拨 ⇒ 新写的行时间戳早于游标 ⇒
     * 纯时间游标永远扫不到它们。注意：源与目标是两个库，**跨库没法做反连接**，
     * 所以「没搬过去的行数」用源/目标行数差额口径（见 run 里的落差判定），
     * 这里只给出「游标之前还压着多少行」这个上下文数。
     *
     * @return 游标之前的行数
     */
    private long countRowsBehindCursor(DataSource sourceDs, SourceSpec sourceSpec,
                                       String watermarkColumn, String watermarkValue, String watermarkType) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(tableRef(sourceSpec.getTable()))
                .append(" WHERE ").append(Identifiers.require(watermarkColumn)).append(" < ?");
        String where = sourceWhere(sourceSpec);
        if (where != null) {
            sql.append(" AND (").append(where).append(")");
        }
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = sourceDs.getConnection();
            statement = connection.prepareStatement(sql.toString());
            statement.setObject(1, WatermarkCursor.normalize(watermarkValue, watermarkType));
            rs = statement.executeQuery();
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new IllegalStateException("统计游标之前的行数失败: " + e.getMessage(), e);
        } finally {
            closeQuietly(rs);
            closeQuietly(statement);
            closeQuietly(connection);
        }
    }

    /** 源侧过滤口径（静态 where + 下推过滤），与对账口径一致。 */
    private String sourceWhere(SourceSpec sourceSpec) {
        List<String> parts = new ArrayList<String>();
        if (sourceSpec.getWhere() != null && !sourceSpec.getWhere().trim().isEmpty()) {
            parts.add("(" + sourceSpec.getWhere().trim() + ")");
        }
        PushdownSpec pushdown = sourceSpec.getPushdown();
        if (pushdown != null && pushdown.hasFilter()) {
            parts.add("(" + pushdown.getFilter().trim() + ")");
        }
        return parts.isEmpty() ? null : String.join(" AND ", parts);
    }

    /** 聚合下推时源侧「行」是聚合结果行，逐行水位口径不适用 ⇒ 跳过越界/回拨检测。 */
    private boolean isAggregatePushdown(SourceSpec sourceSpec) {
        PushdownSpec pushdown = sourceSpec.getPushdown();
        return pushdown != null && pushdown.isAggregatePushdown();
    }

    /** 表引用（schema.table 两段，段内白名单）。 */
    private String tableRef(String table) {
        String[] parts = Identifiers.parts(table);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(parts[i]);
        }
        return sb.toString();
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignore) {
                // 关闭异常不影响主流程
            }
        }
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
