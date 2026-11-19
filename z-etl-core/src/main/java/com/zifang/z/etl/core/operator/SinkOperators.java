package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.job.TargetSpec;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.writer.JdbcUpsertWriter;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 汇算子三件套：JDBC 落地 / 控制台 / 内存收集。
 *
 * <ul>
 *   <li>{@link JdbcSink}：复用旧引擎已验收的 {@link JdbcUpsertWriter}（批量 upsert + 幂等键 +
 *       GENERIC 方言 UPDATE-then-INSERT 兜底）⇒ 幂等口径与旧引擎一致；</li>
 *   <li>{@link ConsoleSink}：调试用，行打到控制台；</li>
 *   <li>{@link CollectSink}：测试 / 预览用，行收进内存列表（断言直接查它）。</li>
 * </ul>
 *
 * @author zifang
 */
public final class SinkOperators {

    private SinkOperators() {
    }

    /** JDBC 落地汇。 */
    public static final class JdbcSink implements Operator {

        private final PipelineNode node;
        private final DataSource dataSource;
        private JdbcUpsertWriter writer;

        public JdbcSink(PipelineNode node, DataSource dataSource) {
            this.node = node;
            this.dataSource = dataSource;
        }

        @Override
        public void open(OperatorContext context) {
            TargetSpec spec = new TargetSpec();
            spec.setDataSourceRef(node.string("dataSourceRef"));
            spec.setTable(node.string("table"));
            if (node.string("table") == null) {
                throw new IllegalArgumentException("jdbc.sink 节点 " + node.getId() + " 缺少 table");
            }
            spec.setBatchSize(node.integer("batchSize", 200));
            spec.setWriteMode(node.string("writeMode", "upsert"));
            List<String> columns = node.strings("columns");
            if (!columns.isEmpty()) {
                spec.setColumns(columns);
            }
            writer = new JdbcUpsertWriter(dataSource, spec, node.strings("idempotencyKey"),
                    columns.isEmpty() ? null : columns);
        }

        @Override
        public void processElement(Record record, Output output) throws Exception {
            writer.write(record.row());
        }

        @Override
        public void processWatermark(long watermark) {
            // 汇不涉水位
        }

        @Override
        public void onTimer(long timestamp, String timerName) {
            // 汇不涉定时器
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }

        /** 已写出的行数。 */
        public long written() {
            return writer == null ? 0L : writer.written();
        }

        @Override
        public String operatorId() {
            return node.getId();
        }
    }

    /** 控制台汇（调试用）。 */
    public static final class ConsoleSink implements Operator {

        private final PipelineNode node;
        private long printed;

        public ConsoleSink(PipelineNode node) {
            this.node = node;
        }

        @Override
        public void open(OperatorContext context) {
            // 调试汇无状态
        }

        @Override
        public void processElement(Record record, Output output) {
            printed++;
            System.out.println("[etl:" + node.getId() + "] " + record.row());
        }

        @Override
        public void processWatermark(long watermark) {
            // 调试汇不涉水位
        }

        @Override
        public void onTimer(long timestamp, String timerName) {
            // 调试汇不涉定时器
        }

        @Override
        public void close() {
            // 无资源
        }

        public long printed() {
            return printed;
        }

        @Override
        public String operatorId() {
            return node.getId();
        }
    }

    /** 内存收集汇（测试 / 预览用）。 */
    public static final class CollectSink implements Operator {

        private final PipelineNode node;
        private final List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

        public CollectSink(PipelineNode node) {
            this.node = node;
        }

        @Override
        public void open(OperatorContext context) {
            // 收集汇无状态
        }

        @Override
        public void processElement(Record record, Output output) {
            rows.add(new LinkedHashMap<String, Object>(record.row()));
        }

        @Override
        public void processWatermark(long watermark) {
            // 收集汇不涉水位
        }

        @Override
        public void onTimer(long timestamp, String timerName) {
            // 收集汇不涉定时器
        }

        @Override
        public void close() {
            // 无资源
        }

        /** 收到的行（断言用）。 */
        public List<Map<String, Object>> rows() {
            return rows;
        }

        @Override
        public String operatorId() {
            return node.getId();
        }
    }
}
