package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.reader.JdbcRowReader;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.WatermarkGenerator;
import com.zifang.z.etl.core.util.Durations;
import com.zifang.z.etl.core.util.RowValues;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC 源算子（流式口径：一页一页拉，记录一条一条吐）。
 *
 * <p>三件事：
 * <ol>
 *   <li><b>取数</b>：复用 {@link JdbcRowReader}（分页游标 + SQL 下推 + 复合水位游标 + 回溯窗口），
 *       页级提交 ⇒ 断点续传的口径与旧引擎一致。</li>
 *   <li><b>事件时间</b>：从 {@code eventTimeColumn} 取事件时间（TIMESTAMP / LONG）。</li>
 *   <li><b>水位</b>：{@code maxEventTime − maxOutOfOrderness}（有界乱序水位）。</li>
 * </ol>
 *
 * <p>「没有事件时间的记录」照常往下发（{@link Record#NO_EVENT_TIME}），
 * 窗口算子会把它们归进全局窗口（流结束才结算），不会凭空丢数据。
 *
 * @author zifang
 */
public class SourceOperator implements Operator {

    private final PipelineNode node;
    private final DataSource dataSource;

    private JdbcRowReader reader;
    private List<Map<String, Object>> buffer;
    private int bufferIndex;

    private String eventTimeColumn;
    private String eventTimeType = "TIMESTAMP";
    private WatermarkGenerator.BoundedOutOfOrderness watermarks;

    private Object resumeValue;
    private Object resumeTiebreak;
    private String tiebreakColumn;

    private long emitted;
    private Object lastEventTimeValue;
    private Object lastTiebreakValue;

    /**
     * @param node        源节点
     * @param dataSource  源数据源
     */
    public SourceOperator(PipelineNode node, DataSource dataSource) {
        this.node = node;
        this.dataSource = dataSource;
    }

    /** 设定断点续传的游标值（增量接力用；首跑传 null）。 */
    public void setResume(Object value, Object tiebreak) {
        this.resumeValue = value;
        this.resumeTiebreak = tiebreak;
    }

    @Override
    public void open(OperatorContext context) {
        SourceSpec spec = new SourceSpec();
        spec.setDataSourceRef(node.string("dataSourceRef"));
        spec.setTable(node.string("table"));
        spec.setColumns(node.strings("columns"));
        spec.setWhere(node.string("where"));
        spec.setOrderBy(node.string("orderBy"));
        spec.setPageSize(node.integer("pageSize", 500));
        spec.setWatermarkColumn(node.string("eventTimeColumn"));

        eventTimeColumn = node.string("eventTimeColumn");
        eventTimeType = node.string("eventTimeType", "TIMESTAMP").toUpperCase();
        long maxOutOfOrderness = Durations.parseMillis(node.string("maxOutOfOrderness"), 0L);
        watermarks = new WatermarkGenerator.BoundedOutOfOrderness(maxOutOfOrderness);

        Map<String, Object> resume = node.map("resume");
        String resumeColumn = MapOperator.text(resume.get("column"));
        String resumeType = MapOperator.text(resume.get("type"));
        tiebreakColumn = MapOperator.text(resume.get("tiebreakColumn"));
        long overlap = MapOperator.asLong(resume.get("overlap"));

        reader = new JdbcRowReader(dataSource, spec,
                resumeColumn, resumeValue, resumeType == null ? "LONG" : resumeType,
                tiebreakColumn, resumeTiebreak, overlap);
        buffer = new java.util.ArrayList<Map<String, Object>>();
        bufferIndex = 0;
    }

    /**
     * 取下一条记录。
     *
     * @return 记录；流已到头返回 {@code null}
     * @throws Exception 读取失败
     */
    public Record next() throws Exception {
        while (bufferIndex >= buffer.size()) {
            List<Map<String, Object>> page = reader.fetchNextPage();
            if (page.isEmpty()) {
                return null;
            }
            buffer = page;
            bufferIndex = 0;
        }
        Map<String, Object> row = buffer.get(bufferIndex++);
        emitted++;
        long eventTime = extractEventTime(row);
        if (eventTime != Record.NO_EVENT_TIME) {
            watermarks.onEvent(eventTime);
            lastEventTimeValue = row.get(eventTimeColumn);
            if (lastEventTimeValue == null) {
                lastEventTimeValue = RowValues.get(row, eventTimeColumn);
            }
            if (tiebreakColumn != null) {
                lastTiebreakValue = RowValues.get(row, tiebreakColumn);
            }
        }
        return new Record(row, eventTime, System.currentTimeMillis(), null);
    }

    /** 当前水位。 */
    public long currentWatermark() {
        return watermarks.currentWatermark();
    }

    /** 已吐出的记录数。 */
    public long emitted() {
        return emitted;
    }

    /** 最后一个带事件时间的原始值（落水位用）。 */
    public Object lastEventTimeValue() {
        return lastEventTimeValue;
    }

    /** 最后一个平局列原始值（落水位用）。 */
    public Object lastTiebreakValue() {
        return lastTiebreakValue;
    }

    /** 事件时间列。 */
    public String eventTimeColumn() {
        return eventTimeColumn;
    }

    /** 事件时间类型。 */
    public String eventTimeType() {
        return eventTimeType;
    }

    /** 平局列（断点续传的第二键；未声明为 {@code null}）。 */
    public String tiebreakColumn() {
        return tiebreakColumn;
    }

    /** 从行里取事件时间（毫秒）；取不到返回 {@link Record#NO_EVENT_TIME}。 */
    private long extractEventTime(Map<String, Object> row) {
        if (eventTimeColumn == null) {
            return Record.NO_EVENT_TIME;
        }
        Object value = RowValues.get(row, eventTimeColumn);
        if (value == null) {
            return Record.NO_EVENT_TIME;
        }
        if ("LONG".equals(eventTimeType)) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            try {
                return Long.parseLong(String.valueOf(value).trim());
            } catch (NumberFormatException e) {
                return Record.NO_EVENT_TIME;
            }
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
        }
        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime();
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Timestamp.valueOf(String.valueOf(value).trim()).getTime();
        } catch (IllegalArgumentException e) {
            return Record.NO_EVENT_TIME;
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        throw new UnsupportedOperationException("源算子不接受推送，只能拉取（next）");
    }

    @Override
    public void processWatermark(long watermark) {
        // 源自己产水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 源不用定时器
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.closePull();
        }
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 源节点引用的表（取证用）。 */
    public String table() {
        return node.string("table");
    }

    /** 构造一个空行（调试用）。 */
    static Map<String, Object> emptyRow() {
        return new LinkedHashMap<String, Object>();
    }
}
