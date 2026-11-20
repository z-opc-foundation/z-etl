package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.checkpoint.WatermarkCursor;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.PullSource;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.runtime.WatermarkGenerator;
import com.zifang.z.etl.core.util.Durations;
import com.zifang.z.etl.core.util.RowValues;

import java.sql.Timestamp;
import java.util.Map;

/**
 * 拉取式源的公共骨架：事件时间、水位、增量游标三件事统一收口。
 *
 * <p>子类只管「怎么取下一行原始数据」（{@link #doNext()}），
 * 事件时间提取、有界乱序水位、复合游标过滤（{@code (w > ?) OR (w = ? AND tb > ?)}）都在这里。
 *
 * <p>游标口径与 JDBC 源**完全一致**（见 {@link WatermarkCursor}）：
 * 水位相等时要比平局列，否则同秒多行会被漏掉。
 *
 * @author zifang
 */
public abstract class AbstractPullSource implements PullSource {

    /** 偏移量游标的保留列名（文件 / 消息这类没有天然事件时间的源用它做增量）。 */
    public static final String OFFSET_COLUMN = "__offset__";

    /** 源节点声明。 */
    protected final PipelineNode node;

    private String eventTimeColumn;
    private String eventTimeType = "TIMESTAMP";
    private String tiebreakColumn;
    private WatermarkGenerator.BoundedOutOfOrderness watermarks;

    private String resumeColumn;
    private String resumeType = "LONG";
    private String resumeTiebreakColumn;
    protected Object resumeValue;
    protected Object resumeTiebreak;

    private Object lastEventTimeValue;
    private Object lastTiebreakValue;
    private long emitted;
    private long rowIndex;

    protected AbstractPullSource(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        eventTimeColumn = node.string("eventTimeColumn");
        eventTimeType = node.string("eventTimeType", "TIMESTAMP").toUpperCase();
        tiebreakColumn = node.string("tiebreakColumn");
        long maxOutOfOrderness = Durations.parseMillis(node.string("maxOutOfOrderness"), 0L);
        watermarks = new WatermarkGenerator.BoundedOutOfOrderness(maxOutOfOrderness);

        Map<String, Object> resume = node.map("resume");
        resumeColumn = Config.text(resume.get("column"));
        String type = Config.text(resume.get("type"));
        resumeType = type == null ? "LONG" : type.toUpperCase();
        resumeTiebreakColumn = Config.text(resume.get("tiebreakColumn"));
        // 平局列：节点级与 resume 级等价（声明任一处即可）
        tiebreakColumn = node.string("tiebreakColumn");
        if (tiebreakColumn == null) {
            tiebreakColumn = resumeTiebreakColumn;
        }
        if (resumeTiebreakColumn == null) {
            resumeTiebreakColumn = tiebreakColumn;
        }
        // 没声明 resume 但声明了事件时间列 ⇒ 默认按事件时间做增量（与 JDBC 源同口径的直觉）
        if (resumeColumn == null && eventTimeColumn != null) {
            resumeColumn = eventTimeColumn;
            resumeType = eventTimeType;
        }
        rowIndex = 0L;
    }

    @Override
    public final Record next() throws Exception {
        while (true) {
            Map<String, Object> row = doNext();
            if (row == null) {
                return null;
            }
            rowIndex++;
            if (!passesResume(row)) {
                continue;
            }
            emitted++;
            long eventTime = extractEventTime(row);
            if (eventTime != Record.NO_EVENT_TIME) {
                watermarks.onEvent(eventTime);
                Object raw = row.get(eventTimeColumn);
                lastEventTimeValue = raw == null ? RowValues.get(row, eventTimeColumn) : raw;
                if (tiebreakColumn != null) {
                    lastTiebreakValue = RowValues.get(row, tiebreakColumn);
                }
            }
            return new Record(row, eventTime, System.currentTimeMillis(), null);
        }
    }

    /**
     * 取下一行原始数据。
     *
     * @return 一行；到头返回 {@code null}
     * @throws Exception 读取失败
     */
    protected abstract Map<String, Object> doNext() throws Exception;

    /**
     * 增量过滤：这一行是否在游标之后。
     *
     * <p>默认口径与 JDBC 源一致：{@code (w > ?) OR (w = ? AND tb > ?)}。
     */
    protected boolean passesResume(Map<String, Object> row) {
        if (resumeValue == null || resumeColumn == null) {
            return true;
        }
        if (OFFSET_COLUMN.equals(resumeColumn)) {
            long from;
            try {
                from = WatermarkCursor.toLong(resumeValue);
            } catch (NumberFormatException e) {
                return true;
            }
            return rowIndex > from;
        }
        Object value = row.get(resumeColumn);
        if (value == null) {
            value = RowValues.get(row, resumeColumn);
        }
        if (value == null) {
            return true;
        }
        String formatted = WatermarkCursor.format(value, resumeType);
        int cmp = WatermarkCursor.compareValues(formatted, String.valueOf(resumeValue), resumeType);
        if (cmp != 0) {
            return cmp > 0;
        }
        if (resumeTiebreakColumn != null && resumeTiebreak != null) {
            Object tiebreak = row.get(resumeTiebreakColumn);
            if (tiebreak == null) {
                tiebreak = RowValues.get(row, resumeTiebreakColumn);
            }
            return WatermarkCursor.compareScalar(tiebreak == null ? null : String.valueOf(tiebreak),
                    String.valueOf(resumeTiebreak)) > 0;
        }
        return false;
    }

    @Override
    public long currentWatermark() {
        return watermarks == null ? Long.MIN_VALUE : watermarks.currentWatermark();
    }

    @Override
    public Object lastEventTimeValue() {
        return lastEventTimeValue;
    }

    @Override
    public Object lastTiebreakValue() {
        return lastTiebreakValue;
    }

    @Override
    public String eventTimeColumn() {
        return eventTimeColumn;
    }

    @Override
    public String eventTimeType() {
        return eventTimeType;
    }

    @Override
    public String tiebreakColumn() {
        return tiebreakColumn;
    }

    @Override
    public void setResume(Object value, Object tiebreak) {
        this.resumeValue = value;
        this.resumeTiebreak = tiebreak;
    }

    @Override
    public String checkpointColumn() {
        if (OFFSET_COLUMN.equals(resumeColumn)) {
            return OFFSET_COLUMN;
        }
        return eventTimeColumn;
    }

    @Override
    public String checkpointType() {
        if (OFFSET_COLUMN.equals(resumeColumn)) {
            return "LONG";
        }
        return eventTimeType;
    }

    @Override
    public Object checkpointValue() {
        if (OFFSET_COLUMN.equals(resumeColumn)) {
            return Long.valueOf(rowIndex);
        }
        return lastEventTimeValue;
    }

    /** 已吐出的记录数。 */
    public long emitted() {
        return emitted;
    }

    /** 已读到的行序号（含被游标过滤掉的行）。 */
    public long rowIndex() {
        return rowIndex;
    }

    /** 游标列（断点续传的第一键）。 */
    protected String resumeColumn() {
        return resumeColumn;
    }

    /** 游标类型。 */
    protected String resumeType() {
        return resumeType;
    }

    /** 游标平局列。 */
    protected String resumeTiebreakColumn() {
        return resumeTiebreakColumn;
    }

    /** 从行里取事件时间（毫秒）；取不到返回 {@link Record#NO_EVENT_TIME}。 */
    protected long extractEventTime(Map<String, Object> row) {
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
    public void processElement(Record record, com.zifang.z.etl.core.runtime.Output output) {
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
        // 无资源（子类覆写）
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
