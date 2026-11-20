package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.connector.AbstractPullSource;
import com.zifang.z.etl.core.job.AggregateSpec;
import com.zifang.z.etl.core.job.PushdownSpec;
import com.zifang.z.etl.core.job.SourceSpec;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.reader.JdbcRowReader;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.util.RowValues;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC 源算子（流式口径：一页一页拉，记录一条一条吐）。
 *
 * <p>三件事：
 * <ol>
 *   <li><b>取数</b>：复用 {@link JdbcRowReader}（分页游标 + SQL 下推 + 复合水位游标 + 回溯窗口），
 *       页级提交 ⇒ 断点续传的口径与旧引擎一致；</li>
 *   <li><b>事件时间</b>：从 {@code eventTimeColumn} 取事件时间（TIMESTAMP / LONG）；</li>
 *   <li><b>水位</b>：{@code maxEventTime − maxOutOfOrderness}（有界乱序水位）。</li>
 * </ol>
 *
 * <p>「没有事件时间的记录」照常往下发（{@link com.zifang.z.etl.core.runtime.Record#NO_EVENT_TIME}），
 * 窗口算子会把它们归进全局窗口（流结束才结算），不会凭空丢数据。
 *
 * <p>增量过滤在 **SQL 侧**做（游标条件拼进查询）⇒ 内存里不再二次过滤。
 *
 * @author zifang
 */
public class SourceOperator extends AbstractPullSource {

    private final DataSource dataSource;

    private JdbcRowReader reader;
    private List<Map<String, Object>> buffer;
    private int bufferIndex;

    /**
     * @param node        源节点
     * @param dataSource  源数据源
     */
    public SourceOperator(PipelineNode node, DataSource dataSource) {
        super(node);
        this.dataSource = dataSource;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        SourceSpec spec = new SourceSpec();
        spec.setDataSourceRef(node.string("dataSourceRef"));
        spec.setTable(node.string("table"));
        spec.setColumns(node.strings("columns"));
        spec.setWhere(node.string("where"));
        spec.setOrderBy(node.string("orderBy"));
        spec.setPageSize(node.integer("pageSize", 500));
        spec.setWatermarkColumn(node.string("eventTimeColumn"));
        spec.setPushdown(pushdown(node));

        Map<String, Object> resume = node.map("resume");
        String resumeColumn = MapOperator.text(resume.get("column"));
        String resumeType = MapOperator.text(resume.get("type"));
        String tiebreakColumn = MapOperator.text(resume.get("tiebreakColumn"));
        long overlap = MapOperator.asLong(resume.get("overlap"));

        reader = new JdbcRowReader(dataSource, spec,
                resumeColumn, resumeValue, resumeType == null ? "LONG" : resumeType,
                tiebreakColumn, resumeTiebreak, overlap);
        buffer = new ArrayList<Map<String, Object>>();
        bufferIndex = 0;
    }

    /**
     * 节点的 {@code pushdown} 配置 → {@link PushdownSpec}（过滤 / 分组 / 聚合下推）。
     *
     * @param node 源节点
     * @return 下推声明；没声明返回 {@code null}
     */
    private static PushdownSpec pushdown(PipelineNode node) {
        Map<String, Object> config = node.map("pushdown");
        if (config.isEmpty()) {
            return null;
        }
        PushdownSpec pushdown = new PushdownSpec();
        pushdown.setFilter(MapOperator.text(config.get("filter")));
        List<String> groupBy = new ArrayList<String>();
        Object groupByRaw = config.get("groupBy");
        if (groupByRaw instanceof List) {
            for (Object item : (List<?>) groupByRaw) {
                String text = MapOperator.text(item);
                if (text != null) {
                    groupBy.add(text);
                }
            }
        }
        pushdown.setGroupBy(groupBy);
        List<AggregateSpec> aggregates = new ArrayList<AggregateSpec>();
        Object aggregatesRaw = config.get("aggregates");
        if (aggregatesRaw instanceof List) {
            for (Object raw : (List<?>) aggregatesRaw) {
                if (!(raw instanceof Map)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> item = (Map<String, Object>) raw;
                AggregateSpec aggregate = new AggregateSpec();
                String fn = MapOperator.text(item.get("fn"));
                aggregate.setFunction(fn == null ? MapOperator.text(item.get("function")) : fn);
                aggregate.setColumn(MapOperator.text(item.get("column")));
                aggregate.setAlias(MapOperator.text(item.get("alias")));
                aggregates.add(aggregate);
            }
        }
        pushdown.setAggregates(aggregates);
        Object distinct = config.get("distinct");
        if (distinct != null) {
            pushdown.setDistinct(Boolean.valueOf(String.valueOf(distinct)));
        }
        return pushdown;
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        while (bufferIndex >= buffer.size()) {
            List<Map<String, Object>> page = reader.fetchNextPage();
            if (page.isEmpty()) {
                return null;
            }
            buffer = page;
            bufferIndex = 0;
        }
        return buffer.get(bufferIndex++);
    }

    /** 增量过滤已在 SQL 侧完成 ⇒ 内存里不再判一次。 */
    @Override
    protected boolean passesResume(Map<String, Object> row) {
        return true;
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.closePull();
        }
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
