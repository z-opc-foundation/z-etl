package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

/**
 * 截取算子：**前 N 条放行**（SQL {@code LIMIT} 口径的流式版）。
 *
 * <p>与 {@code topn} 的分工：topn 是「排序 + 截断」（要收齐全流），limit 是**边流边截**——
 * 头 N 条一到就放完，后面的直接丢。适合「抽个头看看」「有界流限量搬运」。
 *
 * <pre>
 * - id: head100
 *   type: limit
 *   config:
 *     limit: 100     # 放行条数（必填，≥ 0）
 *     offset: 0      # 先跳过多少条（缺省 0；LIMIT offset, n 的口径）
 * </pre>
 *
 * @author zifang
 */
public class LimitOperator implements Operator {

    private final PipelineNode node;

    private long limit;
    private long offset;

    private long skipped;
    private long emitted;
    private OperatorContext context;

    public LimitOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        limit = node.longValue("limit", -1L);
        offset = node.longValue("offset", 0L);
        if (limit < 0L) {
            throw new IllegalArgumentException("limit 节点 " + node.getId() + " 必须声明 config.limit（≥ 0）");
        }
        if (offset < 0L) {
            throw new IllegalArgumentException("limit 节点 " + node.getId() + " 的 offset 不能为负");
        }
        skipped = 0L;
        emitted = 0L;
    }

    @Override
    public void processElement(Record record, Output output) {
        if (skipped < offset) {
            skipped++;
            context.count("offset-skipped");
            return;
        }
        if (emitted < limit) {
            emitted++;
            output.collect(record);
            return;
        }
        context.count("truncated");
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态，不碰水位
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
