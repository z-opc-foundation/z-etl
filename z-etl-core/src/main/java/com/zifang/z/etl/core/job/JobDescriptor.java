package com.zifang.z.etl.core.job;

import java.util.ArrayList;
import java.util.List;

/**
 * job 描述（本仓的**唯一**作业契约）。
 *
 * <p>形态：JSON 或 yml（DataX 式），解析后落到本对象；落库时存 canonical JSON + 原始 yml 两份文本。
 *
 * <pre>
 * job:
 *   name: trade-order-to-snapshot
 *   mode: auto
 *   source:
 *     dataSourceRef: ds-oc-main
 *     table: z_trade_order
 *     columns: [id, order_no, amount, updated_at]
 *     watermarkColumn: updated_at
 *   transform:
 *     fieldMappings:
 *       - {source: id,       target: id,       type: LONG}
 *       - {source: order_no, target: order_no, type: STRING}
 *   target:
 *     dataSourceRef: ds-etl-landing
 *     table: etl_trade_order_snapshot
 *     batchSize: 200
 *   idempotencyKey: [id]
 *   watermark:
 *     column: updated_at
 *     type: TIMESTAMP
 * </pre>
 *
 * @author zifang
 */
public class JobDescriptor {

    /** 作业名（全局唯一，落库主键）。 */
    private String name;

    /** 作业描述（人读）。 */
    private String description;

    /** 运行模式字面量：{@code auto} / {@code init} / {@code incremental}。 */
    private String mode = JobMode.AUTO.id();

    private SourceSpec source;

    private TransformSpec transform = new TransformSpec();

    private TargetSpec target;

    /** 幂等键（目标列名列表）：重跑靠它 upsert，保证不重不漏。 */
    private List<String> idempotencyKey = new ArrayList<String>();

    /** 水位定义；为空表示该 job 无增量语义（只能全量跑）。 */
    private WatermarkSpec watermark;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public SourceSpec getSource() {
        return source;
    }

    public void setSource(SourceSpec source) {
        this.source = source;
    }

    public TransformSpec getTransform() {
        return transform;
    }

    public void setTransform(TransformSpec transform) {
        this.transform = transform == null ? new TransformSpec() : transform;
    }

    public TargetSpec getTarget() {
        return target;
    }

    public void setTarget(TargetSpec target) {
        this.target = target;
    }

    public List<String> getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(List<String> idempotencyKey) {
        this.idempotencyKey = idempotencyKey == null ? new ArrayList<String>() : idempotencyKey;
    }

    public WatermarkSpec getWatermark() {
        return watermark;
    }

    public void setWatermark(WatermarkSpec watermark) {
        this.watermark = watermark;
    }

    /** 模式字面量解析结果；解析不出来返回 {@code null}（校验器负责报错）。 */
    public JobMode modeEnum() {
        return JobMode.fromId(mode);
    }

    /** 是否声明了增量语义（有水位列）。 */
    public boolean hasWatermark() {
        return watermark != null && watermark.getColumn() != null && !watermark.getColumn().trim().isEmpty();
    }
}
