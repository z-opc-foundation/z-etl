package com.zifang.z.etl.core.pipeline;

/**
 * 流水线边（上游节点 → 下游节点）。
 *
 * <p>口径照抄 dbt / Airflow 的教训：**能推导就别手写**。本仓的边优先级：
 * <ol>
 *   <li>显式 {@code edges} 声明（扇出 / 扇入 / join 的两条入边都靠它）；</li>
 *   <li>推导规则：{@code join} 节点的入边由 {@code config.left} / {@code config.right} 指名，
 *       {@code union} 节点的入边就是它的全部入边。</li>
 * </ol>
 *
 * @author zifang
 */
public class PipelineEdge {

    /** 上游节点 id。 */
    private String from;

    /** 下游节点 id。 */
    private String to;

    public PipelineEdge() {
    }

    public PipelineEdge(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return from + " -> " + to;
    }
}
