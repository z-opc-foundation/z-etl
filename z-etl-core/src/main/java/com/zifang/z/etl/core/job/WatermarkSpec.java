package com.zifang.z.etl.core.job;

/**
 * 水位（增量位点）定义。
 *
 * <p>水位是**引擎层**的事（需求 008 notes.md §9.6.1 E2）：五个零件面（z-util-jdbc / expr / office /
 * InMemoryTables / z-script）没有一个有位点概念，{@code maxRows} 只是静默截断不是水位。
 *
 * @author zifang
 */
public class WatermarkSpec {

    /** 水位列（源侧列名）。 */
    private String column;

    /** 水位类型：{@code LONG}（自增 id 游标）/ {@code TIMESTAMP}（update_time 游标）。 */
    private String type = "LONG";

    /** 水位列在目标侧的同名列（可选；给了则每行都带上，便于目标侧自证增量区间）。 */
    private String targetColumn;

    /**
     * 平局列（同秒多多的第二键；可空）。
     *
     * <p>时间戳游标是「同秒多条」的经典坑：过滤条件是 {@code 水位列 > 已存水位}，
     * 同一秒写进的多行里，只有时间戳**严格大于**水位的那部分会被搬走，
     * 时间戳**等于**水位的那些行会被永久漏掉。配了平局列（通常是自增主键）后，
     * 游标变成「水位值 + 平局值」的复合游标，同秒的行按平局列依次接力，不漏行。
     */
    private String tiebreakColumn;

    /**
     * 回溯窗口（默认 0 = 关闭）。
     *
     * <p>TIMESTAMP 水位时单位是**秒**；LONG 水位时单位是**行数偏移**。
     * 大于 0 时增量过滤改成「水位值 − 窗口」的闭区间下界（{@code >=}），
     * 用来兜住**时钟回拨 / 迟到数据**：源库时钟往回拨导致新行的时间戳早于当前水位时，
     * 纯时间游标会漏掉它们，回溯窗口能让它们被重新读到（目标侧 upsert 幂等 ⇒ 重复读不脏数据）。
     * 代价：窗口内的行每轮会被重读一遍。
     */
    private long overlap;

    /**
     * 游标越界时的策略（源表被清 / 水位跑到源数据前面）：{@code FAIL}（默认，明确失败不推进）
     * / {@code RESET}（清掉水位 ⇒ 下一轮退回全量重跑）。
     */
    private String onOutOfRange = "FAIL";

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }

    public String getTiebreakColumn() {
        return tiebreakColumn;
    }

    public void setTiebreakColumn(String tiebreakColumn) {
        this.tiebreakColumn = tiebreakColumn;
    }

    public long getOverlap() {
        return overlap;
    }

    public void setOverlap(long overlap) {
        this.overlap = overlap;
    }

    public String getOnOutOfRange() {
        return onOutOfRange;
    }

    public void setOnOutOfRange(String onOutOfRange) {
        this.onOutOfRange = onOutOfRange;
    }

    /** 越界策略归一：只认 {@code RESET}，其余一律按 {@code FAIL}。 */
    public String effectiveOnOutOfRange() {
        return "RESET".equalsIgnoreCase(onOutOfRange == null ? "" : onOutOfRange.trim()) ? "RESET" : "FAIL";
    }
}
