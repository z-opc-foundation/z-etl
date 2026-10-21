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
}
