package com.zifang.z.etl.core.job;

/**
 * 类型归一规则（独立算子：把某一列归一到声明的类型）。
 *
 * <p>为什么要有独立算子：字段映射里的 {@code type} 是「这一列搬过去时按什么类型落」，
 * 而类型归一算子是**收口**用的 —— 它可以在映射 / 表达式 / 脚本产出之后再统一校一次，
 * 并且能额外指定定点小数位（{@link #scale}）与空值策略（{@link #nullable}）。
 *
 * <p>金额场景必用 {@code BIGDECIMAL} + {@code scale}：JDBC 给的 {@code DECIMAL(18,2)} 读出来是
 * {@code BigDecimal}，但经过表达式（浮点语义）后小数位会漂，收口一步才能保证落库是定点值。
 *
 * @author zifang
 */
public class TypeNormalizationSpec {

    /** 要归一的列名（链上该步骤可见的列名）。 */
    private String column;

    /** 目标类型：{@link TypeNames} 全集之一。 */
    private String type;

    /** 空值策略：{@code true} 保留 null；{@code false} 时 null 落类型默认值。 */
    private Boolean nullable = Boolean.TRUE;

    /** 定点小数位（仅 {@code BIGDECIMAL} 有效）：按 {@code HALF_UP} 舍入到该位。 */
    private Integer scale;

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

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }
}
