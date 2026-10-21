package com.zifang.z.etl.core.job;

/**
 * 字段映射（源列 → 目标列 + 目标类型）。
 *
 * <p>类型归一规则是**引擎层**的事（需求 008 notes.md §9.6.1 E1）：三个源面给出三套完全不同的类型语义
 * （JDBC 给原生类型、Excel 给全 String、内存 SQL 给浮点），必须逐条声明式落规则。
 *
 * @author zifang
 */
public class FieldMapping {

    /** 源列名。 */
    private String source;

    /** 目标列名；为空时默认与 {@link #source} 同名。 */
    private String target;

    /** 目标类型：{@code LONG / INT / STRING / BIGDECIMAL / DOUBLE / BOOLEAN / TIMESTAMP / DATE}；为空表示原样透传。 */
    private String type;

    /** 为空时是否保留 null（默认保留；{@code false} 表示 null 落成类型默认值）。 */
    private Boolean nullable = Boolean.TRUE;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
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

    /** 目标列名：显式给了用显式的，没给就用源列名。 */
    public String effectiveTarget() {
        return (target == null || target.trim().isEmpty()) ? source : target;
    }
}
