package com.zifang.z.etl.core.job;

import java.util.ArrayList;
import java.util.List;

/**
 * transform 链定义。
 *
 * <p>本轮（M1 骨架）落地的变换算子只有一种：{@link FieldMapping}（字段映射 + 类型归一）。
 * D4 要求的行级表达式（z-util-expr）与脚本沙箱（z-script）由 TASK-20261012-015 追加，
 * 届时以 {@code expression} / {@code script} 两种新算子形态挂进同一条链，链的骨架不变。
 *
 * @author zifang
 */
public class TransformSpec {

    private List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();

    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings == null ? new ArrayList<FieldMapping>() : fieldMappings;
    }
}
