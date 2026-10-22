package com.zifang.z.etl.core.transform;

import com.zifang.z.etl.core.job.FieldMapping;
import com.zifang.z.etl.core.job.TransformSpec;
import com.zifang.z.etl.core.util.RowValues;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 字段映射算子：源列 → 目标列 + 类型归一。
 *
 * <p>「取列 → 归一 → 写列」三件事一次做完，这是零件面没有、必须引擎层兜住的部分
 * （需求 008 notes.md §9.6.1 E1 / E8）。
 *
 * @author zifang
 */
public class FieldMappingTransformer implements RowTransformer {

    private final TransformSpec spec;

    public FieldMappingTransformer(TransformSpec spec) {
        this.spec = spec;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        if (spec == null || spec.getFieldMappings() == null || spec.getFieldMappings().isEmpty()) {
            // 没有映射声明 = 原样透传（源列即目标列）
            return row == null ? result : new LinkedHashMap<String, Object>(row);
        }
        for (FieldMapping mapping : spec.getFieldMappings()) {
            if (mapping == null || mapping.getSource() == null) {
                continue;
            }
            String target = mapping.effectiveTarget();
            Object raw = RowValues.get(row, mapping.getSource());
            Object value = TypeNormalizer.normalize(raw, mapping.getType());
            if (value == null && Boolean.FALSE.equals(mapping.getNullable())) {
                value = TypeNormalizer.defaultValue(mapping.getType());
            }
            result.put(target, value);
        }
        return result;
    }
}
