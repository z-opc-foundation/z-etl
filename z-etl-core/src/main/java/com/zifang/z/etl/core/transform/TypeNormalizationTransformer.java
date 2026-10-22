package com.zifang.z.etl.core.transform;

import com.zifang.z.etl.core.job.TypeNormalizationSpec;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 类型归一算子（链尾收口）。
 *
 * <p>把指定列归一到声明类型，并可追加定点小数位（{@code scale}）与空值策略。
 * 放在链尾的意义：表达式/脚本产出（尤其是浮点语义的中间结果）在落库前统一校一次，
 * 金额列才能保证是定点值而不是 {@code 112.99999999999999}。
 *
 * @author zifang
 */
public class TypeNormalizationTransformer implements RowTransformer {

    private final List<TypeNormalizationSpec> specs;

    public TypeNormalizationTransformer(List<TypeNormalizationSpec> specs) {
        this.specs = specs;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<String, Object>(row);
        if (specs == null) {
            return result;
        }
        for (TypeNormalizationSpec spec : specs) {
            if (spec == null || spec.getColumn() == null) {
                continue;
            }
            String step = "type-normalization:" + spec.getColumn();
            Object raw = com.zifang.z.etl.core.util.RowValues.get(result, spec.getColumn());
            Object value = TypeNormalizer.coerce(raw, spec.getType(), spec.getNullable());
            if (value instanceof BigDecimal && spec.getScale() != null) {
                value = ((BigDecimal) value).setScale(spec.getScale().intValue(), RoundingMode.HALF_UP);
            }
            result.put(spec.getColumn(), value);
        }
        return result;
    }

    @Override
    public String stepName() {
        return "type-normalization";
    }
}
