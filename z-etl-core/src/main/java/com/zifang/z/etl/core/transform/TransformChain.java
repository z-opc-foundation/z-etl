package com.zifang.z.etl.core.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * transform 链：按声明顺序串行执行各算子。
 *
 * @author zifang
 */
public class TransformChain implements RowTransformer {

    private final List<RowTransformer> operators = new ArrayList<RowTransformer>();

    public TransformChain() {
    }

    public TransformChain(List<RowTransformer> operators) {
        if (operators != null) {
            this.operators.addAll(operators);
        }
    }

    public TransformChain add(RowTransformer operator) {
        if (operator != null) {
            operators.add(operator);
        }
        return this;
    }

    public List<RowTransformer> operators() {
        return Collections.unmodifiableList(operators);
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> row) {
        Map<String, Object> current = row;
        for (RowTransformer operator : operators) {
            try {
                current = operator.transform(current);
            } catch (TransformStepException e) {
                throw e;
            } catch (RuntimeException e) {
                // 兜底：算子自己抛的异常一律补上「哪一步」再抛（不静默吞）
                throw new TransformStepException(operator.stepName(),
                        e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(), e);
            }
        }
        return current;
    }
}
