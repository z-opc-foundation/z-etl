package com.zifang.z.etl.core.transform;

import com.zifang.z.etl.core.job.ExpressionSpec;
import com.zifang.util.expr.el.ElEvaluator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 行级表达式算子（z-util-expr 家族）。
 *
 * <p>两个引擎：
 * <ul>
 *   <li>{@code EL}（默认）：z-util-expr 的 {@link ElEvaluator}（自研 SpEL 子集，无任意代码执行）。
 *       ⚠️ 它的算术走 double ⇒ 金额用它会失真（{@code 100.00*1.13 = 112.99999999999999}），
 *       金额场景请用 {@code GROOVY} 引擎或后续的类型归一收口。</li>
 *   <li>{@code GROOVY}：**走 z-script 的 GroovySandbox**（不是 z-util-expr 的裸 GroovyExecutor ——
 *       那个没有沙箱，等于给作业描述开了 RCE），BigDecimal 算术精确，且有墙钟超时。</li>
 * </ul>
 *
 * <p>变量作用域 = 该步骤可见的行内容（列名 → 值）；产出列由 {@code name} 指定。
 *
 * @author zifang
 */
public class ExpressionTransformer implements RowTransformer {

    private final List<ExpressionSpec> specs;
    private final ScriptSandbox sandbox;

    public ExpressionTransformer(List<ExpressionSpec> specs, ScriptSandbox sandbox) {
        this.specs = specs;
        this.sandbox = sandbox;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<String, Object>(row);
        if (specs == null) {
            return result;
        }
        for (ExpressionSpec spec : specs) {
            if (spec == null) {
                continue;
            }
            String step = "expression:" + spec.getName();
            // 求值作用域 = 累积结果（后一条表达式可以用到前一条的产出列）
            Object value = evaluate(spec, result, step);
            result.put(spec.getName(), TypeNormalizer.coerce(value, spec.getType(), spec.getNullable()));
        }
        return result;
    }

    /** 求值：按引擎分派；失败一律抛 {@link TransformStepException}（不静默吞）。 */
    private Object evaluate(ExpressionSpec spec, Map<String, Object> row, String step) {
        String engine = spec.effectiveEngine();
        try {
            if (ExpressionSpec.ENGINE_EL.equals(engine)) {
                return new ElEvaluator().eval(spec.getExpression(), row);
            }
            if (ExpressionSpec.ENGINE_GROOVY.equals(engine)) {
                if (sandbox == null) {
                    throw new TransformStepException(step, "GROOVY 引擎需要沙箱执行器，但未配置");
                }
                return sandbox.evalGroovy(spec.getExpression(), row, spec.effectiveTimeoutMs());
            }
            throw new TransformStepException(step, "引擎不支持: " + spec.getEngine()
                    + "（LUA 无沙箱可逃逸、JS 模块无 JS 引擎 ⇒ 硬拒绝）");
        } catch (TransformStepException e) {
            throw e;
        } catch (com.zifang.z.script.engine.sandbox.ScriptTimeoutException e) {
            throw new TransformStepException(step,
                    "表达式执行超时(" + spec.effectiveTimeoutMs() + "ms)，已强制中断", e);
        } catch (RuntimeException e) {
            throw new TransformStepException(step, "求值失败: " + e.getMessage(), e);
        }
    }

    @Override
    public String stepName() {
        return "expression";
    }
}
