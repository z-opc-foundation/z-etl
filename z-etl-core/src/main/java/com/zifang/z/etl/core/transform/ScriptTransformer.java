package com.zifang.z.etl.core.transform;

import com.zifang.z.etl.core.job.ScriptSpec;
import com.zifang.z.script.engine.sandbox.ScriptTimeoutException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 脚本沙箱算子（z-script 执行复杂清洗）。
 *
 * <p>安全边界（三道闸）：
 * <ol>
 *   <li><b>沙箱</b>：Groovy 走编译期 AST 黑名单，EL 走 SpEL {@code SimpleEvaluationContext}；
 *       LUA 引擎**硬拒绝**（z-script 的 Lua 无沙箱无超时，实测可逃逸）。</li>
 *   <li><b>超时</b>：每条规则独立墙钟时限，超时 ⇒ 步骤失败 + job 失败（消息含步骤名与时限）。</li>
 *   <li><b>产出上限</b>：单步产出大小上限（{@code maxOutputChars}）——
 *       这是引擎侧兜的「内存越限」闸（z-script 无内存配额，属零件缺口）。</li>
 * </ol>
 *
 * @author zifang
 */
public class ScriptTransformer implements RowTransformer {

    private final List<ScriptSpec> specs;
    private final ScriptSandbox sandbox;

    public ScriptTransformer(List<ScriptSpec> specs, ScriptSandbox sandbox) {
        this.specs = specs;
        this.sandbox = sandbox;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<String, Object>(row);
        if (specs == null) {
            return result;
        }
        for (ScriptSpec spec : specs) {
            if (spec == null) {
                continue;
            }
            String step = "script:" + spec.getName();
            // 脚本作用域 = 累积结果（脚本可以用到表达式产出的列）
            Object value = evaluate(spec, result, step);
            result.put(spec.getName(), TypeNormalizer.coerce(value, spec.getType(), spec.getNullable()));
        }
        return result;
    }

    /** 执行脚本；失败一律抛 {@link TransformStepException}（不静默吞）。 */
    private Object evaluate(ScriptSpec spec, Map<String, Object> row, String step) {
        if (sandbox == null) {
            throw new TransformStepException(step, "脚本步骤需要沙箱执行器，但未配置");
        }
        String engine = spec.effectiveEngine();
        long timeoutMs = spec.effectiveTimeoutMs();
        Object value;
        try {
            if (ScriptSpec.ENGINE_GROOVY.equals(engine)) {
                value = sandbox.evalGroovy(spec.getScript(), row, timeoutMs);
            } else if (ScriptSpec.ENGINE_EL.equals(engine)) {
                value = sandbox.evalEl(spec.getScript(), row, timeoutMs);
            } else {
                throw new TransformStepException(step, "引擎不支持: " + spec.getEngine()
                        + "（LUA 执行器无沙箱无超时、实测可逃逸 ⇒ 硬拒绝）");
            }
        } catch (TransformStepException e) {
            throw e;
        } catch (ScriptTimeoutException e) {
            throw new TransformStepException(step,
                    "脚本执行超时(" + timeoutMs + "ms)，已强制中断", e);
        } catch (RuntimeException e) {
            throw new TransformStepException(step, "脚本执行失败: " + e.getMessage(), e);
        }
        int limit = spec.effectiveMaxOutputChars();
        int size = measure(value);
        if (size > limit) {
            throw new TransformStepException(step,
                    "脚本产出 " + size + " 字符，超过单步上限 " + limit + " 字符（内存越限兜底）");
        }
        return value;
    }

    /** 产出大小（按字符串长度；非字符串按其文本长度算）。 */
    private static int measure(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof CharSequence) {
            return ((CharSequence) value).length();
        }
        return String.valueOf(value).length();
    }

    @Override
    public String stepName() {
        return "script";
    }
}
