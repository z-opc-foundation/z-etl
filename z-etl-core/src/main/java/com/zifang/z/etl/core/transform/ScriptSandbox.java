package com.zifang.z.etl.core.transform;

import com.zifang.z.script.engine.sandbox.ElSandbox;
import com.zifang.z.script.engine.sandbox.GroovySandbox;
import com.zifang.z.script.engine.sandbox.SandboxExecutor;
import com.zifang.z.script.engine.sandbox.SandboxPolicy;
import com.zifang.z.script.engine.sandbox.ScriptTimeoutException;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * z-script 沙箱封装（Groovy / EL 两个执行器 + 墙钟超时）。
 *
 * <p>为什么引擎侧要再包一层：
 * <ul>
 *   <li>z-script 的 {@link SandboxExecutor} 是**按调用**传超时的（不是构造时定死），
 *       所以每条脚本规则可以有自己的时限。</li>
 *   <li>z-script 的超时异常（{@link ScriptTimeoutException}）消息里没有「是哪条规则」，
 *       引擎侧要补上步骤名才谈得上「失败信息可读」。</li>
 * </ul>
 *
 * <p>⚠️ 已知零件缺口：**z-script 侧没有内存配额**（TASK-20261012-010 实测结论），
 * 因此本仓兜的是「单步产出大小上限」（{@link ScriptSpec#getMaxOutputChars()}，在
 * {@link ScriptTransformer} 里判定），真正的 JVM 堆配额需 z-script 仓补。
 *
 * @author zifang
 */
public class ScriptSandbox implements AutoCloseable {

    /** 沙箱线程池大小（单线程 ETL 顺序执行，2 足够）。 */
    private final SandboxExecutor executor;

    /** Groovy 沙箱（编译期 AST 黑名单 + {@code @ThreadInterrupt}）。 */
    private final GroovySandbox groovy;

    /** EL 沙箱（Spring SpEL + {@code SimpleEvaluationContext}）。 */
    private final ElSandbox el;

    public ScriptSandbox() {
        this(SandboxPolicy.defaultPolicy());
    }

    public ScriptSandbox(SandboxPolicy policy) {
        this.executor = new SandboxExecutor(2);
        this.groovy = new GroovySandbox(policy);
        this.el = new ElSandbox();
    }

    /**
     * 执行 Groovy 脚本（沙箱内）。
     *
     * @param code      脚本文本
     * @param bindings  变量绑定（行内容）
     * @param timeoutMs 墙钟超时（毫秒）
     * @return 脚本返回值
     */
    public Object evalGroovy(final String code, final Map<String, Object> bindings, long timeoutMs) {
        return executor.execute(new Callable<Object>() {
            @Override
            public Object call() {
                return groovy.eval(code, bindings);
            }
        }, timeoutMs);
    }

    /**
     * 执行 EL（SpEL）表达式（沙箱内）。
     *
     * @param code      表达式文本
     * @param bindings  变量绑定（行内容）
     * @param timeoutMs 墙钟超时（毫秒）
     * @return 求值结果
     */
    public Object evalEl(final String code, final Map<String, Object> bindings, long timeoutMs) {
        return executor.execute(new Callable<Object>() {
            @Override
            public Object call() {
                return el.eval(code, bindings);
            }
        }, timeoutMs);
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
