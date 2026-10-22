package com.zifang.z.etl.core.job;

/**
 * 行级表达式规则（一行算一个值，产出新列）。
 *
 * <p>引擎口径（TASK-20261012-010 的实跑结论 + 本仓安全底线）：
 * <ul>
 *   <li>{@code EL}（默认）—— z-util-expr 的自研 EL（SpEL 子集）：**无任意代码执行**，
 *       算术走 double（金额用它会失真，见 {@code scale} 收口）。</li>
 *   <li>{@code GROOVY} —— 走 **z-script 的 GroovySandbox**（编译期 AST 黑名单 + 线程超时），
 *       BigDecimal 算术精确。刻意**不**用 z-util-expr 的裸 {@code GroovyExecutor}
 *       （无沙箱 ⇒ 等于给了 RCE）。</li>
 *   <li>{@code LUA} —— **硬拒绝**：z-util-expr 的 Lua 走 Luaj + {@code JsePlatform}，
 *       含 {@code os.execute} / {@code luajava.bindClass} 逃逸面，无沙箱无超时。</li>
 *   <li>{@code JS} —— **硬拒绝**：z-util-expr-js 模块里没有 JS 引擎（只有 SpEL/脚本引擎门面）。</li>
 * </ul>
 *
 * @author zifang
 */
public class ExpressionSpec {

    /** EL 引擎（z-util-expr，默认）。 */
    public static final String ENGINE_EL = "EL";
    /** GROOVY 引擎（z-script 沙箱）。 */
    public static final String ENGINE_GROOVY = "GROOVY";

    /** 输出列名。 */
    private String name;

    /** 引擎：{@link #ENGINE_EL}（默认）/ {@link #ENGINE_GROOVY}。 */
    private String engine = ENGINE_EL;

    /** 表达式原文。 */
    private String expression;

    /** 输出类型（可选）：声明后按该类型归一。 */
    private String type;

    /** 墙钟超时（毫秒）；仅 {@code GROOVY} 引擎用得上（EL 是纯解析求值，无外部调用）。 */
    private Long timeoutMs = Long.valueOf(2000L);

    /** 空值策略：{@code true} 保留 null；{@code false} 时 null 落类型默认值。 */
    private Boolean nullable = Boolean.TRUE;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    /** 有效超时（毫秒）。 */
    public long effectiveTimeoutMs() {
        return timeoutMs == null || timeoutMs.longValue() <= 0L ? 2000L : timeoutMs.longValue();
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    /** 归一后的引擎名（大写）。 */
    public String effectiveEngine() {
        return engine == null || engine.trim().isEmpty() ? ENGINE_EL : engine.trim().toUpperCase();
    }
}
