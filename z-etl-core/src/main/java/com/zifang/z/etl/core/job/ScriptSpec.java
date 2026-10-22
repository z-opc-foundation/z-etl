package com.zifang.z.etl.core.job;

/**
 * 脚本沙箱步骤（复杂清洗：整段脚本算一个值，产出新列）。
 *
 * <p>执行器口径（TASK-20261012-010 实跑结论）：
 * <ul>
 *   <li>{@code GROOVY}（默认）—— z-script {@code GroovySandbox}：编译期 AST 黑名单
 *       （禁 import / 禁危险接收者 / 禁危险方法名）+ 墙钟超时（{@code @ThreadInterrupt} 打断死循环）。</li>
 *   <li>{@code EL} —— z-script {@code ElSandbox}：Spring SpEL + {@code SimpleEvaluationContext}
 *       （禁类型引用 / 禁构造器 / 禁 Bean 引用），同样受墙钟超时约束。</li>
 *   <li>{@code LUA} —— **硬拒绝**：z-script 的 Lua 执行器**完全没有沙箱**，
 *       实测 {@code os.execute} 可逃逸、且无超时 ⇒ 不能作为清洗步骤的执行器。</li>
 *   <li>{@code SQL} —— 本仓不接（清洗脚本走表达式/脚本两种执行器即可）。</li>
 * </ul>
 *
 * <p>内存上限：z-script 侧**没有**内存配额能力（TASK-010 实测结论），
 * 因此本仓在**引擎侧**兜一道可判定的上限（{@link #maxOutputChars}：单步产出大小上限），
 * 真正的 JVM 堆配额属于零件缺口，需 z-script 仓补（见任务卡「人工执行清单」）。
 *
 * @author zifang
 */
public class ScriptSpec {

    /** GROOVY 引擎（z-script 沙箱，默认）。 */
    public static final String ENGINE_GROOVY = "GROOVY";
    /** EL 引擎（z-script SpEL 沙箱）。 */
    public static final String ENGINE_EL = "EL";

    /** 默认墙钟超时（毫秒）。 */
    public static final long DEFAULT_TIMEOUT_MS = 2000L;

    /** 默认单步产出大小上限（字符）。 */
    public static final int DEFAULT_MAX_OUTPUT_CHARS = 65536;

    /** 输出列名。 */
    private String name;

    /** 引擎：{@link #ENGINE_GROOVY}（默认）/ {@link #ENGINE_EL}。 */
    private String engine = ENGINE_GROOVY;

    /** 脚本原文。 */
    private String script;

    /** 墙钟超时（毫秒）；超时 ⇒ 步骤失败且 job 失败（不静默吞）。 */
    private Long timeoutMs = Long.valueOf(DEFAULT_TIMEOUT_MS);

    /** 单步产出大小上限（字符）：引擎侧兜的「内存越限」闸。 */
    private Integer maxOutputChars = Integer.valueOf(DEFAULT_MAX_OUTPUT_CHARS);

    /** 输出类型（可选）：声明后按该类型归一。 */
    private String type;

    /** 空值策略。 */
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

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public Integer getMaxOutputChars() {
        return maxOutputChars;
    }

    public void setMaxOutputChars(Integer maxOutputChars) {
        this.maxOutputChars = maxOutputChars;
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

    /** 归一后的引擎名（大写）。 */
    public String effectiveEngine() {
        return engine == null || engine.trim().isEmpty() ? ENGINE_GROOVY : engine.trim().toUpperCase();
    }

    /** 有效超时（毫秒）。 */
    public long effectiveTimeoutMs() {
        return timeoutMs == null || timeoutMs.longValue() <= 0L ? DEFAULT_TIMEOUT_MS : timeoutMs.longValue();
    }

    /** 有效产出上限（字符）。 */
    public int effectiveMaxOutputChars() {
        return maxOutputChars == null || maxOutputChars.intValue() <= 0
                ? DEFAULT_MAX_OUTPUT_CHARS : maxOutputChars.intValue();
    }
}
