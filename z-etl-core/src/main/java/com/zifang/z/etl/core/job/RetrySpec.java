package com.zifang.z.etl.core.job;

import java.util.ArrayList;
import java.util.List;

/**
 * 失败重试策略（作业级 / 流水线级共用）。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code maxAttempts} = **总尝试次数**（含第一次）；默认 1 ⇒ 不重试（行为与从前一致）；</li>
 *   <li>{@code backoffMs} = 首次重试前的等待；之后按 {@code multiplier} 指数退避，
 *       封顶 {@code maxBackoffMs}（默认 60s）；</li>
 *   <li>重试是**整体重跑**：水位只在本页写成功后推进 ⇒ 重跑从上次成功页接力，不会重搬已落的行
 *       （目标侧是幂等 upsert，重搬也无副作用）。</li>
 * </ul>
 *
 * @author zifang
 */
public class RetrySpec {

    /** 总尝试次数（含第一次）；默认 1 = 不重试。 */
    private int maxAttempts = 1;

    /** 首次重试前的等待（毫秒）。 */
    private long backoffMs = 0L;

    /** 退避上限（毫秒）。 */
    private long maxBackoffMs = 60000L;

    /** 退避倍数。 */
    private double multiplier = 2.0d;

    /** 总尝试次数（≥1）。 */
    public int effectiveMaxAttempts() {
        return maxAttempts < 1 ? 1 : maxAttempts;
    }

    /**
     * 第 {@code attempt} 次失败后到下一次尝试前的等待（毫秒）。
     *
     * @param attempt 已失败的次数（1 起）
     * @return 等待毫秒
     */
    public long backoffFor(int attempt) {
        if (backoffMs <= 0L) {
            return 0L;
        }
        double value = backoffMs;
        for (int i = 1; i < attempt; i++) {
            value = value * (multiplier <= 0d ? 1d : multiplier);
            if (value >= maxBackoffMs) {
                return Math.max(0L, maxBackoffMs);
            }
        }
        return (long) Math.min(value, Math.max(0L, maxBackoffMs));
    }

    /** 校验（把问题列出来，交给校验器汇总）。 */
    public List<String> errors(String side) {
        List<String> errors = new ArrayList<String>();
        if (maxAttempts < 1) {
            errors.add(side + ".retry.maxAttempts 必须 ≥ 1");
        }
        if (backoffMs < 0L) {
            errors.add(side + ".retry.backoffMs 不能为负");
        }
        if (maxBackoffMs < 0L) {
            errors.add(side + ".retry.maxBackoffMs 不能为负");
        }
        if (multiplier < 0d) {
            errors.add(side + ".retry.multiplier 不能为负");
        }
        return errors;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffMs() {
        return backoffMs;
    }

    public void setBackoffMs(long backoffMs) {
        this.backoffMs = backoffMs;
    }

    public long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public void setMaxBackoffMs(long maxBackoffMs) {
        this.maxBackoffMs = maxBackoffMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }
}
