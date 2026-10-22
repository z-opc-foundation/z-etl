package com.zifang.z.etl.core.transform;

/**
 * 转换步骤失败（**不静默吞**：消息里必须能看出「哪一步、什么原因」）。
 *
 * <p>失败信息三要素：步骤名（{@code script:clean_user} / {@code expression:tax} /
 * {@code type-normalization:amount}）、原因、约束值（超时时长 / 上限字节数等）。
 *
 * @author zifang
 */
public class TransformStepException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** 步骤名。 */
    private final String step;

    /** 失败原因（人读）。 */
    private final String reason;

    public TransformStepException(String step, String reason) {
        super("转换步骤 [" + step + "] 失败: " + reason);
        this.step = step;
        this.reason = reason;
    }

    public TransformStepException(String step, String reason, Throwable cause) {
        super("转换步骤 [" + step + "] 失败: " + reason, cause);
        this.step = step;
        this.reason = reason;
    }

    public String getStep() {
        return step;
    }

    public String getReason() {
        return reason;
    }
}
