package com.zifang.z.etl.core.job;

/**
 * job 运行模式。
 *
 * <p>口径来自需求 008 「两段模式：初始化模式（首跑全量）→ 自动切增量模式（水位接力）」。
 *
 * <ul>
 *   <li>{@link #AUTO} —— 首跑按全量（INIT）执行，落水位后**自动切增量**；这是默认口径。</li>
 *   <li>{@link #INIT} —— 强制全量重跑（用于「重跑幂等」验证：全量重跑不重不漏）。</li>
 *   <li>{@link #INCREMENTAL} —— 强制增量；无水位时视为配置错误直接失败，不静默退化成全量。</li>
 * </ul>
 *
 * @author zifang
 */
public enum JobMode {

    AUTO("auto"),
    INIT("init"),
    INCREMENTAL("incremental");

    private final String id;

    JobMode(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    /**
     * 按字面量解析，容忍大小写与空白；解析不出来返回 {@code null}（由校验器报错，不在此处抛）。
     */
    public static JobMode fromId(String raw) {
        if (raw == null) {
            return null;
        }
        String key = raw.trim().toLowerCase();
        for (JobMode mode : values()) {
            if (mode.id.equals(key)) {
                return mode;
            }
        }
        return null;
    }
}
