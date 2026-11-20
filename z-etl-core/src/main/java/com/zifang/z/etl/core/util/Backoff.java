package com.zifang.z.etl.core.util;

/**
 * 重试退避助手（等待用）。
 *
 * @author zifang
 */
public final class Backoff {

    private Backoff() {
    }

    /**
     * 等待一段时间（毫秒）。
     *
     * <p>被中断时恢复中断标志并抛 {@link IllegalStateException}（不吞中断）。
     *
     * @param millis 等待毫秒
     */
    public static void sleep(long millis) {
        if (millis <= 0L) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("重试等待被中断", e);
        }
    }
}
