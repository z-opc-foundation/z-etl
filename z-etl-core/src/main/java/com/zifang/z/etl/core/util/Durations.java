package com.zifang.z.etl.core.util;

import java.util.concurrent.TimeUnit;

/**
 * 时长字面量解析。
 *
 * <p>口径：{@code 500ms} / {@code 30s} / {@code 5m} / {@code 2h} / {@code 1d}；纯数字按**毫秒**。
 * 作业描述里凡是「时间跨度」语义的字段（窗口大小、乱序容忍、回溯窗口、存活时间）都走这里，
 * 避免同一含义在不同算子里各写一套解析。
 *
 * @author zifang
 */
public final class Durations {

    private Durations() {
    }

    /**
     * 解析时长（毫秒）。
     *
     * @param text         时长字面量
     * @param defaultMillis 解析不出来时的默认值
     * @return 毫秒数
     */
    public static long parseMillis(String text, long defaultMillis) {
        if (text == null) {
            return defaultMillis;
        }
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return defaultMillis;
        }
        if (isDigits(trimmed)) {
            return Long.parseLong(trimmed);
        }
        int unitIndex = 0;
        while (unitIndex < trimmed.length() && !Character.isLetter(trimmed.charAt(unitIndex))) {
            unitIndex++;
        }
        if (unitIndex == 0) {
            return defaultMillis;
        }
        String number = trimmed.substring(0, unitIndex).trim();
        String unit = trimmed.substring(unitIndex).trim().toLowerCase();
        if (!isDigits(number)) {
            return defaultMillis;
        }
        long value = Long.parseLong(number);
        if ("ms".equals(unit)) {
            return value;
        }
        if ("s".equals(unit)) {
            return TimeUnit.SECONDS.toMillis(value);
        }
        if ("m".equals(unit)) {
            return TimeUnit.MINUTES.toMillis(value);
        }
        if ("h".equals(unit)) {
            return TimeUnit.HOURS.toMillis(value);
        }
        if ("d".equals(unit)) {
            return TimeUnit.DAYS.toMillis(value);
        }
        return defaultMillis;
    }

    /**
     * 解析时长（毫秒），解析不出来按 0。
     *
     * @param text 时长字面量
     * @return 毫秒数
     */
    public static long parseMillis(String text) {
        return parseMillis(text, 0L);
    }

    private static boolean isDigits(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (!Character.isDigit(text.charAt(i))) {
                return false;
            }
        }
        return !text.isEmpty();
    }
}
