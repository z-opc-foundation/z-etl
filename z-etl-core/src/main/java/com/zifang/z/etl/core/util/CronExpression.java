package com.zifang.z.etl.core.util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * 最小 cron 表达式（5 段：分 时 日 月 周）。
 *
 * <p>支持：{@code *}、{@code *}{@code /n}、{@code a-b}、{@code a-b}{@code /n}、{@code a,b,c}、单值。
 * 语义照 cron 惯例：**日**与**周**都写了且都不是 {@code *} 时，两者取**并**（任一命中即跑）。
 *
 * <p>用途：给「调度登记」算出**下一次触发时间**（可观测）——真正的定时器由宿主驱动
 * （z-ops 定时任务 / k8s CronJob 调 REST 触发口），本仓不起常驻线程。
 *
 * @author zifang
 */
public final class CronExpression {

    private final String expression;
    private final boolean[] minutes = new boolean[60];
    private final boolean[] hours = new boolean[24];
    private final boolean[] daysOfMonth = new boolean[32];
    private final boolean[] months = new boolean[13];
    private final boolean[] daysOfWeek = new boolean[7];
    private final boolean everyDayOfMonth;
    private final boolean everyDayOfWeek;

    private CronExpression(String expression) {
        this.expression = expression;
        String[] parts = expression.trim().split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException("cron 表达式必须是 5 段（分 时 日 月 周），实际: " + expression);
        }
        parse(parts[0], minutes, 0, 59, "分");
        parse(parts[1], hours, 0, 23, "时");
        parse(parts[2], daysOfMonth, 1, 31, "日");
        parse(parts[3], months, 1, 12, "月");
        parse(parts[4], daysOfWeek, 0, 6, "周");
        everyDayOfMonth = "*".equals(parts[2]);
        everyDayOfWeek = "*".equals(parts[4]);
    }

    /**
     * 解析 cron 表达式。
     *
     * @param expression 表达式文本
     * @return 表达式对象
     */
    public static CronExpression parse(String expression) {
        if (expression == null || expression.trim().isEmpty()) {
            throw new IllegalArgumentException("cron 表达式不能为空");
        }
        return new CronExpression(expression);
    }

    /** 表达式原文。 */
    public String expression() {
        return expression;
    }

    /**
     * 下一次触发时间（毫秒时间戳；找不到返回 {@code -1}）。
     *
     * @param afterMillis 起点（毫秒）
     * @return 下一次触发时间（毫秒）
     */
    public long nextFireTimeAfter(long afterMillis) {
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
        calendar.setTimeInMillis(afterMillis);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.MINUTE, 1);
        for (int guard = 0; guard < 100000; guard++) {
            int month = calendar.get(Calendar.MONTH) + 1;
            if (!months[month]) {
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.add(Calendar.MONTH, 1);
                continue;
            }
            if (!dayMatches(calendar)) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                continue;
            }
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            if (!hours[hour]) {
                calendar.add(Calendar.HOUR_OF_DAY, 1);
                calendar.set(Calendar.MINUTE, 0);
                continue;
            }
            int minute = calendar.get(Calendar.MINUTE);
            if (!minutes[minute]) {
                calendar.add(Calendar.MINUTE, 1);
                continue;
            }
            return calendar.getTimeInMillis();
        }
        return -1L;
    }

    /** 当天是否命中（日 / 周取并的 cron 惯例）。 */
    private boolean dayMatches(Calendar calendar) {
        boolean domHit = daysOfMonth[calendar.get(Calendar.DAY_OF_MONTH)];
        int week = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        boolean dowHit = daysOfWeek[week];
        if (everyDayOfMonth) {
            return dowHit;
        }
        if (everyDayOfWeek) {
            return domHit;
        }
        return domHit || dowHit;
    }

    /** 解析一段（写入布尔表）。 */
    private static void parse(String part, boolean[] target, int min, int max, String label) {
        List<Integer> values = new ArrayList<Integer>();
        for (String item : part.split(",")) {
            String text = item.trim();
            if (text.isEmpty()) {
                throw new IllegalArgumentException("cron 的" + label + "段有空项: " + part);
            }
            int step = 1;
            String range = text;
            int slash = text.indexOf('/');
            if (slash >= 0) {
                range = text.substring(0, slash).trim();
                String stepText = text.substring(slash + 1).trim();
                try {
                    step = Integer.parseInt(stepText);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("cron 的" + label + "段步长非法: " + text);
                }
                if (step <= 0) {
                    throw new IllegalArgumentException("cron 的" + label + "段步长必须为正: " + text);
                }
            }
            int from;
            int to;
            if ("*".equals(range)) {
                from = min;
                to = max;
            } else if (range.contains("-")) {
                String[] bounds = range.split("-");
                from = parseValue(bounds[0], min, max, label);
                to = parseValue(bounds[1], min, max, label);
                if (from > to) {
                    throw new IllegalArgumentException("cron 的" + label + "段区间反了: " + text);
                }
            } else {
                from = parseValue(range, min, max, label);
                to = slash >= 0 ? max : from;
            }
            for (int value = from; value <= to; value += step) {
                values.add(Integer.valueOf(value));
            }
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("cron 的" + label + "段没有有效取值: " + part);
        }
        for (Integer value : values) {
            target[value.intValue()] = true;
        }
    }

    private static int parseValue(String text, int min, int max, String label) {
        int value;
        try {
            value = Integer.parseInt(text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("cron 的" + label + "段取值非法: " + text);
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException("cron 的" + label + "段取值越界（" + min + "-" + max + "）: " + text);
        }
        return value;
    }

    @Override
    public String toString() {
        return expression;
    }
}
