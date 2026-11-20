package com.zifang.z.etl.core;

import com.zifang.z.etl.core.util.CronExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * cron 表达式验收（调度登记靠它算「下一次触发时间」）。
 *
 * @author zifang
 */
class CronExpressionTest {

    @Test
    @DisplayName("*/5 分：下一次触发落在 5 分钟刻度上")
    void everyFiveMinutes() {
        Calendar base = at(2026, Calendar.JANUARY, 15, 10, 3);
        long next = CronExpression.parse("*/5 * * * *").nextFireTimeAfter(base.getTimeInMillis());
        Calendar fired = calendarOf(next);
        assertEquals(10, fired.get(Calendar.HOUR_OF_DAY));
        assertEquals(5, fired.get(Calendar.MINUTE), "应落在 5 分钟刻度");
        assertEquals(0, fired.get(Calendar.SECOND));
    }

    @Test
    @DisplayName("每天 03:00：下一次触发是当天或次日的 3 点")
    void dailyAtThree() {
        Calendar base = at(2026, Calendar.JANUARY, 15, 1, 0);
        long next = CronExpression.parse("0 3 * * *").nextFireTimeAfter(base.getTimeInMillis());
        Calendar fired = calendarOf(next);
        assertEquals(3, fired.get(Calendar.HOUR_OF_DAY));
        assertEquals(0, fired.get(Calendar.MINUTE));
        assertEquals(15, fired.get(Calendar.DAY_OF_MONTH), "当天 3 点还没到 ⇒ 同一天");

        Calendar late = at(2026, Calendar.JANUARY, 15, 4, 0);
        long nextDay = CronExpression.parse("0 3 * * *").nextFireTimeAfter(late.getTimeInMillis());
        assertEquals(16, calendarOf(nextDay).get(Calendar.DAY_OF_MONTH), "过了 3 点 ⇒ 次日");
    }

    @Test
    @DisplayName("每月 1 号 00:00 与「周一 09:00」都算得出来")
    void monthlyAndWeekly() {
        Calendar base = at(2026, Calendar.JANUARY, 15, 0, 0);
        long monthly = CronExpression.parse("0 0 1 * *").nextFireTimeAfter(base.getTimeInMillis());
        Calendar monthlyFire = calendarOf(monthly);
        assertEquals(1, monthlyFire.get(Calendar.DAY_OF_MONTH));
        assertEquals(Calendar.FEBRUARY, monthlyFire.get(Calendar.MONTH), "1 月 15 号之后 ⇒ 下一个是 2 月 1 号");

        long weekly = CronExpression.parse("0 9 * * 1").nextFireTimeAfter(base.getTimeInMillis());
        Calendar weeklyFire = calendarOf(weekly);
        assertEquals(9, weeklyFire.get(Calendar.HOUR_OF_DAY));
        assertEquals(Calendar.MONDAY, weeklyFire.get(Calendar.DAY_OF_WEEK), "周字段 1 = 周一");
    }

    @Test
    @DisplayName("列表与区间：0,30 分 与 9-17 时")
    void listsAndRanges() {
        Calendar base = at(2026, Calendar.JANUARY, 15, 10, 5);
        long next = CronExpression.parse("0,30 9-17 * * *").nextFireTimeAfter(base.getTimeInMillis());
        Calendar fired = calendarOf(next);
        assertEquals(30, fired.get(Calendar.MINUTE), "下一个刻度是半点");
        assertEquals(10, fired.get(Calendar.HOUR_OF_DAY));
    }

    @Test
    @DisplayName("非法表达式直接报错（段数 / 越界 / 空）")
    void rejectsInvalidExpressions() {
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                CronExpression.parse("* * *");
            }
        });
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                CronExpression.parse("60 * * * *");
            }
        });
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                CronExpression.parse("abc * * * *");
            }
        });
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                CronExpression.parse("  ");
            }
        });
    }

    @Test
    @DisplayName("每次触发后都能接着算下一次（不会卡在同一分钟）")
    void nextFireMovesForward() {
        Calendar base = at(2026, Calendar.JANUARY, 15, 10, 0);
        CronExpression expression = CronExpression.parse("* * * * *");
        long first = expression.nextFireTimeAfter(base.getTimeInMillis());
        long second = expression.nextFireTimeAfter(first);
        assertTrue(second > first, "下一次必须严格大于上一次");
        assertEquals(60000L, second - first, "每分钟一次 ⇒ 间隔 60 秒");
    }

    private static Calendar at(int year, int month, int day, int hour, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, day, hour, minute, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    private static Calendar calendarOf(long millis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        return calendar;
    }
}
