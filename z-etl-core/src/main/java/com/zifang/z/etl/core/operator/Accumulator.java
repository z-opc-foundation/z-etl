package com.zifang.z.etl.core.operator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 聚合累加器（可序列化 ⇒ 能进检查点、能断点续跑）。
 *
 * <p>一个累加器对应一个聚合函数（{@code fn} + {@code column} + {@code alias}），
 * 持有该函数在某个键 / 某个窗口上的中间状态：
 * <ul>
 *   <li>{@code SUM} / {@code AVG}：求和 + 计数；</li>
 *   <li>{@code COUNT}：计数；</li>
 *   <li>{@code MIN} / {@code MAX}：极值；</li>
 *   <li>{@code FIRST} / {@code LAST}：首值 / 末值。</li>
 * </ul>
 *
 * <p>数值一律走 {@link BigDecimal}：金额与浮点混算是 ETL 里最常见的一类静默错误，
 * 累加器这一层就把 double 语义挡掉。
 *
 * @author zifang
 */
public class Accumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 求和。 */
    public static final String FN_SUM = "SUM";
    /** 计数。 */
    public static final String FN_COUNT = "COUNT";
    /** 平均。 */
    public static final String FN_AVG = "AVG";
    /** 最小。 */
    public static final String FN_MIN = "MIN";
    /** 最大。 */
    public static final String FN_MAX = "MAX";
    /** 首值。 */
    public static final String FN_FIRST = "FIRST";
    /** 末值。 */
    public static final String FN_LAST = "LAST";

    /** AVG 的结果小数位（默认 4，与旧引擎聚合下推的 {@code DECIMAL(18,4)} 对齐）。 */
    private static final int DEFAULT_AVG_SCALE = 4;

    private final String fn;
    private final String column;
    private final String alias;

    private BigDecimal sum;
    private long count;
    private Object extreme;
    private Object first;
    private Object last;
    private boolean hasValue;

    /**
     * @param fn     聚合函数
     * @param column 源列（{@code *} 表示 COUNT(*))
     * @param alias  输出列名
     */
    public Accumulator(String fn, String column, String alias) {
        this.fn = fn;
        this.column = column;
        this.alias = alias;
    }

    /** 聚合函数。 */
    public String fn() {
        return fn;
    }

    /** 源列。 */
    public String column() {
        return column;
    }

    /** 输出列名。 */
    public String alias() {
        return alias;
    }

    /** 是否收到过非空值。 */
    public boolean hasValue() {
        return hasValue;
    }

    /**
     * 累加一条输入值。
     *
     * @param value 输入值（可为 null）
     */
    public void accumulate(Object value) {
        if (value == null) {
            if (FN_COUNT.equals(fn)) {
                // COUNT(*) 也要数空行
                count++;
                hasValue = true;
            }
            return;
        }
        hasValue = true;
        count++;
        BigDecimal numeric = toBigDecimal(value);
        if (numeric != null) {
            sum = sum == null ? numeric : sum.add(numeric);
        }
        if (first == null) {
            first = normalize(value);
        }
        last = normalize(value);
        if (FN_MIN.equals(fn) || FN_MAX.equals(fn)) {
            Object normalized = normalize(value);
            if (extreme == null) {
                extreme = normalized;
            } else {
                int cmp = compare(normalized, extreme);
                if (FN_MIN.equals(fn) ? cmp < 0 : cmp > 0) {
                    extreme = normalized;
                }
            }
        }
    }

    /**
     * 合并另一个累加器（会话窗口合并 / 状态恢复时用）。
     *
     * @param other 另一个累加器
     */
    public void merge(Accumulator other) {
        if (other == null) {
            return;
        }
        if (other.sum != null) {
            sum = sum == null ? other.sum : sum.add(other.sum);
        }
        count += other.count;
        if (other.hasValue) {
            hasValue = true;
        }
        if (first == null) {
            first = other.first;
        }
        if (other.last != null) {
            last = other.last;
        }
        if (other.extreme != null) {
            if (extreme == null) {
                extreme = other.extreme;
            } else {
                int cmp = compare(other.extreme, extreme);
                if (FN_MIN.equals(fn) ? cmp < 0 : cmp > 0) {
                    extreme = other.extreme;
                }
            }
        }
    }

    /**
     * 聚合结果。
     *
     * @return 结果值
     */
    public Object result() {
        if (FN_COUNT.equals(fn)) {
            return Long.valueOf(count);
        }
        if (FN_SUM.equals(fn)) {
            return sum == null ? BigDecimal.ZERO : sum;
        }
        if (FN_AVG.equals(fn)) {
            if (sum == null || count == 0L) {
                return null;
            }
            return sum.divide(BigDecimal.valueOf(count), DEFAULT_AVG_SCALE, RoundingMode.HALF_UP);
        }
        if (FN_MIN.equals(fn) || FN_MAX.equals(fn)) {
            return extreme;
        }
        if (FN_FIRST.equals(fn)) {
            return first;
        }
        if (FN_LAST.equals(fn)) {
            return last;
        }
        return null;
    }

    /** 值归一：数值转 BigDecimal；其余转字符串（保证可序列化）。 */
    private static Object normalize(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return value;
        }
        if (value instanceof Number) {
            return toBigDecimal(value);
        }
        if (value instanceof Boolean || value instanceof String
                || value instanceof java.util.Date || value instanceof byte[]) {
            return value;
        }
        return String.valueOf(value);
    }

    /** 值转 BigDecimal；转不了返回 {@code null}。 */
    public static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 比较两个归一后的值：数值按数值比，其余按字符串比。 */
    private static int compare(Object left, Object right) {
        BigDecimal leftNumber = toBigDecimal(left);
        BigDecimal rightNumber = toBigDecimal(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }
}
