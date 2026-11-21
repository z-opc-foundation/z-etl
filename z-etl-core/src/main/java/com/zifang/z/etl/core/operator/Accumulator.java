package com.zifang.z.etl.core.operator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 聚合累加器（可序列化 ⇒ 能进检查点、能断点续跑）。
 *
 * <p>一个累加器对应一个聚合函数（{@code fn} + {@code column} + {@code alias}），
 * 持有该函数在某个键 / 某个窗口上的中间状态：
 * <ul>
 *   <li>{@code SUM} / {@code AVG}：求和 + 计数；</li>
 *   <li>{@code COUNT}：计数；</li>
 *   <li>{@code MIN} / {@code MAX}：极值；</li>
 *   <li>{@code FIRST} / {@code LAST}：首值 / 末值；</li>
 *   <li>{@code COLLECT_LIST} / {@code COLLECT_SET}：值收成数组（跳过 null，封顶
 *       {@link #MAX_COLLECT}；SET 同值只收一次、保持插入序）。</li>
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
    /** 收集（值收成数组；跳过 null；配下游 explode 可「聚合再还原」）。 */
    public static final String FN_COLLECT_LIST = "COLLECT_LIST";
    /** 去重收集（同 COLLECT_LIST 但同值只收一次，保持插入序）。 */
    public static final String FN_COLLECT_SET = "COLLECT_SET";

    /** COLLECT_LIST 单键单窗口的收集上限（防状态撑爆；超出丢并计数）。 */
    public static final int MAX_COLLECT = 65536;

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
    private java.util.Collection<Object> collected;
    private long collectDropped;

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
        if (FN_COLLECT_LIST.equals(fn) || FN_COLLECT_SET.equals(fn)) {
            // 收集口径：跳过 null、数值归一、封顶防状态撑爆；SET 去重（插入序）
            if (collected == null) {
                collected = FN_COLLECT_SET.equals(fn)
                        ? new java.util.LinkedHashSet<Object>() : new ArrayList<Object>();
            }
            if (collected.size() < MAX_COLLECT) {
                collected.add(normalize(value));
            } else {
                collectDropped++;
            }
            return;
        }
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
        if (other.collected != null) {
            if (collected == null) {
                collected = new ArrayList<Object>();
            }
            for (Object item : other.collected) {
                if (collected.size() >= MAX_COLLECT) {
                    collectDropped++;
                } else {
                    collected.add(item);
                }
            }
            collectDropped += other.collectDropped;
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
        if (FN_COLLECT_LIST.equals(fn) || FN_COLLECT_SET.equals(fn)) {
            // 快照口径：给拷贝，不把内部活列表的引用交出去（滚动聚合每行产出的应是当时状态）
            return collected == null ? new ArrayList<Object>() : new ArrayList<Object>(collected);
        }
        return null;
    }

    /** COLLECT_LIST 超上限被丢掉的个数（读后清零 —— 增量口径，供算子转计计数器）。 */
    public long collectDroppedDelta() {
        long delta = collectDropped;
        collectDropped = 0L;
        return delta;
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
