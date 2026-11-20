package com.zifang.z.etl.core.runtime;

/**
 * 可计数的汇（sink）。
 *
 * <p>执行器汇总「写了多少行」时不再逐个 instanceof，而是问这个接口 ——
 * 加一种新输出类型时，计数能力随实现一起来，执行器零改动。
 *
 * @author zifang
 */
public interface CountingSink extends Operator {

    /** 已写出的行数。 */
    long written();
}
