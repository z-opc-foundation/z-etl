package com.zifang.z.etl.core.job;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 目标类型名全集（字段映射 {@code type} 的合法取值）。
 *
 * <p>类型归一规则是引擎层的事（需求 008 notes.md §9.6.1 E1）：JDBC 给原生类型、Excel 给全 String、
 * 内存 SQL 给浮点，三套语义必须各自落一条归一规则，这里给出规则**能表达的类型名**。
 *
 * @author zifang
 */
public final class TypeNames {

    /** 64 位整型。 */
    public static final String LONG = "LONG";
    /** 32 位整型。 */
    public static final String INT = "INT";
    /** 字符串。 */
    public static final String STRING = "STRING";
    /** 高精度小数（金额必须落这个，见 notes §9.2.3 浮点漂移）。 */
    public static final String BIGDECIMAL = "BIGDECIMAL";
    /** 双精度浮点。 */
    public static final String DOUBLE = "DOUBLE";
    /** 布尔。 */
    public static final String BOOLEAN = "BOOLEAN";
    /** 时间戳。 */
    public static final String TIMESTAMP = "TIMESTAMP";
    /** 日期。 */
    public static final String DATE = "DATE";

    private static final Set<String> KNOWN = Collections.unmodifiableSet(new LinkedHashSet<String>(Arrays.asList(
            LONG, INT, STRING, BIGDECIMAL, DOUBLE, BOOLEAN, TIMESTAMP, DATE)));

    private TypeNames() {
    }

    /** 是否是已知类型名（大小写不敏感）。 */
    public static boolean isKnown(String type) {
        return type != null && KNOWN.contains(type.trim().toUpperCase());
    }

    /** 全部类型名的可读串（报错用）。 */
    public static String supported() {
        return String.join("/", KNOWN);
    }

    /** 全部类型名（副本）。 */
    public static Set<String> all() {
        return KNOWN;
    }
}
