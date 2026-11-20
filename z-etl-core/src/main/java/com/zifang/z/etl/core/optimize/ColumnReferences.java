package com.zifang.z.etl.core.optimize;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 列引用提取：从表达式 / 脚本文本里找出「像列名的标识符」。
 *
 * <p>口径（保守优先）：**不排除任何关键字** —— 提取到的标识符只当**候选列名**，
 * 最终与源声明的列清单求交才作数。
 * 于是「把关键字当列名」的代价只是多留一列（安全），
 * 而「漏掉某个真实列名」的代价是丢数据（不安全）—— 后者必须避免。
 *
 * @author zifang
 */
public final class ColumnReferences {

    /** 标识符：字母或下划线开头，后接字母数字下划线。 */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private ColumnReferences() {
    }

    /**
     * 提取文本里的候选列名（小写化；去重且保序）。
     *
     * @param text 表达式 / 脚本文本
     * @return 候选列名（小写）
     */
    public static Set<String> candidates(String text) {
        Set<String> result = new LinkedHashSet<String>();
        if (text == null) {
            return result;
        }
        Matcher matcher = IDENTIFIER.matcher(text);
        while (matcher.find()) {
            result.add(matcher.group().toLowerCase());
        }
        return result;
    }

    /**
     * 文本里的候选列名（多个文本合并）。
     *
     * @param texts 表达式 / 脚本文本集合
     * @return 候选列名（小写）
     */
    public static Set<String> candidates(Iterable<String> texts) {
        Set<String> result = new LinkedHashSet<String>();
        if (texts == null) {
            return result;
        }
        for (String text : texts) {
            result.addAll(candidates(text));
        }
        return result;
    }

    /** 列名小写化（比较用；行内取值本身是大小写不敏感的）。 */
    public static String normalize(String column) {
        return column == null ? null : column.trim().toLowerCase();
    }
}
