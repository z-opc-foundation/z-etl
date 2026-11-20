package com.zifang.z.etl.core.pipeline;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 管道参数（占位符 {@code ${name}}）：同一份声明跑不同批次。
 *
 * <p>口径：
 * <ul>
 *   <li><b>声明里写占位符，运行时给值</b>：{@code where: "dt = '${bizdate}'"}、
 *       {@code table: "orders_${bizdate}"}、{@code path: "/data/${bizdate}/orders.csv"}；</li>
 *   <li><b>内置参数</b>：{@code bizdate}（今天）/ {@code yesterday}（昨天）/ {@code now}（当前时间），
 *       显式传入的同名参数优先；</li>
 *   <li><b>缺参数直接报错</b>：不拿空串糊过去 —— 空串会让 WHERE 变成「恒真」，
 *       增量悄悄变全量（隐性数据事故）；</li>
 *   <li><b>参数值走白名单</b>：只允许字母数字与 {@code _ . : / + - =}（无引号、无分号、无空格）
 *       ⇒ 拼进 {@code where} / {@code orderBy} 这类 SQL 片段也不会变成注入面；</li>
 *   <li><b>流水线名不支持占位符</b>：名字是水位 / 状态快照的键，改名等于换一条管道。</li>
 * </ul>
 *
 * @author zifang
 */
public final class PipelineParams {

    /** 占位符：{@code ${name}}。 */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}");

    /** 参数值白名单（无引号 / 分号 / 空格 / 美元号 ⇒ 拼进 SQL 也安全）。 */
    private static final Pattern SAFE_VALUE = Pattern.compile("[A-Za-z0-9_.:/+\\-=]{0,200}");

    /** 内置参数名（可被显式参数覆盖）。 */
    public static final String BIZDATE = "bizdate";
    public static final String YESTERDAY = "yesterday";
    public static final String NOW = "now";

    private PipelineParams() {
    }

    /**
     * 内置参数（今天 / 昨天 / 现在）。
     *
     * @return 参数表
     */
    public static Map<String, String> builtins() {
        Map<String, String> params = new LinkedHashMap<String, String>();
        LocalDate today = LocalDate.now();
        params.put(BIZDATE, today.toString());
        params.put(YESTERDAY, today.minusDays(1).toString());
        params.put(NOW, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        return params;
    }

    /**
     * 用参数解析声明里的占位符（**就地改写**）。
     *
     * @param spec   声明（调用方负责传副本，别把落库的定义改坏）
     * @param params 参数（可为 {@code null} ⇒ 只有内置参数可用）
     * @return 同一个声明对象（便于链式调用）
     */
    public static PipelineSpec resolve(PipelineSpec spec, Map<String, String> params) {
        if (spec == null) {
            throw new IllegalArgumentException("流水线声明为空");
        }
        if (spec.getName() != null && PLACEHOLDER.matcher(spec.getName()).find()) {
            throw new IllegalArgumentException("流水线名不支持占位符（名字是水位 / 状态快照的键）: " + spec.getName());
        }
        Map<String, String> merged = new LinkedHashMap<String, String>(builtins());
        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                String key = entry.getKey() == null ? "" : entry.getKey().trim();
                if (!key.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                    throw new IllegalArgumentException("参数名非法: " + entry.getKey() + "（只允许字母数字与下划线）");
                }
                merged.put(key, entry.getValue() == null ? "" : entry.getValue());
            }
        }
        Set<String> missing = new LinkedHashSet<String>();
        if (spec.getDescription() != null) {
            spec.setDescription(resolveText(spec.getDescription(), merged, missing));
        }
        for (PipelineNode node : spec.getNodes()) {
            if (node == null) {
                continue;
            }
            node.setConfig(resolveMap(node.getConfig(), merged, missing));
        }
        spec.setCheckpoint(resolveMap(spec.getCheckpoint(), merged, missing));
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("流水线 " + spec.getName() + " 缺少参数: " + String.join(", ", missing)
                    + "（可用内置参数: " + BIZDATE + "/" + YESTERDAY + "/" + NOW + "）");
        }
        return spec;
    }

    /**
     * 声明文本里的占位符替换（只替换字符串里的占位符，参数值本身安全）。
     *
     * @param text    原文
     * @param params  参数表
     * @return 替换后的文本
     */
    public static String resolveText(String text, Map<String, String> params) {
        Set<String> missing = new LinkedHashSet<String>();
        String result = resolveText(text, params, missing);
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("缺少参数: " + String.join(", ", missing));
        }
        return result;
    }

    /** 替换（缺参数记进 missing，不在这里抛）。 */
    private static String resolveText(String text, Map<String, String> params, Set<String> missing) {
        if (text == null || text.indexOf("${") < 0) {
            return text;
        }
        Matcher matcher = PLACEHOLDER.matcher(text);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = params.get(key);
            if (value == null) {
                missing.add(key);
                matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group()));
                continue;
            }
            requireSafeValue(key, value);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /** 参数值必须过白名单（否则拼进 SQL 就是注入面）。 */
    private static void requireSafeValue(String key, String value) {
        if (!SAFE_VALUE.matcher(value).matches()) {
            throw new IllegalArgumentException("参数 " + key + " 的值不合法: \"" + value
                    + "\"（只允许字母数字与 _ . : / + - =，长度 ≤ 200）");
        }
    }

    /** 映射里的值递归替换（字符串 / 嵌套映射 / 列表）。 */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> resolveMap(Map<String, Object> raw, Map<String, String> params,
                                                   Set<String> missing) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        if (raw == null) {
            return result;
        }
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            result.put(entry.getKey(), resolveValue(entry.getValue(), params, missing));
        }
        return result;
    }

    /** 值的递归替换。 */
    private static Object resolveValue(Object value, Map<String, String> params, Set<String> missing) {
        if (value instanceof String) {
            return resolveText((String) value, params, missing);
        }
        if (value instanceof Map) {
            return resolveMap((Map<String, Object>) value, params, missing);
        }
        if (value instanceof List) {
            List<Object> result = new ArrayList<Object>();
            for (Object item : (List<?>) value) {
                result.add(resolveValue(item, params, missing));
            }
            return result;
        }
        return value;
    }
}
