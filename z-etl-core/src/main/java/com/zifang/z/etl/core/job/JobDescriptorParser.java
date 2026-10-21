package com.zifang.z.etl.core.job;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * job 描述解析器：JSON / yml → {@link JobDescriptor}。
 *
 * <p>两种形态的落点一致（同一个 {@link JobDescriptor}），所以「落库 / 重跑」对两种形态是同一条路径。
 * 判别规则：去掉空白后以 {@code &#123;} 开头按 JSON 解析，否则按 yml 解析。
 *
 * <p>yml 走 snakeyaml 的 {@link SafeConstructor}（不实例化任意 Java 类），JSON 走 Jackson。
 *
 * @author zifang
 */
public final class JobDescriptorParser {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JobDescriptorParser() {
    }

    /**
     * 解析作业描述文本（自动判别 JSON / yml）。
     *
     * @param text 作业描述原文
     * @return 解析后的描述
     * @throws IllegalArgumentException 文本为空 / 语法错 / 结构不合法
     */
    public static JobDescriptor parse(String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("作业描述为空");
        }
        String trimmed = text.trim();
        if (trimmed.startsWith("{")) {
            return parseJson(trimmed);
        }
        return parseYaml(trimmed);
    }

    /**
     * 解析 JSON 形态。允许顶层直接是描述对象，也允许包一层 {@code {"job": {...}}}。
     */
    public static JobDescriptor parseJson(String text) {
        try {
            Map<String, Object> raw = MAPPER.readValue(text, Map.class);
            return fromMap(raw);
        } catch (IOException e) {
            throw new IllegalArgumentException("JSON 作业描述解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 解析 yml 形态。允许顶层直接是描述对象，也允许包一层 {@code job:}。
     */
    public static JobDescriptor parseYaml(String text) {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new SafeConstructor(options));
        Object loaded;
        try {
            loaded = yaml.load(text);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("yml 作业描述解析失败: " + e.getMessage(), e);
        }
        if (!(loaded instanceof Map)) {
            throw new IllegalArgumentException("yml 作业描述顶层必须是映射结构");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> raw = (Map<String, Object>) loaded;
        return fromMap(raw);
    }

    /** 把映射结构（可能包一层 job）转成描述对象。 */
    @SuppressWarnings("unchecked")
    private static JobDescriptor fromMap(Map<String, Object> raw) {
        Object inner = raw.get("job");
        Map<String, Object> body = (inner instanceof Map) ? (Map<String, Object>) inner : raw;
        try {
            return MAPPER.convertValue(body, JobDescriptor.class);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("作业描述结构不合法: " + e.getMessage(), e);
        }
    }

    /** 描述对象 → canonical JSON（落库用；键序稳定，便于 diff 与幂等比较）。 */
    public static String toJson(JobDescriptor descriptor) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(descriptor);
        } catch (IOException e) {
            throw new IllegalStateException("作业描述序列化失败: " + e.getMessage(), e);
        }
    }

    /** 描述对象 → yml（落库用；与 JSON 是同一份内容）。 */
    @SuppressWarnings("unchecked")
    public static String toYaml(JobDescriptor descriptor) {
        Map<String, Object> raw = MAPPER.convertValue(descriptor, LinkedHashMap.class);
        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(new SafeConstructor(options));
        return yaml.dump(raw);
    }
}
