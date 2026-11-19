package com.zifang.z.etl.core.pipeline;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 流水线声明解析器：JSON / yml → {@link PipelineSpec}。
 *
 * <p>与旧版 {@link com.zifang.z.etl.core.job.JobDescriptorParser} 同口径：
 * 去掉空白后以 <code>{</code> 开头按 JSON 解析，否则按 yml 解析；
 * yml 走 snakeyaml 的 {@link SafeConstructor}（不实例化任意 Java 类），JSON 走 Jackson。
 *
 * <p>顶层允许包一层 {@code job:}（与旧版一致），也允许直接是描述对象。
 *
 * @author zifang
 */
public final class PipelineParser {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private PipelineParser() {
    }

    /**
     * 解析流水线声明（自动判别 JSON / yml）。
     *
     * @param text 声明原文
     * @return 流水线声明
     */
    public static PipelineSpec parse(String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("流水线声明为空");
        }
        String trimmed = text.trim();
        if (trimmed.startsWith("{")) {
            return parseJson(trimmed);
        }
        return parseYaml(trimmed);
    }

    /**
     * 解析 JSON 形态。
     *
     * @param text JSON 文本
     * @return 流水线声明
     */
    public static PipelineSpec parseJson(String text) {
        try {
            Map<String, Object> raw = MAPPER.readValue(text, Map.class);
            return fromMap(raw);
        } catch (IOException e) {
            throw new IllegalArgumentException("JSON 流水线声明解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 解析 yml 形态。
     *
     * @param text yml 文本
     * @return 流水线声明
     */
    public static PipelineSpec parseYaml(String text) {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new SafeConstructor(options));
        Object loaded;
        try {
            loaded = yaml.load(text);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("yml 流水线声明解析失败: " + e.getMessage(), e);
        }
        if (!(loaded instanceof Map)) {
            throw new IllegalArgumentException("yml 流水线声明顶层必须是映射结构");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> raw = (Map<String, Object>) loaded;
        return fromMap(raw);
    }

    @SuppressWarnings("unchecked")
    private static PipelineSpec fromMap(Map<String, Object> raw) {
        Object inner = raw.get("job");
        Map<String, Object> body = (inner instanceof Map) ? (Map<String, Object>) inner : raw;
        PipelineSpec spec;
        try {
            spec = MAPPER.convertValue(body, PipelineSpec.class);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("流水线声明结构不合法: " + e.getMessage(), e);
        }
        if (spec == null) {
            throw new IllegalArgumentException("流水线声明结构不合法");
        }
        // 类型串 → 算子类型（解析期就定下来，校验期不用再猜）
        for (PipelineNode node : spec.getNodes()) {
            if (node == null) {
                continue;
            }
            OperatorType type = OperatorType.fromId(node.getType());
            if (type == null) {
                throw new IllegalArgumentException("节点 " + node.getId() + " 的类型无法识别: " + node.getType()
                        + "（可用类型: " + OperatorType.allIds() + "）");
            }
            node.setOperatorType(type);
        }
        return spec;
    }

    /** 声明对象 → canonical JSON（落库用；键序稳定，便于 diff 与幂等比较）。 */
    public static String toJson(PipelineSpec spec) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(spec);
        } catch (IOException e) {
            throw new IllegalStateException("流水线声明序列化失败: " + e.getMessage(), e);
        }
    }

    /** 声明对象 → yml（落库用；与 JSON 是同一份内容）。 */
    @SuppressWarnings("unchecked")
    public static String toYaml(PipelineSpec spec) {
        Map<String, Object> raw = MAPPER.convertValue(spec, LinkedHashMap.class);
        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(new SafeConstructor(options));
        return yaml.dump(raw);
    }
}
