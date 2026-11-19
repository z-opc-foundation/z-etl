package com.zifang.z.etl.core.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 流水线节点（算子实例声明）。
 *
 * <p>形态照抄 Kestra / n8n：{@code id + type + 自由 config}。
 * {@code type} 是命名空间类型串（{@code jdbc.source} / {@code window.aggregate} …），
 * {@code config} 是该类型自己的自由配置（键由算子自己解释）。
 *
 * <pre>
 * - id: enrich
 *   type: script
 *   config:
 *     name: pay_channel
 *     engine: GROOVY
 *     script: "..."
 * </pre>
 *
 * @author zifang
 */
public class PipelineNode {

    /** 节点 id（流水线内唯一）。 */
    private String id;

    /** 节点说明（人读）。 */
    private String description;

    /** 类型串（原样保留，便于报错回显）。 */
    private String type;

    /** 解析后的算子类型（解析后由 {@link PipelineParser} 填好）。 */
    private OperatorType operatorType;

    /** 算子自由配置。 */
    private Map<String, Object> config = new LinkedHashMap<String, Object>();

    public PipelineNode() {
    }

    public PipelineNode(String id, OperatorType operatorType) {
        this.id = id;
        this.operatorType = operatorType;
        this.type = operatorType == null ? null : operatorType.id();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config == null ? new LinkedHashMap<String, Object>() : config;
    }

    // —— config 取值助手（类型安全；缺省值由调用点给）——

    /** 取字符串配置。 */
    public String string(String key) {
        return string(key, null);
    }

    /** 取字符串配置（带缺省）。 */
    public String string(String key, String defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? defaultValue : text;
    }

    /** 取整型配置。 */
    public int integer(String key, int defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /** 取长整型配置。 */
    public long longValue(String key, long defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /** 取布尔配置。 */
    public boolean bool(String key, boolean defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }
        return "true".equalsIgnoreCase(String.valueOf(value).trim());
    }

    /** 取字符串列表配置。 */
    public List<String> strings(String key) {
        List<String> result = new ArrayList<String>();
        Object value = config.get(key);
        if (value == null) {
            return result;
        }
        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                if (item != null) {
                    result.add(String.valueOf(item).trim());
                }
            }
        } else {
            result.add(String.valueOf(value).trim());
        }
        return result;
    }

    /** 取映射配置。 */
    @SuppressWarnings("unchecked")
    public Map<String, Object> map(String key) {
        Object value = config.get(key);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return new LinkedHashMap<String, Object>();
    }

    /** 取映射列表配置（{@code aggregates} / {@code mappings} / {@code rules} 这类）。 */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> mapList(String key) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        Object value = config.get(key);
        if (!(value instanceof List)) {
            return result;
        }
        for (Object item : (List<?>) value) {
            if (item instanceof Map) {
                result.add((Map<String, Object>) item);
            }
        }
        return result;
    }

    /** 配置里有没有某个键。 */
    public boolean has(String key) {
        return config.containsKey(key);
    }

    @Override
    public String toString() {
        return "PipelineNode{id=" + id + ", type=" + type + "}";
    }
}
