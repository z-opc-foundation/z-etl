package com.zifang.z.etl.core.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.transform.TypeNormalizer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 平台脚本算子：把 **z-script 脚本平台**（版本 / 灰度 / 配额 / 调用审计那套）当成一行一个的远程函数调。
 *
 * <p>与 {@link ScriptOperator}（本地沙箱内嵌执行）分工：本地沙箱管「管道里现写的清洗逻辑」，
 * 本算子管「脚本中心里已上线、带版本与审计的脚本」——清洗逻辑沉淀到平台后，管道按
 * {@code scriptCode} 引用即可，改脚本不用改管道声明。
 *
 * <p>调用契约（z-script 平台真实端点）：
 * {@code POST {baseUrl}/api/script/run?scriptCode=xxx}，body = 整行 JSON 当执行参数，
 * 鉴权走 {@code X-Api-Key} 头（app + AK 口径）；响应 {@code {success, data, errorMessage}}，
 * {@code success=false} 抛错不静默吞。
 *
 * <p>产出模式与本地脚本算子同形：
 * {@code column}（默认，data 落 {@code name} 列）｜{@code row}（data 是 Map 整行替换）。
 *
 * <pre>
 * - id: enrich
 *   type: script.remote
 *   config:
 *     baseUrl: http://127.0.0.1:8086/script
 *     scriptCode: clean-order
 *     apiKey: sk-xxxx
 *     mode: column
 *     name: pay_channel
 * </pre>
 *
 * <p>⚠️ 信任边界：{@code baseUrl} 与 {@code apiKey} 都来自流水线声明 ⇒ 只让可信的
 * 作业登记人写声明；AK 走平台配额与审计，不用 4A / SSO。
 *
 * @author zifang
 */
public class ScriptRemoteOperator implements Operator {

    /** 产出模式：落一列。 */
    public static final String MODE_COLUMN = "column";
    /** 产出模式：整行替换。 */
    public static final String MODE_ROW = "row";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineNode node;

    private String baseUrl;
    private String scriptCode;
    private String apiKey;
    private String mode;
    private String name;
    private String type;
    private boolean nullable = true;
    private int timeoutMs;

    private HttpClient client;

    public ScriptRemoteOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) {
        baseUrl = trimTrailingSlash(node.string("baseUrl"));
        scriptCode = node.string("scriptCode");
        if (baseUrl == null) {
            throw new IllegalArgumentException("script.remote 节点 " + node.getId() + " 缺少 baseUrl（z-script 平台地址，含 context-path）");
        }
        if (scriptCode == null || scriptCode.trim().isEmpty()) {
            throw new IllegalArgumentException("script.remote 节点 " + node.getId() + " 缺少 scriptCode");
        }
        apiKey = node.string("apiKey");
        mode = node.string("mode", MODE_COLUMN).toLowerCase();
        name = node.string("name");
        type = node.string("type");
        nullable = node.bool("nullable", true);
        timeoutMs = node.integer("timeoutMs", 10000);
        if (MODE_ROW.equals(mode) && name != null) {
            throw new IllegalArgumentException("script.remote 节点 " + node.getId() + " 的 mode=row 不需要 name（整行替换）");
        }
        if (MODE_COLUMN.equals(mode) && name == null) {
            throw new IllegalArgumentException("script.remote 节点 " + node.getId() + " 的 mode=column 必须声明 name");
        }
        if (!MODE_COLUMN.equals(mode) && !MODE_ROW.equals(mode)) {
            throw new IllegalArgumentException("script.remote 节点 " + node.getId()
                    + " 的 mode 只支持 column|row，实际: " + mode);
        }
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
    }

    @Override
    public void processElement(Record record, Output output) {
        Object value = invoke(record.row());
        if (MODE_ROW.equals(mode)) {
            if (!(value instanceof Map)) {
                throw new IllegalStateException("script.remote 节点 " + node.getId()
                        + " 的 mode=row 要求脚本返回对象，实际返回 "
                        + (value == null ? "null" : value.getClass().getName()));
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> newRow = new LinkedHashMap<String, Object>((Map<String, Object>) value);
            output.collect(record.withRow(newRow));
            return;
        }
        Object coerced = TypeNormalizer.coerce(value, type, Boolean.valueOf(nullable));
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        newRow.put(name, coerced);
        output.collect(record.withRow(newRow));
    }

    /** 调平台跑一次脚本；success=false / HTTP 非 2xx 一律抛。 */
    private Object invoke(Map<String, Object> row) {
        String target = baseUrl + "/api/script/run?scriptCode=" + urlEncode(scriptCode.trim());
        try {
            String body = MAPPER.writeValueAsString(row);
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(target))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json; charset=utf-8")
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            if (apiKey != null && !apiKey.trim().isEmpty()) {
                builder.header("X-Api-Key", apiKey.trim());
            }
            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException("script.remote 节点 " + node.getId() + " 调用平台失败：HTTP "
                        + response.statusCode() + " " + abbreviate(response.body()));
            }
            JsonNode root = MAPPER.readTree(response.body());
            if (root.path("success").asBoolean(false)) {
                JsonNode data = root.get("data");
                return MAPPER.convertValue(data, Object.class);
            }
            throw new IllegalStateException("script.remote 节点 " + node.getId() + " 脚本 "
                    + scriptCode + " 执行失败: " + root.path("errorMessage").asText("(平台未给原因)"));
        } catch (IllegalStateException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("script.remote 节点 " + node.getId() + " 调用被中断", e);
        } catch (Exception e) {
            throw new IllegalStateException("script.remote 节点 " + node.getId() + " 调用平台失败: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    private static String urlEncode(String text) {
        try {
            return java.net.URLEncoder.encode(text, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            return text;
        }
    }

    private static String trimTrailingSlash(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String abbreviate(String text) {
        if (text == null) {
            return "";
        }
        return text.length() > 200 ? text.substring(0, 200) + "…" : text;
    }
}
