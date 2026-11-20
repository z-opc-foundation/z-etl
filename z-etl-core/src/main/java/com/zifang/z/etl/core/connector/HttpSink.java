package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HTTP 接口汇（输出类型）：把行 POST 到外部接口（webhook / 业务 API）。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code method}（默认 POST）、{@code headers}；</li>
 *   <li>{@code format}：{@code json}（默认，整行 JSON）/ {@code form}（URL 编码表单）；</li>
 *   <li>失败即抛（不静默吞）：HTTP 非 2xx 直接让流水线失败。</li>
 * </ul>
 *
 * @author zifang
 */
public class HttpSink implements CountingSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineNode node;
    private String url;
    private String method = "POST";
    private Map<String, Object> headers = new LinkedHashMap<String, Object>();
    private String format = "json";
    private int timeoutMs = 10000;

    private HttpClient client;
    private long written;

    public HttpSink(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        url = node.string("url");
        if (url == null) {
            throw new IllegalArgumentException("http.sink 节点 " + node.getId() + " 缺少 url");
        }
        method = node.string("method", "POST").toUpperCase();
        headers = node.map("headers");
        format = node.string("format", "json").toLowerCase();
        timeoutMs = node.integer("timeoutMs", 10000);
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        Map<String, Object> row = record.row();
        String payload;
        String contentType;
        if ("form".equals(format)) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (sb.length() > 0) {
                    sb.append('&');
                }
                sb.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8.name()))
                        .append('=')
                        .append(URLEncoder.encode(entry.getValue() == null ? "" : String.valueOf(entry.getValue()),
                                StandardCharsets.UTF_8.name()));
            }
            payload = sb.toString();
            contentType = "application/x-www-form-urlencoded";
        } else {
            payload = MAPPER.writeValueAsString(row);
            contentType = "application/json";
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", contentType);
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            builder.header(entry.getKey(), String.valueOf(entry.getValue()));
        }
        if ("GET".equals(method)) {
            builder.GET();
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(payload));
        }
        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("http.sink 节点 " + node.getId() + " 调用 " + url
                    + " 失败：HTTP " + response.statusCode() + " " + response.body());
        }
        written++;
    }

    @Override
    public void processWatermark(long watermark) {
        // 汇不涉水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 汇不涉定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public long written() {
        return written;
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
