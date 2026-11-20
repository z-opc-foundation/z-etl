package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP 接口源（输入类型）：把 REST / JSON 接口当成一条流来拉。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code method}（默认 GET）、{@code headers}、{@code body}；</li>
 *   <li>{@code itemPath} 定位响应里的数组（点路径）；默认根节点就是数组；</li>
 *   <li>{@code pageParam} + {@code pageSize} + {@code maxPages} ⇒ 简单分页（页码参数拼在 URL 上）；</li>
 *   <li>{@code allowedHosts} 声明后只允许这些主机（内网接口的信任边界）。</li>
 * </ul>
 *
 * <p>⚠️ 信任边界：URL 来自流水线声明 ⇒ 能访问主机上的任意地址；
 * 生产上请用 {@code allowedHosts} 收口，或只让可信的作业登记人写声明。
 *
 * @author zifang
 */
public class HttpSource extends AbstractPullSource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String url;
    private String method = "GET";
    private Map<String, Object> headers = new LinkedHashMap<String, Object>();
    private String body;
    private String itemPath;
    private List<String> declaredColumns = new ArrayList<String>();
    private Map<String, Object> columnTypes;
    private String pageParam;
    private int pageSize;
    private int maxPages = 1;
    private int timeoutMs = 10000;
    private List<String> allowedHosts = new ArrayList<String>();

    private HttpClient client;
    private List<Map<String, Object>> buffer = new ArrayList<Map<String, Object>>();
    private int bufferIndex;
    private int nextPage = 1;
    private boolean exhausted;
    private long produced;

    public HttpSource(PipelineNode node) {
        super(node);
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        url = node.string("url");
        if (url == null) {
            throw new IllegalArgumentException("http.source 节点 " + node.getId() + " 缺少 url");
        }
        method = node.string("method", "GET").toUpperCase();
        headers = node.map("headers");
        body = node.string("body");
        itemPath = node.string("itemPath");
        declaredColumns = node.strings("columns");
        columnTypes = node.map("columnTypes");
        pageParam = node.string("pageParam");
        pageSize = node.integer("pageSize", 0);
        maxPages = node.integer("maxPages", 1);
        timeoutMs = node.integer("timeoutMs", 10000);
        allowedHosts = node.strings("allowedHosts");
        checkHost(url);
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        if (bufferIndex >= buffer.size() && !exhausted) {
            fill();
        }
        if (bufferIndex >= buffer.size()) {
            return null;
        }
        produced++;
        return buffer.get(bufferIndex++);
    }

    /** 拉一页（必要时翻页）。 */
    private void fill() throws Exception {
        while (bufferIndex >= buffer.size() && !exhausted) {
            String target = pageParam == null ? url : appendPage(url, nextPage);
            List<Map<String, Object>> page = fetch(target);
            if (page.isEmpty()) {
                exhausted = true;
                break;
            }
            buffer = page;
            bufferIndex = 0;
            if (pageParam == null || nextPage >= maxPages) {
                exhausted = true;
                break;
            }
            nextPage++;
        }
    }

    /** 拼页码参数（{@code pageParam=页码}；声明了 pageSize 再带 {@code pageSize=}）。 */
    private String appendPage(String target, int page) {
        StringBuilder sb = new StringBuilder(target);
        sb.append(target.indexOf('?') >= 0 ? '&' : '?');
        sb.append(pageParam).append('=').append(page);
        if (pageSize > 0) {
            sb.append("&pageSize=").append(pageSize);
        }
        return sb.toString();
    }

    /** 调一次接口，取出数组项。 */
    private List<Map<String, Object>> fetch(String target) throws Exception {
        checkHost(target);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(target))
                .timeout(Duration.ofMillis(timeoutMs));
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            builder.header(entry.getKey(), String.valueOf(entry.getValue()));
        }
        if ("GET".equals(method)) {
            builder.GET();
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
        }
        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("http.source 节点 " + node.getId() + " 调用 " + target
                    + " 失败：HTTP " + response.statusCode());
        }
        return parseItems(response.body());
    }

    /** 响应文本 → 行列表。 */
    private List<Map<String, Object>> parseItems(String text) throws Exception {
        JsonNode root = MAPPER.readTree(text);
        JsonNode array = root;
        if (itemPath != null) {
            array = navigate(root, itemPath);
        }
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        if (array == null || array.isNull()) {
            return rows;
        }
        if (!array.isArray()) {
            rows.add(toRow(array));
            return rows;
        }
        for (JsonNode item : array) {
            rows.add(toRow(item));
        }
        return rows;
    }

    private JsonNode navigate(JsonNode root, String path) {
        JsonNode current = root;
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            if (current == null) {
                return null;
            }
            current = current.get(parts[i]);
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> toRow(JsonNode node) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        if (node == null || !node.isObject()) {
            return row;
        }
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            if (!declaredColumns.isEmpty() && !declaredColumns.contains(name)) {
                continue;
            }
            JsonNode value = node.get(name);
            Object raw;
            if (value == null || value.isNull()) {
                raw = null;
            } else if (value.isObject()) {
                raw = MAPPER.convertValue(value, Map.class);
            } else if (value.isArray()) {
                raw = MAPPER.convertValue(value, List.class);
            } else if (value.isBoolean()) {
                raw = Boolean.valueOf(value.booleanValue());
            } else if (value.isInt()) {
                raw = Integer.valueOf(value.intValue());
            } else if (value.isLong()) {
                raw = Long.valueOf(value.longValue());
            } else if (value.isDouble() || value.isFloat()) {
                raw = Double.valueOf(value.doubleValue());
            } else {
                raw = value.asText();
            }
            row.put(name, ColumnTypes.convert(raw, ColumnTypes.declared(columnTypes, name)));
        }
        return row;
    }

    /** 主机白名单校验（声明了 {@code allowedHosts} 才生效）。 */
    private void checkHost(String target) {
        if (allowedHosts.isEmpty()) {
            return;
        }
        String host;
        try {
            host = URI.create(target).getHost();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("http.source 节点 " + node.getId() + " 的 url 不合法: " + target);
        }
        for (String allowed : allowedHosts) {
            if (allowed.equalsIgnoreCase(host)) {
                return;
            }
        }
        throw new IllegalArgumentException("http.source 节点 " + node.getId() + " 拒绝访问主机 " + host
                + "（allowedHosts: " + String.join(", ", allowedHosts) + "）");
    }
}
