package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 资料磨坊源（输入类型）：把 z-mill（全格式文档 → Markdown/JSON/文本/HTML）当成一条流。
 *
 * <p>走 z-mill-web 的 HTTP 面（{@code POST /api/grind} multipart 上传本地文件、
 * {@code POST /api/grind-url} 抓网页），两种产出模式：
 * <ul>
 *   <li>{@code chunk}（默认）：按标题路径分块，一块一行（{@code source/index/heading/path/text}）——
 *       RAG 资料粒度直接进管道做向量化 / 清洗 / 入库；</li>
 *   <li>{@code doc}：一文档一行（{@code source/format/blocks/chars/markdown/json/text/html}）——
 *       整件成品搬运。</li>
 * </ul>
 *
 * <pre>
 * - id: docs
 *   type: mill.source
 *   config:
 *     baseUrl: http://127.0.0.1:8891      # z-mill-web
 *     file: /data/资料库/报告.docx        # file 与 url 二选一
 *     # url: https://example.com/page    # 网页直磨
 *     mode: chunk                         # chunk（默认）| doc
 * </pre>
 *
 * <p>⚠️ 信任边界：{@code baseUrl} / {@code file} / {@code url} 都来自流水线声明 ⇒
 * 只让可信的作业登记人写声明。
 *
 * @author zifang
 */
public class MillSource extends AbstractPullSource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** z-mill-web 地址。 */
    private final String baseUrl;

    /** 本地文件路径（与 url 二选一）。 */
    private final String file;

    /** 网页地址（与 file 二选一）。 */
    private final String url;

    /** 产出模式：chunk | doc。 */
    private final String mode;

    private final int timeoutMs;

    private final HttpClient client;

    private List<Map<String, Object>> buffer = new ArrayList<Map<String, Object>>();
    private int bufferIndex;

    public MillSource(PipelineNode node) {
        super(node);
        baseUrl = trimTrailingSlash(node.string("baseUrl"));
        if (baseUrl == null) {
            throw new IllegalArgumentException("mill.source 节点 " + node.getId() + " 缺少 baseUrl（z-mill-web 地址）");
        }
        file = node.string("file");
        url = node.string("url");
        if ((file == null) == (url == null)) {
            throw new IllegalArgumentException("mill.source 节点 " + node.getId()
                    + " 的 file 与 url 必须二选一（本地文件走 multipart 上传，网页走 grind-url 抓取）");
        }
        mode = node.string("mode", "chunk").toLowerCase();
        if (!"chunk".equals(mode) && !"doc".equals(mode)) {
            throw new IllegalArgumentException("mill.source 节点 " + node.getId()
                    + " 的 mode 只支持 chunk|doc，实际: " + mode);
        }
        timeoutMs = node.integer("timeoutMs", 30000);
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        buffer = "doc".equals(mode) ? grindDoc() : grindChunks();
        bufferIndex = 0;
    }

    @Override
    protected Map<String, Object> doNext() {
        if (bufferIndex >= buffer.size()) {
            return null;
        }
        return buffer.get(bufferIndex++);
    }

    /** doc 模式：一文档一行，带四形态成品。 */
    private List<Map<String, Object>> grindDoc() throws Exception {
        JsonNode result = grind("md,json,txt,html");
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("source", textOf(result.get("name")));
        row.put("format", textOf(result.get("format")));
        JsonNode stats = result.get("stats");
        row.put("blocks", stats == null ? null : intOf(stats.get("blocks")));
        row.put("chars", stats == null ? null : intOf(stats.get("chars")));
        JsonNode outputs = result.get("outputs");
        row.put("markdown", textOf(outputs.get("md")));
        row.put("json", textOf(outputs.get("json")));
        row.put("text", textOf(outputs.get("txt")));
        row.put("html", textOf(outputs.get("html")));
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows;
    }

    /** chunk 模式：一块一行（RAG 粒度）。 */
    private List<Map<String, Object>> grindChunks() throws Exception {
        JsonNode result = grind("md");
        JsonNode chunks = result.get("chunks");
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        if (chunks == null || !chunks.isArray()) {
            return rows;
        }
        String source = textOf(result.get("name"));
        int index = 0;
        for (JsonNode chunk : chunks) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("source", source);
            row.put("index", index++);
            row.put("heading", textOf(chunk.get("heading")));
            row.put("path", joinPath(chunk.get("path")));
            row.put("text", textOf(chunk.get("text")));
            rows.add(row);
        }
        return rows;
    }

    /**
     * 调 z-mill-web 磨一次，返回 results[0]。
     *
     * @param views 成品形态（grind-url 固定四形态，这里只用来对齐语义）
     */
    private JsonNode grind(String views) throws Exception {
        HttpResponse<String> response = file != null ? postFile(views) : postUrl();
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("mill.source 节点 " + node.getId() + " 调用 z-mill 失败：HTTP "
                    + response.statusCode() + " " + abbreviate(response.body()));
        }
        JsonNode root = MAPPER.readTree(response.body());
        JsonNode results = root.get("results");
        if (results == null || !results.isArray() || results.size() == 0) {
            throw new IllegalStateException("mill.source 节点 " + node.getId() + " 的 z-mill 响应没有 results");
        }
        JsonNode result = results.get(0);
        String status = textOf(result.get("status"));
        if (!"ok".equals(status)) {
            JsonNode error = result.get("error");
            throw new IllegalStateException("mill.source 节点 " + node.getId() + " 研磨失败（status=" + status
                    + "）：source=" + textOf(result.get("name"))
                    + (error == null ? "" : " " + textOf(error)));
        }
        return result;
    }

    /** 本地文件 → multipart 上传 /api/grind。 */
    private HttpResponse<String> postFile(String views) throws Exception {
        Path path = Paths.get(file);
        byte[] content;
        try {
            content = Files.readAllBytes(path);
        } catch (IOException e) {
            throw new IllegalStateException("mill.source 节点 " + node.getId() + " 读不到文件: " + file, e);
        }
        String boundary = "zetl-mill-" + Long.toHexString(System.currentTimeMillis());
        String name = path.getFileName() == null ? "document" : path.getFileName().toString();
        java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();
        appendPart(body, boundary, "file", name, content);
        appendField(body, boundary, "views", views);
        body.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/grind"))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** 网页 → /api/grind-url。 */
    private HttpResponse<String> postUrl() throws Exception {
        String json = "{\"url\":\"" + escapeJson(url) + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/grind-url"))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void appendPart(java.io.ByteArrayOutputStream body, String boundary,
                                   String name, String filename, byte[] content) throws IOException {
        body.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
        if (filename != null) {
            body.write(("Content-Disposition: form-data; name=\"" + name + "\"; filename=\""
                    + escapeJson(filename) + "\"\r\nContent-Type: application/octet-stream\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            body.write(content);
            body.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 复合字段拼装（views 这类无文件名的 part）。 */
    private static void appendField(java.io.ByteArrayOutputStream body, String boundary,
                                    String name, String value) throws IOException {
        body.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
        body.write(("Content-Disposition: form-data; name=\"" + name + "\"\r\n\r\n" + value + "\r\n")
                .getBytes(StandardCharsets.UTF_8));
    }

    private static String joinPath(JsonNode path) {
        if (path == null || !path.isArray()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Iterator<JsonNode> items = path.elements();
        while (items.hasNext()) {
            if (sb.length() > 0) {
                sb.append(" / ");
            }
            sb.append(items.next().asText());
        }
        return sb.toString();
    }

    private static String textOf(JsonNode node) {
        return node == null || node.isNull() ? null : node.asText();
    }

    private static Integer intOf(JsonNode node) {
        return node == null || node.isNull() ? null : node.asInt();
    }

    private static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
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
