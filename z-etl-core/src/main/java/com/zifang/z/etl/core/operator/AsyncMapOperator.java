package com.zifang.z.etl.core.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.util.RowValues;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 异步映射算子：每行**异步外呼** HTTP 接口补列（Flink AsyncFunction 口径）。
 *
 * <p>与 {@code map}（表达式）的区别：补的列来自**外部接口** —— 拉一次行情、查一次用户档案。
 * 执行器是单线程推送，异步在这里体现为：请求 {@code HttpClient.sendAsync} 并发在途，
 * 行到达时**先发已完成的、在途满了才阻塞等一个**（容量背压，与 Flink async 的 capacity 同口径）。
 *
 * <pre>
 * - id: enrich
 *   type: async.map
 *   config:
 *     url: "http://user-api/profile/{user_id}"   # {列名} 从行取（URL 编码）
 *     method: GET                                # GET / POST
 *     headers: {authorization: "Bearer ${token}"}
 *     bodyColumns: [user_id, ts]                 # POST 时按列拼 JSON body
 *     fields:                                    # 响应 JSON 路径 → 行列
 *       - {target: balance, source: data.balance}
 *     timeoutMs: 3000
 *     capacity: 100        # 在途上限（Flink async capacity 口径）
 *     outputMode: ordered  # ordered（按到达序）/ unordered（按完成序）
 *     onError: fail        # fail（出错抛出）/ skip（丢行并计数）
 *     allowedHosts: [user-api]                   # 内网接口的信任边界
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li><b>背压</b>：在途请求数到 {@code capacity} 就不再拉新行（阻塞在最早在途上）——
 *       下游不会被内存冲垮；</li>
 *   <li><b>产出顺序</b>：{@code ordered} 严格按行到达序发出（乱序完成的等前面的）；
 *       {@code unordered} 完成即发（吞吐优先）；</li>
 *   <li><b>流结束刷余</b>：源跑完后 {@code flush(Output)} 把在途的全部发出去
 *       （{@link Operator#flush} 的 endOfInput 口径）；</li>
 *   <li><b>失败语义</b>：缺省 fail fast；{@code onError: skip} 丢行并计数
 *       （{@code failed-skipped}），不静默装成功。</li>
 * </ul>
 *
 * @author zifang
 */
public class AsyncMapOperator implements Operator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** url 模板里的 {@code {列名}} 占位符。 */
    private static final Pattern TEMPLATE_COLUMN = Pattern.compile("\\{([A-Za-z0-9_]+)\\}");

    private final PipelineNode node;

    private String urlTemplate;
    private String method;
    private Map<String, Object> headers = new LinkedHashMap<String, Object>();
    private List<String> bodyColumns = new ArrayList<String>();
    private List<FieldExtract> fields = new ArrayList<FieldExtract>();
    private long timeoutMs;
    private int capacity;
    private boolean ordered;
    private boolean failFast;
    private List<String> allowedHosts = new ArrayList<String>();

    private HttpClient client;
    private final Deque<InFlight> inFlight = new ArrayDeque<InFlight>();

    /**
     * @param node 节点声明
     */
    public AsyncMapOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) {
        this.context = context;
        urlTemplate = node.string("url");
        if (urlTemplate == null || urlTemplate.trim().isEmpty()) {
            throw new IllegalArgumentException("async.map 节点 " + node.getId() + " 必须声明 url");
        }
        method = node.string("method", "GET").toUpperCase();
        Map<String, Object> declared = node.map("headers");
        if (declared != null) {
            headers.putAll(declared);
        }
        bodyColumns = node.strings("bodyColumns");
        for (Map<String, Object> item : node.mapList("fields")) {
            String target = item.get("target") == null ? null : String.valueOf(item.get("target")).trim();
            if (target == null || target.isEmpty()) {
                throw new IllegalArgumentException("async.map 节点 " + node.getId()
                        + " 的 fields 项缺少 target");
            }
            String source = item.get("source") == null ? target : String.valueOf(item.get("source")).trim();
            fields.add(new FieldExtract(target, source));
        }
        timeoutMs = node.longValue("timeoutMs", 3000L);
        capacity = Math.max(1, node.integer("capacity", 100));
        ordered = !"unordered".equalsIgnoreCase(node.string("outputMode", "ordered"));
        failFast = !"skip".equalsIgnoreCase(node.string("onError", "fail"));
        allowedHosts = node.strings("allowedHosts");
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        drainCompleted(output);
        while (inFlight.size() >= capacity) {
            waitOne(output);
        }
        launch(record);
    }

    /** 流结束：在途的全部发出去（发完才返回）。 */
    @Override
    public void flush(Output output) throws Exception {
        while (!inFlight.isEmpty()) {
            waitOne(output);
        }
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态；在途请求的产出在 processElement / flush 里发
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        // HttpClient 无需显式关闭（Java 8 风格的 JDK HttpClient 没有关闭口）
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 发一个在途请求。 */
    private void launch(Record record) {
        String target = renderUrl(record.row());
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(target))
                .timeout(Duration.ofMillis(timeoutMs));
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            builder.header(entry.getKey(), String.valueOf(entry.getValue()));
        }
        if ("POST".equals(method)) {
            builder.POST(HttpRequest.BodyPublishers.ofString(renderBody(record.row())));
        } else {
            builder.GET();
        }
        CompletableFuture<HttpResponse<String>> future =
                client.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString());
        inFlight.add(new InFlight(future, record));
    }

    /** 发出队头**已完成的**在途（乱序完成的等前面——顺序由 waitOne 的 anyOf 口径补）。 */
    private void drainCompleted(Output output) throws Exception {
        while (!inFlight.isEmpty()) {
            InFlight head = inFlight.peek();
            if (!head.future.isDone()) {
                return;
            }
            inFlight.poll();
            emit(head, output);
        }
    }

    /** 阻塞等至少一个在途完成并发出去（容量背压 / 流结束刷余用）。 */
    private void waitOne(Output output) throws Exception {
        if (inFlight.isEmpty()) {
            return;
        }
        if (ordered) {
            InFlight head = inFlight.peek();
            joinQuietly(head.future);
            inFlight.poll();
            emit(head, output);
            return;
        }
        List<CompletableFuture<?>> futures = new ArrayList<CompletableFuture<?>>();
        for (InFlight pending : inFlight) {
            futures.add(pending.future);
        }
        CompletableFuture.anyOf(futures.toArray(new CompletableFuture<?>[0])).join();
        for (Iterator<InFlight> it = inFlight.iterator(); it.hasNext(); ) {
            InFlight pending = it.next();
            if (pending.future.isDone()) {
                it.remove();
                emit(pending, output);
            }
        }
    }

    /** 一条在途完成 ⇒ 出行（失败按 onError 口径）。 */
    private void emit(InFlight pending, Output output) throws Exception {
        Record record = pending.record;
        String text;
        try {
            HttpResponse<String> response = pending.future.join();
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException("async.map 节点 " + node.getId() + " 调用失败：HTTP "
                        + response.statusCode());
            }
            text = response.body();
        } catch (RuntimeException e) {
            if (failFast) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new IllegalStateException("async.map 节点 " + node.getId()
                        + " 外呼失败: " + cause.getMessage(), cause);
            }
            context().count("failed-skipped");
            return;
        }
        Map<String, Object> merged = new LinkedHashMap<String, Object>(record.row());
        Object payload = text == null || text.trim().isEmpty() ? null : MAPPER.convertValue(MAPPER.readTree(text), Object.class);
        for (FieldExtract field : fields) {
            merged.put(field.target, extract(payload, field.source));
        }
        context().count("enriched");
        output.collect(record.withRow(merged));
    }

    /** JSON 载荷的点路径取值（{@code data.balance}；数组下标 {@code items.0}）。 */
    private static Object extract(Object payload, String path) {
        if (payload == null || path == null || path.isEmpty()) {
            return null;
        }
        Object current = payload;
        for (String part : path.split("\\.")) {
            if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
            } else if (current instanceof List) {
                try {
                    current = ((List<?>) current).get(Integer.parseInt(part));
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    return null;
                }
            } else {
                return null;
            }
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /** url 模板渲染：{列名} → 行值（URL 编码）；缺列直接报错。 */
    private String renderUrl(Map<String, Object> row) {
        Matcher matcher = TEMPLATE_COLUMN.matcher(urlTemplate);
        StringBuilder rendered = new StringBuilder();
        while (matcher.find()) {
            String column = matcher.group(1);
            Object value = row.get(column) == null ? RowValues.get(row, column) : row.get(column);
            if (value == null) {
                throw new IllegalArgumentException("async.map 节点 " + node.getId()
                        + " 的 url 模板需要列 " + column + "，但行里没有");
            }
            String encoded = URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8);
            matcher.appendReplacement(rendered, Matcher.quoteReplacement(encoded));
        }
        matcher.appendTail(rendered);
        String target = rendered.toString();
        checkHost(target);
        return target;
    }

    /** POST body：声明列拼 JSON。 */
    private String renderBody(Map<String, Object> row) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        for (String column : bodyColumns) {
            body.put(column, row.get(column) == null ? RowValues.get(row, column) : row.get(column));
        }
        try {
            return MAPPER.writeValueAsString(body);
        } catch (Exception e) {
            throw new IllegalStateException("async.map 节点 " + node.getId() + " 拼 body 失败: " + e.getMessage(), e);
        }
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
            throw new IllegalArgumentException("async.map 节点 " + node.getId() + " 的 url 不合法: " + target);
        }
        for (String allowed : allowedHosts) {
            if (allowed.equalsIgnoreCase(host)) {
                return;
            }
        }
        throw new IllegalArgumentException("async.map 节点 " + node.getId() + " 拒绝访问主机 " + host
                + "（allowedHosts: " + String.join(", ", allowedHosts) + "）");
    }

    private OperatorContext context;

    private OperatorContext context() {
        return context;
    }

    /** join 出来的异常统一成 CompletionException（emit 里按口径处理）。 */
    private static void joinQuietly(CompletableFuture<?> future) {
        try {
            future.join();
        } catch (RuntimeException ignored) {
            // 异常在 emit 的 join 里再抛（保持口径一致）
        }
    }

    /** 一条在途请求。 */
    private static final class InFlight {

        private final CompletableFuture<HttpResponse<String>> future;
        private final Record record;

        private InFlight(CompletableFuture<HttpResponse<String>> future, Record record) {
            this.future = future;
            this.record = record;
        }
    }

    /** 响应列提取声明。 */
    private static final class FieldExtract {

        private final String target;
        private final String source;

        private FieldExtract(String target, String source) {
            this.target = target;
            this.source = source;
        }
    }
}
