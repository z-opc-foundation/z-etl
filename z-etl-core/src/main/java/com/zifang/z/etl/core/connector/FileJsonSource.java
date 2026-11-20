package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON 文件源（输入类型）：支持 **NDJSON**（一行一个 JSON 对象）与 **JSON 数组** 两种形态。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code itemPath} 指定数组所在路径（点路径，如 {@code data.items}）；默认根节点就是数组；</li>
 *   <li>{@code columns} 做字段投影（只要这几列）；{@code columnTypes} 声明列类型；</li>
 *   <li>增量：与 CSV 源同口径（事件时间列 / {@code __offset__} 行号游标）。</li>
 * </ul>
 *
 * @author zifang
 */
public class FileJsonSource extends AbstractPullSource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String path;
    private String format = "ndjson";
    private String itemPath;
    private String encoding = "UTF-8";
    private List<String> declaredColumns = new ArrayList<String>();
    private Map<String, Object> columnTypes;
    private long limit;

    private BufferedReader reader;
    private List<Map<String, Object>> buffer = new ArrayList<Map<String, Object>>();
    private int bufferIndex;
    private long produced;

    public FileJsonSource(PipelineNode node) {
        super(node);
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.json.source 节点 " + node.getId() + " 缺少 path");
        }
        format = node.string("format", "ndjson").toLowerCase();
        itemPath = node.string("itemPath");
        encoding = node.string("encoding", "UTF-8");
        declaredColumns = node.strings("columns");
        columnTypes = node.map("columnTypes");
        limit = node.longValue("limit", 0L);

        if ("json".equals(format)) {
            String text = new String(Files.readAllBytes(Paths.get(path)), Charset.forName(encoding));
            buffer = parseArray(text);
            bufferIndex = 0;
        } else {
            reader = new BufferedReader(new InputStreamReader(
                    Files.newInputStream(Paths.get(path)), Charset.forName(encoding)));
        }
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        if (limit > 0L && produced >= limit) {
            return null;
        }
        if ("json".equals(format)) {
            if (bufferIndex >= buffer.size()) {
                return null;
            }
            produced++;
            return buffer.get(bufferIndex++);
        }
        if (reader == null) {
            return null;
        }
        String line = reader.readLine();
        while (line != null && line.trim().isEmpty()) {
            line = reader.readLine();
        }
        if (line == null) {
            return null;
        }
        JsonNode node = MAPPER.readTree(line);
        Map<String, Object> row = toRow(node);
        produced++;
        return row;
    }

    /** JSON 数组文本 → 行列表（带投影与类型归一）。 */
    private List<Map<String, Object>> parseArray(String text) throws Exception {
        JsonNode root = MAPPER.readTree(text);
        JsonNode array = root;
        if (itemPath != null) {
            array = navigate(root, itemPath);
        }
        if (array == null || !array.isArray()) {
            throw new IllegalArgumentException("file.json.source 节点 " + node.getId()
                    + " 的 itemPath=" + itemPath + " 没定位到 JSON 数组");
        }
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (JsonNode item : array) {
            rows.add(toRow(item));
        }
        return rows;
    }

    /** 点路径导航（{@code data.items}）。 */
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

    /** JSON 节点 → 行（字段扁平化 + 投影 + 类型归一）。 */
    @SuppressWarnings("unchecked")
    private Map<String, Object> toRow(JsonNode node) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        if (node == null || node.isNull()) {
            return row;
        }
        if (!node.isObject()) {
            row.put("value", scalar(node));
            return row;
        }
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            if (!declaredColumns.isEmpty() && !declaredColumns.contains(name)) {
                continue;
            }
            JsonNode value = node.get(name);
            row.put(name, ColumnTypes.convert(scalar(value), ColumnTypes.declared(columnTypes, name)));
        }
        return row;
    }

    /** JSON 标量 → Java 值（对象 / 数组保留为 Map / List）。 */
    @SuppressWarnings("unchecked")
    private Object scalar(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            return MAPPER.convertValue(node, Map.class);
        }
        if (node.isArray()) {
            return MAPPER.convertValue(node, List.class);
        }
        if (node.isBoolean()) {
            return Boolean.valueOf(node.booleanValue());
        }
        if (node.isInt()) {
            return Integer.valueOf(node.intValue());
        }
        if (node.isLong()) {
            return Long.valueOf(node.longValue());
        }
        if (node.isDouble() || node.isFloat()) {
            return Double.valueOf(node.doubleValue());
        }
        return node.asText();
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception ignore) {
                // 关闭异常不影响主流程
            }
            reader = null;
        }
    }
}
