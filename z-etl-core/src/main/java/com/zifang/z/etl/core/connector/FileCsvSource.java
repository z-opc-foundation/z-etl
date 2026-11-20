package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CSV 文件源（输入类型）。
 *
 * <p>口径：
 * <ul>
 *   <li>第一行作表头（{@code header=false} 时用声明的 {@code columns} 或 {@code col_1..col_N}）；</li>
 *   <li>列类型：声明 {@code columnTypes} 优先，没声明的列**按内容推断**（整数 / 定点小数 / 布尔 / 时间戳 / 文本）；</li>
 *   <li>增量：声明 {@code eventTimeColumn} 即按事件时间做复合游标过滤；也可以用
 *       {@code resume: {column: "__offset__"}} 按行号接力（文件追加场景最常用）。</li>
 * </ul>
 *
 * <pre>
 * - id: csv
 *   type: file.csv.source
 *   config:
 *     path: /data/orders.csv
 *     delimiter: ","
 *     columnTypes: {amount: BIGDECIMAL, updated_at: TIMESTAMP}
 *     eventTimeColumn: updated_at
 * </pre>
 *
 * @author zifang
 */
public class FileCsvSource extends AbstractPullSource {

    private String path;
    private char delimiter = ',';
    private char quote = '"';
    private boolean header = true;
    private String encoding = "UTF-8";
    private List<String> declaredColumns = new ArrayList<String>();
    private Map<String, Object> columnTypes;
    private int skipRows;
    private long limit;

    private BufferedReader reader;
    private List<String> names = new ArrayList<String>();
    private long produced;

    public FileCsvSource(PipelineNode node) {
        super(node);
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.csv.source 节点 " + node.getId() + " 缺少 path");
        }
        String delimiterText = node.string("delimiter", ",");
        delimiter = delimiterText.isEmpty() ? ',' : delimiterText.charAt(0);
        String quoteText = node.string("quote", "\"");
        quote = quoteText.isEmpty() ? '"' : quoteText.charAt(0);
        header = node.bool("header", true);
        encoding = node.string("encoding", "UTF-8");
        declaredColumns = node.strings("columns");
        columnTypes = node.map("columnTypes");
        skipRows = node.integer("skipRows", 0);
        limit = node.longValue("limit", 0L);

        reader = new BufferedReader(new InputStreamReader(
                Files.newInputStream(Paths.get(path)), Charset.forName(encoding)));
        if (header) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                names = new ArrayList<String>();
                return;
            }
            names = CsvSupport.parseLine(headerLine, delimiter, quote);
        } else if (!declaredColumns.isEmpty()) {
            names = new ArrayList<String>(declaredColumns);
        }
        for (int i = 0; i < skipRows; i++) {
            if (reader.readLine() == null) {
                break;
            }
        }
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        if (reader == null) {
            return null;
        }
        if (limit > 0L && produced >= limit) {
            return null;
        }
        String line = reader.readLine();
        while (line != null && line.trim().isEmpty()) {
            line = reader.readLine();
        }
        if (line == null) {
            return null;
        }
        List<String> values = CsvSupport.parseLine(line, delimiter, quote);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (int i = 0; i < values.size(); i++) {
            String name = columnName(i);
            String raw = values.get(i);
            row.put(name, ColumnTypes.convert(raw, ColumnTypes.declared(columnTypes, name)));
        }
        produced++;
        return row;
    }

    /** 列名：表头优先；没有表头时用声明列名，再没有就用 {@code col_N}。 */
    private String columnName(int index) {
        if (index < names.size()) {
            return names.get(index);
        }
        if (index < declaredColumns.size()) {
            return declaredColumns.get(index);
        }
        return "col_" + (index + 1);
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
