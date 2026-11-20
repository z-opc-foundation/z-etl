package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CSV 文件汇（输出类型）：把流写成 CSV 文件。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code columns} 决定列顺序；没声明时按**首行**的键序（后续行缺列补空、多列忽略）；</li>
 *   <li>{@code append=true} 追加，否则覆盖；{@code delimiter} / {@code quote} / {@code encoding} 可配；</li>
 *   <li>落盘发生在 {@code close()}（流水线收尾时刷批）。</li>
 * </ul>
 *
 * @author zifang
 */
public class FileCsvSink implements CountingSink {

    private final PipelineNode node;
    private String path;
    private char delimiter = ',';
    private char quote = '"';
    private String encoding = "UTF-8";
    private boolean append;
    private List<String> declaredColumns;

    private BufferedWriter writer;
    private List<String> columns;
    private boolean headerWritten;
    private long written;

    public FileCsvSink(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.csv.sink 节点 " + node.getId() + " 缺少 path");
        }
        String delimiterText = node.string("delimiter", ",");
        delimiter = delimiterText.isEmpty() ? ',' : delimiterText.charAt(0);
        String quoteText = node.string("quote", "\"");
        quote = quoteText.isEmpty() ? '"' : quoteText.charAt(0);
        encoding = node.string("encoding", "UTF-8");
        append = node.bool("append", false);
        declaredColumns = node.strings("columns");
        columns = new ArrayList<String>(declaredColumns);

        Path target = Paths.get(path);
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        if (append) {
            writer = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(target, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    Charset.forName(encoding)));
            headerWritten = !Files.exists(target) || Files.size(target) == 0L;
        } else {
            writer = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(target, StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
                    Charset.forName(encoding)));
        }
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        Map<String, Object> row = record.row();
        if (columns.isEmpty()) {
            columns = new ArrayList<String>(new LinkedHashSet<String>(row.keySet()));
        }
        if (!headerWritten) {
            writer.write(CsvSupport.formatRow(cells(columns), delimiter, quote));
            writer.newLine();
            headerWritten = true;
        }
        List<String> values = new ArrayList<String>(columns.size());
        for (String column : columns) {
            Object value = row.get(column);
            values.add(value == null ? "" : String.valueOf(value));
        }
        writer.write(CsvSupport.formatRow(values, delimiter, quote));
        writer.newLine();
        written++;
    }

    /** 表头行（列名）。 */
    private List<String> cells(List<String> names) {
        List<String> header = new ArrayList<String>(names.size());
        for (String name : names) {
            header.add(name);
        }
        return header;
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
    public void close() throws Exception {
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
    }

    @Override
    public long written() {
        return written;
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 落盘后的列顺序（取证用）。 */
    public List<String> columns() {
        return columns;
    }

    /** 文件里的列名集合（取证用，避免与 {@link #columns()} 语义混淆）。 */
    public Set<String> columnNames() {
        return new LinkedHashSet<String>(columns);
    }
}
